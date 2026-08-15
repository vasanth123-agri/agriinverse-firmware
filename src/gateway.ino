// =============================================================================
//  AgriInverse — ESP32 LoRa/LTE Gateway  v4.5
//
//  RADIO ARCHITECTURE:
//  Single loraTask owns radio exclusively (Core 0, pri 3).
//  TX → blocking endPacket() → immediate LoRa.receive() → no race condition.
//
//  COMMAND FLOW:
//  1. MQTT cmd received  → send LoRa cmd → arm retry slot (attempt 1/3)
//  2. ACK within 90 s?   → YES: publish relay_ack to AWS, done
//  3. No ACK             → retry attempt 2/3
//  4. ACK within 90 s?   → YES: publish relay_ack to AWS, done
//  5. No ACK             → retry attempt 3/3
//  6. ACK within 90 s?   → YES: publish relay_ack to AWS, done
//  7. No ACK             → publish no_ack to AWS, done
//
//  OLED LAYOUT (128×64, SSD1306):
//  ┌──────────────────────────────┐
//  │ [LoRa▂▄▆█]  [GSM ▂▄▆█]     │  row 0  — signal bars
//  │ LoRa:ACTIVE   GSM:CONNECTED  │  row 18 — status labels
//  │ MQTT: CONNECTED              │  row 30 — MQTT status
//  │ CMD:  Valve A open           │  row 42 — last command
//  │ ERR:  --                     │  row 54 — error code
//  └──────────────────────────────┘
//
//  ERROR CODES:
//  100 — No ACK from node after 3 attempts
//  101 — LoRa radio reinit / hardware failure
//  102 — Received invalid/non-JSON packet
//  103 — JSON parse failure
// =============================================================================

#define TINY_GSM_MODEM_A7670
#define SerialMon  Serial
#define SerialAT   Serial1
#define TINY_GSM_DEBUG SerialMon
#define GSM_PIN    ""

#include <TinyGsmClient.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <LoRa.h>
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "certificate.h"
#include "privatekey.h"
#include "root_ca.h"

// =============================================================================
//  SECTION 1 — IDENTITY & PINS
// =============================================================================

#define DEVICE_ID    "42"

#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY  4

#define LORA_SS      5
#define LORA_RST     14
#define LORA_DIO0    26
#define LORA_FREQ    433E6

#define NODE3_ID     0xBB
#define NODE5_ID     0xD1   // Solenoid valve node
#define NODE6_ID     0xD2   // Motor relay node
#define NODE7_ID     0xD3   // Appliances node
#define NODE8_ID     0xD4   // Sensor node
#define NODE9_ID     0xD5   // Fertigation node

// =============================================================================
//  SECTION 2 — OLED
// =============================================================================

#define OLED_SDA     21
#define OLED_SCL     22
#define OLED_ADDR    0x3C
#define SCREEN_W     128
#define SCREEN_H     64
#define OLED_RESET   -1

Adafruit_SSD1306 display(SCREEN_W, SCREEN_H, &Wire, OLED_RESET);
static bool oledOK = false;

// =============================================================================
//  SECTION 3 — NETWORK & BROKER
// =============================================================================

const char apn[]      = "iot.com";
const char gprsUser[] = "";
const char gprsPass[] = "";

const char broker[]            = "a2nvakoqnh614u-ats.iot.us-east-1.amazonaws.com";
const int  broker_port         = 8883;
const char client_id[]         = DEVICE_ID;

const char subscribe_topic[]     = "device/cmd";
const char ota_subscribe_topic[] = "OTA/mode";
const char publish_topic[]       = "relay/ack";
const char publish_power_topic[] = "relay/power";
const char publish_ota_topic[]   = "relay/ota_ack";

// =============================================================================
//  SECTION 4 — LoRa RF PARAMETERS
// =============================================================================

#define LORA_SF        7
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xAB
#define LORA_TXPOWER   20
#define LORA_PREAMBLE  8

// =============================================================================
//  SECTION 5 — QUEUE DEPTHS & TIMEOUTS
// =============================================================================

#define RX_QUEUE_DEPTH       16
#define TX_HI_QUEUE_DEPTH    8
#define TX_LO_QUEUE_DEPTH    16
#define MQTT_PUB_QUEUE_DEPTH 8

#define MQTT_RETRY_MIN_MS   10000UL
#define MQTT_RETRY_MAX_MS   60000UL
#define LORA_WATCHDOG_MS    (5UL * 60 * 1000)
#define LTE_WATCHDOG_MS     (10UL * 60 * 1000)

#define CMD_MAX_RETRIES  2
#define CMD_RETRY_MS     90000UL

// =============================================================================
//  SECTION 6 — ERROR CODES
// =============================================================================

#define ERR_NONE          0
#define ERR_NO_ACK      100   // No ACK from node after 3 attempts
#define ERR_LORA_REINIT 101   // LoRa radio reinit / hardware failure
#define ERR_INVALID_PKT 102   // Received non-JSON / malformed packet
#define ERR_JSON_PARSE  103   // JSON deserialize failure

// =============================================================================
//  SECTION 7 — STRUCTS
// =============================================================================

struct LoRaRxPacket {
    uint8_t  src;
    uint8_t  dest;
    uint8_t  buf[256];
    uint16_t len;
    int      rssi;
    float    snr;
};

struct LoRaTxPacket {
    uint8_t  dest;
    uint8_t  src;
    uint8_t  buf[256];
    uint16_t len;
};

struct MqttPubReq {
    char topic[128];
    char payload[256];
};

struct PendingCmd {
    bool     active      = false;
    uint8_t  destNode    = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[256]= {};
    uint16_t payloadLen  = 0;
    char     deviceId[32]= {};
    char     actuator[16]= {};
    char     ackJson[320]= {};
};

struct OtaPending {
    bool     active      = false;
    uint8_t  destNode    = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[256]= {};
    uint16_t payloadLen  = 0;
    char     deviceId[32]= {};
    char     nodeType[24]= {};   // "valve node" / "motor node" / "appliances node" / etc
    char     ackJson[320]= {};
};

// =============================================================================
//  SECTION 8 — DISPLAY STATE  (written by any task, read by displayTask)
// =============================================================================

struct DisplayState {
    bool     loraActive   = false;
    bool     mqttOnline   = false;
    int      loraRSSI     = -120;
    int      gsmRSSI      = 0;       // CSQ value 0–31 from modem
    uint16_t errorCode    = ERR_NONE;
    char     lastCmd[32]  = "--";    // e.g. "Valve A open"
};
static volatile DisplayState dState;

// =============================================================================
//  SECTION 9 — RTOS OBJECTS
// =============================================================================

static QueueHandle_t  rxQueue;
static QueueHandle_t  txHiQueue;
static QueueHandle_t  txLoQueue;
static QueueHandle_t  mqttPubQueue;

static TaskHandle_t   loraTaskHandle    = nullptr;
static TaskHandle_t   mqttTaskHandle    = nullptr;
static TaskHandle_t   appTaskHandle     = nullptr;
static TaskHandle_t   displayTaskHandle = nullptr;

#define PENDING_VALVE_IDX  0
#define PENDING_MOTOR_IDX  1
static PendingCmd pendingCmd[2];

// Single in-flight OTA job at a time (simple queueing model — extend to an
// array if you need concurrent OTA pushes to multiple nodes).
static OtaPending otaPending;
#define OTA_MAX_RETRIES  2
#define OTA_RETRY_MS     90000UL

// =============================================================================
//  SECTION 10 — SHARED STATE
// =============================================================================

static volatile bool     mqttOnline   = false;
static volatile uint32_t lastLoRaRxMs = 0;

// =============================================================================
//  SECTION 11 — DIAGNOSTICS
// =============================================================================

struct Diag {
    uint32_t rxTotal;
    uint32_t rxDroppedDest;
    uint32_t rxDroppedJson;
    uint32_t txSent;
    uint32_t txQueueFull;
    uint32_t mqttPubOk;
    uint32_t mqttPubFail;
    uint32_t mqttReconnects;
    uint32_t lteWatchdogFires;
    uint32_t radioReinits;
    uint32_t cmdRetries;
    uint32_t cmdAckAttempt1;
    uint32_t cmdAckAttempt2;
    uint32_t cmdAckAttempt3;
    uint32_t cmdNoAck;
};
static Diag diag = {};

// =============================================================================
//  SECTION 12 — MODEM
// =============================================================================

TinyGsm modem(SerialAT);

// =============================================================================
//  SECTION 13 — FORWARD DECLARATIONS
// =============================================================================

void  loraTask(void*);
void  mqttTask(void*);
void  appTask(void*);
void  displayTask(void*);
void  reinitLoRaRadio();
bool  enqueueTxHi(uint8_t dest, uint8_t src, const uint8_t* data, uint16_t len);
bool  enqueueTxLo(uint8_t dest, uint8_t src, const uint8_t* data, uint16_t len);
bool  enqueueTxHiJson(uint8_t dest, uint8_t src, JsonDocument& doc);
void  processRxPacket(LoRaRxPacket& pkt);
void  handleLoRaAck(DynamicJsonDocument& loraDoc, uint8_t srcNode);
void  handleLoRaPower(DynamicJsonDocument& doc);
void  mqttCallback(const char* topic, const uint8_t* payload, uint32_t len);
bool  mqttConnectOnce();
void  enqueueValveCommand(const String& device, const String& valve,
                          const String& cmd, const String& fid, const String& bid);
void  enqueueMotorCommand(const String& device, const String& state,
                          const String& fid, const String& bid);
void  enqueueMqttStatus(bool connected);
void  armRetry(int idx, uint8_t dest, const char* device,
               const char* actuator, const char* jsonBuf, uint16_t len);
void  publishAck(int idx);
void  publishNoAck(int idx);
void  clearPending(int idx);
bool  isValveAck(DynamicJsonDocument& doc);
void  setError(uint16_t code);
void  clearError();
void  printDiagnostics();

// OTA
void  handleOtaCommand(DynamicJsonDocument& doc);
bool  nodeTypeToAddr(const String& deviceType, uint8_t& outAddr);
void  armOtaRetry(uint8_t dest, const char* device, const char* nodeType,
                   const char* jsonBuf, uint16_t len);
void  publishOtaAck(bool success, const char* deviceId, const char* nodeType, const char* detail);
void  handleLoRaOtaAck(DynamicJsonDocument& loraDoc);

// OLED helpers
void  oledDrawSignalBars(int x, int y, int bars, int maxBars);
int   rssiToLoRaBars(int rssi);
int   csmToGsmBars(int csq);
void  oledRender();

// =============================================================================
//  SECTION 14 — ERROR HELPERS
// =============================================================================

void setError(uint16_t code) {
    dState.errorCode = code;
    Serial.printf("⚠️ Error set: %d\n", code);
}

void clearError() {
    dState.errorCode = ERR_NONE;
}

// =============================================================================
//  SECTION 15 — SINGLE LoRa TASK  (Core 0, pri 3)
// =============================================================================

static void loraDoTx(LoRaTxPacket& pkt) {
    LoRa.beginPacket();
    LoRa.write(pkt.dest);
    LoRa.write(pkt.src);
    LoRa.write(pkt.buf, pkt.len);
    LoRa.endPacket();       // BLOCKING — TX complete before returning
    diag.txSent++;
    Serial.printf("📡 LoRa TX → dest:0x%02X  src:0x%02X  len:%u\n",
                  pkt.dest, pkt.src, pkt.len);
    LoRa.receive();         // immediately re-enter RX
    Serial.println("📻 Radio back in RX mode");
}

void loraTask(void* param) {
    LoRa.receive();
    dState.loraActive = true;
    Serial.println("📻 LoRa task started — radio in RX mode");

    uint32_t lastWatchdog = millis();

    for (;;) {
        // 1. TX hi-priority
        {
            LoRaTxPacket pkt;
            if (xQueueReceive(txHiQueue, &pkt, 0) == pdTRUE) {
                loraDoTx(pkt);
                continue;
            }
        }

        // 2. TX lo-priority
        {
            LoRaTxPacket pkt;
            if (xQueueReceive(txLoQueue, &pkt, 0) == pdTRUE) {
                loraDoTx(pkt);
                continue;
            }
        }

        // 3. RX
        int packetSize = LoRa.parsePacket();
        if (packetSize >= 2) {
            LoRaRxPacket rx;
            rx.dest = LoRa.read();
            rx.src  = LoRa.read();

            int bodyLen = packetSize - 2;
            if (bodyLen > (int)(sizeof(rx.buf) - 1)) bodyLen = sizeof(rx.buf) - 1;
            rx.len = (uint16_t)LoRa.readBytes(rx.buf, bodyLen);
            rx.buf[rx.len] = '\0';
            rx.rssi = LoRa.packetRssi();
            rx.snr  = LoRa.packetSnr();

            lastLoRaRxMs      = millis();
            lastWatchdog      = millis();
            dState.loraRSSI   = rx.rssi;
            dState.loraActive = true;
            diag.rxTotal++;

            if (rx.dest != NODE3_ID) {
                diag.rxDroppedDest++;
            } else {
                if (xQueueSend(rxQueue, &rx, 0) != pdTRUE)
                    Serial.printf("⚠️ rxQueue full — dropped src:0x%02X\n", rx.src);
            }
            LoRa.receive();
        }

        // 4. LoRa watchdog
        if ((millis() - lastWatchdog) >= LORA_WATCHDOG_MS) {
            Serial.println("⚠️ LoRa watchdog — reinitialising radio");
            dState.loraActive = false;
            reinitLoRaRadio();
            diag.radioReinits++;
            lastWatchdog = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

// =============================================================================
//  SECTION 16 — TX QUEUE HELPERS
// =============================================================================

static bool enqueueTxRaw(QueueHandle_t q, uint8_t dest, uint8_t src,
                         const uint8_t* data, uint16_t len, bool dropOld) {
    LoRaTxPacket pkt;
    pkt.dest = dest;
    pkt.src  = src;
    pkt.len  = (len < sizeof(pkt.buf)) ? len : (uint16_t)sizeof(pkt.buf);
    memcpy(pkt.buf, data, pkt.len);

    if (xQueueSend(q, &pkt, 0) == pdTRUE) return true;
    diag.txQueueFull++;
    if (dropOld) {
        LoRaTxPacket discard;
        xQueueReceive(q, &discard, 0);
        return xQueueSend(q, &pkt, 0) == pdTRUE;
    }
    return false;
}

bool enqueueTxHi(uint8_t dest, uint8_t src, const uint8_t* d, uint16_t len) {
    return enqueueTxRaw(txHiQueue, dest, src, d, len, true);
}
bool enqueueTxLo(uint8_t dest, uint8_t src, const uint8_t* d, uint16_t len) {
    return enqueueTxRaw(txLoQueue, dest, src, d, len, false);
}
bool enqueueTxHiJson(uint8_t dest, uint8_t src, JsonDocument& doc) {
    char buf[256];
    uint16_t n = (uint16_t)serializeJson(doc, buf, sizeof(buf));
    return enqueueTxHi(dest, src, (const uint8_t*)buf, n);
}

// =============================================================================
//  SECTION 17 — COMMAND HELPERS
// =============================================================================

void enqueueValveCommand(const String& device, const String& valve,
                         const String& cmd, const String& fid, const String& bid) {
    StaticJsonDocument<256> doc;
    doc["type"]    = "command_broadcast";
    doc["UId"]     = DEVICE_ID;
    doc["device"]  = device;
    doc["valve"]   = valve;
    doc["command"] = cmd;
    if (fid.length()) doc["FId"] = fid;
    if (bid.length()) doc["BId"] = bid;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(doc, buf, sizeof(buf));

    if (enqueueTxLo(NODE5_ID, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [Attempt 1/3] VALVE CMD → node:0x%02X  dev:%s  valve:%s  cmd:%s\n",
                      NODE5_ID, device.c_str(), valve.c_str(), cmd.c_str());

        // Update display last command
        char cmdStr[32];
        snprintf(cmdStr, sizeof(cmdStr), "Valve %s %s", valve.c_str(), cmd.c_str());
        strlcpy((char*)dState.lastCmd, cmdStr, sizeof(dState.lastCmd));
        clearError();

        armRetry(PENDING_VALVE_IDX, NODE5_ID, device.c_str(), "valve", buf, n);
    }
}

void enqueueMotorCommand(const String& device, const String& state,
                         const String& fid, const String& bid) {
    StaticJsonDocument<256> doc;
    doc["type"]   = "command_broadcast";
    doc["UId"]    = DEVICE_ID;
    doc["device"] = device;
    doc["state"]  = state;
    if (fid.length()) doc["FId"] = fid;
    if (bid.length()) doc["BId"] = bid;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(doc, buf, sizeof(buf));

    if (enqueueTxLo(NODE6_ID, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [Attempt 1/3] MOTOR CMD → node:0x%02X  dev:%s  state:%s\n",
                      NODE6_ID, device.c_str(), state.c_str());

        char cmdStr[32];
        snprintf(cmdStr, sizeof(cmdStr), "Motor %s", state.c_str());
        strlcpy((char*)dState.lastCmd, cmdStr, sizeof(dState.lastCmd));
        clearError();

        armRetry(PENDING_MOTOR_IDX, NODE6_ID, device.c_str(), "motor", buf, n);
    }
}

void enqueueMqttStatus(bool connected) {
    StaticJsonDocument<128> doc;
    doc["type"]  = "mqtt";
    doc["state"] = connected ? "connected" : "failed";
    enqueueTxHiJson(NODE5_ID, NODE3_ID, doc);
    enqueueTxHiJson(NODE6_ID, NODE3_ID, doc);
}

// =============================================================================
//  SECTION 18 — MQTT CALLBACK
// =============================================================================

void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len) {
    char msgBuf[512];
    uint32_t n = (len < sizeof(msgBuf) - 1) ? len : sizeof(msgBuf) - 1;
    memcpy(msgBuf, payload, n);
    msgBuf[n] = '\0';

    Serial.printf("\n📥 MQTT IN [%s]: %s\n", topic, msgBuf);

    DynamicJsonDocument doc(512);
    if (deserializeJson(doc, msgBuf) != DeserializationError::Ok) {
        Serial.println("⚠️ MQTT JSON parse error");
        setError(ERR_JSON_PARSE);
        return;
    }

    if (strcmp(doc["UId"] | "", DEVICE_ID) != 0) {
        Serial.println("⏭ UId mismatch — ignoring");
        return;
    }

    if (strcmp(topic, ota_subscribe_topic) == 0) {
        handleOtaCommand(doc);
        return;
    }

    if (strcmp(doc["type"] | "", "command_broadcast") != 0) {
        Serial.printf("⏭ Unhandled MQTT type: '%s'\n", doc["type"] | "");
        return;
    }

    const char* device = doc["device"] | "";

    if (doc.containsKey("valve")) {
        const char* valve = doc["valve"]   | "";
        const char* cmd   = doc["command"] | "";
        const char* fid   = doc["FId"]     | "";
        const char* bid   = doc["BId"]     | "";
        if (!strlen(valve)) { Serial.println("⚠️ Missing 'valve' value"); return; }
        enqueueValveCommand(device, valve, cmd, fid, bid);

    } else if (doc.containsKey("state")) {
        const char* state = doc["state"] | "";
        const char* fid   = doc["FId"]   | "";
        const char* bid   = doc["BId"]   | "";
        if (!strlen(state)) { Serial.println("⚠️ Missing 'state' value"); return; }
        enqueueMotorCommand(device, state, fid, bid);

    } else {
        Serial.println("⚠️ command_broadcast: no 'valve' or 'state' field");
    }
}

// =============================================================================
//  SECTION 18b — OTA HANDLING
//
//  Expected "OTA/mode" MQTT payload from AWS:
//  {
//    "UId":    "42",
//    "FId":    "farm1",
//    "BId":    "block3",
//    "device": "valve node",     // see nodeTypeToAddr() for the full list
//    "VId":    "1",              // only when device == "valve node"
//    "AId":    "1",              // only when device == "appliances node"
//    "MId":    "1",              // only when device == "motor node"
//    "wifi_ssid": "farm-ap",
//    "wifi_pass": "secret",
//    "url":    "https://github.com/org/repo/releases/download/v1.2.0/node.bin"
//  }
//
//  The gateway does NOT flash itself here (unless device type matches this
//  gateway's own role) — it repackages the job as a small LoRa "ota" packet
//  and pushes it to the target node, using the same 3-attempt / 90 s ACK
//  retry pattern as command_broadcast. The node firmware is responsible for
//  joining wifi_ssid/wifi_pass, downloading "url", and self-flashing —
//  that part lives in the NODE codebase, not this gateway.
// =============================================================================

// Map a device-type string (as sent by AWS) to a LoRa node address.
// Returns false for gateway-type strings (VMCgateway, appliances gateway,
// fertigation gateway, sensor gateway) since those aren't LoRa nodes.
bool nodeTypeToAddr(const String& deviceType, uint8_t& outAddr) {
    String t = deviceType; t.toLowerCase();
    if (t == "valve node")       { outAddr = NODE5_ID; return true; }
    if (t == "motor node")       { outAddr = NODE6_ID; return true; }
    if (t == "appliances node")  { outAddr = NODE7_ID; return true; }
    if (t == "sensor node")      { outAddr = NODE8_ID; return true; }
    if (t == "fertigation node") { outAddr = NODE9_ID; return true; }
    return false;   // *gateway types (or unrecognised) — not a LoRa target
}

void armOtaRetry(uint8_t dest, const char* device, const char* nodeType,
                 const char* jsonBuf, uint16_t len) {
    otaPending.active      = true;
    otaPending.destNode    = dest;
    otaPending.retryCount  = 0;
    otaPending.nextRetryMs = millis() + OTA_RETRY_MS;
    otaPending.payloadLen  = len;
    memset(otaPending.ackJson, 0, sizeof(otaPending.ackJson));
    memcpy(otaPending.payload, jsonBuf, len);
    strlcpy(otaPending.deviceId, device,  sizeof(otaPending.deviceId));
    strlcpy(otaPending.nodeType, nodeType, sizeof(otaPending.nodeType));

    Serial.printf("⏱ OTA armed — %s (%s) → node:0x%02X — ack timeout %lu s\n",
                  device, nodeType, dest, OTA_RETRY_MS / 1000);
}

void publishOtaAck(bool success, const char* deviceId, const char* nodeType, const char* detail) {
    StaticJsonDocument<256> doc;
    doc["type"]   = success ? "ota_ack" : "ota_no_ack";
    doc["UId"]    = DEVICE_ID;
    doc["device"] = deviceId;
    doc["node"]   = nodeType;
    if (detail && strlen(detail)) doc["detail"] = detail;

    MqttPubReq req;
    strlcpy(req.topic, publish_ota_topic, sizeof(req.topic));
    serializeJson(doc, req.payload, sizeof(req.payload));

    Serial.printf("📤 %s → AWS: %s\n", success ? "ota_ack" : "ota_no_ack", req.payload);
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — OTA ack dropped");
    }
}

void handleOtaCommand(DynamicJsonDocument& doc) {
    const char* deviceType = doc["device"] | "";
    if (!strlen(deviceType)) {
        Serial.println("⚠️ OTA: missing 'device' field");
        return;
    }

    uint8_t destAddr;
    if (!nodeTypeToAddr(String(deviceType), destAddr)) {
        // Not a LoRa node — presumably a *gateway* device type. Self-OTA for
        // this ESP32 (e.g. via HTTPUpdate over the LTE modem) is NOT wired
        // up in this file; hook it in here once you've picked an HTTPS
        // download path for A7670-based modems.
        Serial.printf("ℹ️ OTA target '%s' is a gateway role, not a LoRa node — "
                      "self-OTA not implemented in this build\n", deviceType);
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "gateway self-OTA not implemented");
        return;
    }

    if (otaPending.active) {
        Serial.println("⚠️ OTA already in progress — ignoring new request until it completes");
        return;
    }

    const char* url  = doc["url"] | "";
    if (!strlen(url)) {
        Serial.println("⚠️ OTA: missing 'url' field");
        return;
    }

    // Per-actuator numeric id (VId / AId / MId) — optional, forwarded as-is
    const char* vId = doc["VId"] | "";
    const char* aId = doc["AId"] | "";
    const char* mId = doc["MId"] | "";
    const char* fid = doc["FId"] | "";
    const char* bid = doc["BId"] | "";

    StaticJsonDocument<256> pkt;
    pkt["type"]  = "ota";
    pkt["UId"]   = DEVICE_ID;
    pkt["url"]   = url;
    if (doc.containsKey("wifi_ssid")) pkt["wifi"] = doc["wifi_ssid"];
    if (doc.containsKey("wifi_pass")) pkt["pass"] = doc["wifi_pass"];
    if (strlen(fid)) pkt["FId"] = fid;
    if (strlen(bid)) pkt["BId"] = bid;
    if (strlen(vId)) pkt["VId"] = vId;
    if (strlen(aId)) pkt["AId"] = aId;
    if (strlen(mId)) pkt["MId"] = mId;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(pkt, buf, sizeof(buf));

    if (n >= sizeof(buf)) {
        Serial.println("❌ OTA packet too large for LoRa payload (256 B) — shorten the URL");
        return;
    }

    if (enqueueTxLo(destAddr, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [OTA attempt 1/3] → node:0x%02X  dev:%s  type:%s  url:%s\n",
                      destAddr, doc["UId"] | DEVICE_ID, deviceType, url);
        armOtaRetry(destAddr, doc["UId"] | DEVICE_ID, deviceType, buf, n);
    } else {
        Serial.println("❌ OTA: LoRa TX queue full — could not enqueue");
    }
}

// Node replies over LoRa once flashing succeeds/fails:
//   {"type":"ota_ack","node":"valve node","status":"ok"}    or "fail"
void handleLoRaOtaAck(DynamicJsonDocument& loraDoc) {
    if (!otaPending.active) {
        Serial.println("⏭ Stale/duplicate OTA ack — no job in flight");
        return;
    }
    const char* status = loraDoc["status"] | "ok";
    bool ok = (strcasecmp(status, "fail") != 0);

    Serial.printf("✅ OTA ack from node — status:%s\n", status);
    publishOtaAck(ok, otaPending.deviceId, otaPending.nodeType, status);
    otaPending.active = false;
}

// =============================================================================
//  SECTION 19 — MQTT CONNECT
// =============================================================================

bool mqttConnectOnce() {
    modem.mqtt_set_certificate(root_ca, certificate, privateKey);
    if (!modem.mqtt_connect(0, broker, broker_port, client_id)) return false;
    modem.mqtt_subscribe(0, subscribe_topic, 1);
    modem.mqtt_subscribe(0, ota_subscribe_topic, 1);
    return true;
}

// =============================================================================
//  SECTION 20 — MQTT TASK  (Core 1, pri 3)
// =============================================================================

void mqttTask(void* param) {

    pinMode(MODEM_PWKEY, OUTPUT);
    digitalWrite(MODEM_PWKEY, LOW);
    vTaskDelay(pdMS_TO_TICKS(1500));
    digitalWrite(MODEM_PWKEY, HIGH);
    vTaskDelay(pdMS_TO_TICKS(5000));

    Serial.println("📶 Modem booting...");
    modem.init();
    vTaskDelay(pdMS_TO_TICKS(500));
    Serial.println("📶 Modem: " + modem.getModemInfo());

    modem.sendAT("+CNMP=38");
    modem.waitResponse();

    Serial.println("📶 Waiting for network...");
    uint32_t netDeadline = millis() + 90000UL;
    while (!modem.waitForNetwork(5000)) {
        Serial.println("⏳ Registering...");
        if (millis() > netDeadline) {
            Serial.println("🔄 Network timeout — restarting modem");
            modem.restart();
            vTaskDelay(pdMS_TO_TICKS(5000));
            netDeadline = millis() + 90000UL;
        }
    }
    Serial.println("✅ Network registered");

    while (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
        Serial.println("⏳ GPRS retry 5 s...");
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
    Serial.println("✅ GPRS connected | IP: " + modem.localIP().toString());

    modem.NTPServerSync("pool.ntp.org", 20);
    vTaskDelay(pdMS_TO_TICKS(2000));
    Serial.println("✅ NTP done");

    modem.mqtt_begin(false);
    modem.mqtt_set_callback(mqttCallback);

    enum State { CONNECTING, CONNECTED };
    State    state         = CONNECTING;
    uint32_t retryDelay    = MQTT_RETRY_MIN_MS;
    uint32_t nextRetry     = 0;
    uint32_t lastHandleMs  = 0;
    uint32_t lastSuccessMs = millis();
    uint32_t lastCsqMs     = 0;
    bool     notified      = false;


    for (;;) {
        uint32_t now = millis();

        if (now - lastHandleMs >= 10) {
            lastHandleMs = now;
            modem.mqtt_handle();
        }

        // Update GSM signal strength every 10 s
        if (now - lastCsqMs >= 10000UL) {
            lastCsqMs    = now;
            dState.gsmRSSI = modem.getSignalQuality();   // CSQ 0–31
        }

      
        // Drain publish queue when connected
        if (state == CONNECTED) {
            MqttPubReq req;
            while (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                int qos = (strcmp(req.topic, publish_power_topic) == 0) ? 0 : 1;  // ← NEW LINE
                if (modem.mqtt_publish(0, req.topic, req.payload, qos)) {          // ← CHANGED
                    diag.mqttPubOk++;
                    Serial.printf("✅ Published → [%s]: %s\n", req.topic, req.payload);
                } else {
                    diag.mqttPubFail++;
                    Serial.println("❌ Publish failed — re-queuing");
                    xQueueSend(mqttPubQueue, &req, 0);
                    break;
                }
            }
        }

        switch (state) {
        case CONNECTING:
            if (now < nextRetry) break;

            if (lastSuccessMs > 0 && (now - lastSuccessMs) > LTE_WATCHDOG_MS) {
                diag.lteWatchdogFires++;
                Serial.println("🔄 LTE watchdog — restarting modem");
                modem.restart();
                vTaskDelay(pdMS_TO_TICKS(5000));
                modem.waitForNetwork(15000);
                modem.gprsConnect(apn, gprsUser, gprsPass);
                modem.NTPServerSync("pool.ntp.org", 20);
                vTaskDelay(pdMS_TO_TICKS(1000));
                lastSuccessMs = millis();
                retryDelay    = MQTT_RETRY_MIN_MS;
                nextRetry     = 0;
                break;
            }

            Serial.println("🔌 MQTT connecting...");
            if (mqttConnectOnce()) {
                state            = CONNECTED;
                retryDelay       = MQTT_RETRY_MIN_MS;
                lastSuccessMs    = millis();
                notified         = false;
                mqttOnline       = true;
                dState.mqttOnline= true;
                diag.mqttReconnects++;
                Serial.println("✅ MQTT connected");
            } else {
                Serial.printf("❌ MQTT connect failed — retry in %lu s\n", retryDelay / 1000);
                mqttOnline        = false;
                dState.mqttOnline = false;
                nextRetry  = now + retryDelay;
                retryDelay = min(retryDelay * 2, (uint32_t)MQTT_RETRY_MAX_MS);
            }
            break;

        case CONNECTED:
            if (!modem.mqtt_connected()) {
                Serial.println("⚠️ MQTT dropped");
                mqttOnline        = false;
                dState.mqttOnline = false;
                enqueueMqttStatus(false);
                notified  = false;
                state     = CONNECTING;
                nextRetry = now + retryDelay;
                break;
            }
            if (!notified) {
                mqttOnline        = true;
                dState.mqttOnline = true;
                enqueueMqttStatus(true);
                notified = true;
            }
            lastSuccessMs = now;
            break;
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// =============================================================================
//  SECTION 21 — RETRY HELPERS
// =============================================================================

void armRetry(int idx, uint8_t dest, const char* device,
              const char* actuator, const char* jsonBuf, uint16_t len) {
    PendingCmd& p = pendingCmd[idx];
    p.active      = true;
    p.destNode    = dest;
    p.retryCount  = 0;
    p.nextRetryMs = millis() + CMD_RETRY_MS;
    p.payloadLen  = len;
    memset(p.ackJson, 0, sizeof(p.ackJson));
    memcpy(p.payload, jsonBuf, len);
    strlcpy(p.deviceId, device,   sizeof(p.deviceId));
    strlcpy(p.actuator, actuator, sizeof(p.actuator));

    Serial.printf("⏱ Waiting for ACK — %s (%s) — timeout in %lu s\n",
                  device, actuator, CMD_RETRY_MS / 1000);
}

void publishAck(int idx) {
    PendingCmd& p = pendingCmd[idx];

    MqttPubReq req;
    strlcpy(req.topic,   publish_topic, sizeof(req.topic));
    strlcpy(req.payload, p.ackJson,     sizeof(req.payload));

    Serial.printf("📤 relay_ack → AWS: %s\n", req.payload);

    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — relay_ack dropped");
    }
    clearPending(idx);
}

void publishNoAck(int idx) {
    PendingCmd& p = pendingCmd[idx];

    StaticJsonDocument<256> doc;
    doc["type"]     = "no_ack";
    doc["UId"]      = DEVICE_ID;
    doc["device"]   = p.deviceId;
    doc["actuator"] = p.actuator;
    doc["attempts"] = CMD_MAX_RETRIES + 1;

    MqttPubReq req;
    strlcpy(req.topic, publish_topic, sizeof(req.topic));
    serializeJson(doc, req.payload, sizeof(req.payload));

    Serial.printf("❌ NO ACK — %s (%s) after %d attempts → AWS: %s\n",
                  p.deviceId, p.actuator, CMD_MAX_RETRIES + 1, req.payload);

    setError(ERR_NO_ACK);
    diag.cmdNoAck++;

    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — no_ack dropped");
    }
    clearPending(idx);
}

void clearPending(int idx) {
    pendingCmd[idx].active = false;
}

// =============================================================================
//  SECTION 22 — ACK IDENTIFICATION & HANDLER
// =============================================================================

bool isValveAck(DynamicJsonDocument& doc) {
    const char* nodeLabel = doc["node"] | "";
    return (strcasestr(nodeLabel, "valve")    != nullptr ||
            strcasestr(nodeLabel, "solenoid") != nullptr);
}

void handleLoRaAck(DynamicJsonDocument& loraDoc, uint8_t srcNode) {
    bool valve = isValveAck(loraDoc);
    int  idx   = valve ? PENDING_VALVE_IDX : PENDING_MOTOR_IDX;

    PendingCmd& p = pendingCmd[idx];

    if (!p.active) {
        Serial.printf("⏭ Duplicate/stale ACK from 0x%02X — slot already cleared\n", srcNode);
        return;
    }

    int attemptNum = p.retryCount + 1;
    Serial.printf("✅ ACK received on attempt %d/3 from %s (%s)\n",
                  attemptNum, p.deviceId, p.actuator);

    if      (attemptNum == 1) diag.cmdAckAttempt1++;
    else if (attemptNum == 2) diag.cmdAckAttempt2++;
    else                      diag.cmdAckAttempt3++;

    clearError();   // ACK received → clear any previous error

    StaticJsonDocument<320> mqttDoc;
    mqttDoc["type"]   = "relay_ack";
    mqttDoc["UId"]    = DEVICE_ID;
    mqttDoc["device"] = loraDoc["from"];

    const char* rawStatus = loraDoc["status"] | "";

    if (valve) {
        if (strstr(rawStatus, " B ") || strstr(rawStatus, "B O") || strstr(rawStatus, "B C"))
            mqttDoc["actuator"] = "Valve B";
        else
            mqttDoc["actuator"] = "Valve A";
        mqttDoc["state"] = strcasestr(rawStatus, "open") ? "open" : "close";
    } else {
        mqttDoc["actuator"] = "motor";
        mqttDoc["node"]     = loraDoc["node"];
        if      (strcasecmp(rawStatus, "Motor On")  == 0) mqttDoc["state"] = "on";
        else if (strcasecmp(rawStatus, "Motor Off") == 0) mqttDoc["state"] = "off";
        else                                               mqttDoc["state"] = rawStatus;
    }

    if (loraDoc.containsKey("FId")) mqttDoc["FId"] = loraDoc["FId"];
    if (loraDoc.containsKey("BId")) mqttDoc["BId"] = loraDoc["BId"];

    serializeJson(mqttDoc, p.ackJson, sizeof(p.ackJson));
    publishAck(idx);
}

// =============================================================================
//  SECTION 23 — POWER HANDLER
// =============================================================================

void handleLoRaPower(DynamicJsonDocument& loraDoc) {
    StaticJsonDocument<256> mqttDoc;
    mqttDoc["type"]   = "power";
    mqttDoc["UId"]    = DEVICE_ID;
    mqttDoc["device"] = loraDoc["from"] | "";

    if (loraDoc["V"].is<float>()) mqttDoc["V"] = loraDoc["V"].as<float>();
    else                          mqttDoc["V"] = loraDoc["V"] | "0";
    if (loraDoc["I"].is<float>()) mqttDoc["I"] = loraDoc["I"].as<float>();
    else                          mqttDoc["I"] = loraDoc["I"] | "0";
    if (loraDoc["P"].is<float>()) mqttDoc["P"] = loraDoc["P"].as<float>();
    else                          mqttDoc["P"] = loraDoc["P"] | "0";

    MqttPubReq req;
    strlcpy(req.topic, publish_power_topic, sizeof(req.topic));
    serializeJson(mqttDoc, req.payload, sizeof(req.payload));

    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) == pdTRUE)
        Serial.printf("📤 POWER queued: %s\n", req.payload);
    else {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — POWER dropped");
    }
}

// =============================================================================
//  SECTION 24 — PACKET PROCESSING
// =============================================================================

void processRxPacket(LoRaRxPacket& pkt) {
    String payload((char*)pkt.buf, pkt.len);
    payload.trim();

    // Strip leading garbage bytes before '{'
    int js = payload.indexOf('{');
    if (js < 0 || !payload.endsWith("}")) {
        diag.rxDroppedJson++;
        setError(ERR_INVALID_PKT);
        Serial.printf("❌ Invalid packet from 0x%02X\n", pkt.src);
        return;
    }
    if (js > 0) payload = payload.substring(js);

    DynamicJsonDocument doc(512);
    if (deserializeJson(doc, payload) != DeserializationError::Ok) {
        diag.rxDroppedJson++;
        setError(ERR_JSON_PARSE);
        Serial.printf("❌ JSON parse fail src:0x%02X\n", pkt.src);
        return;
    }

    Serial.printf("\n📥 LoRa 0x%02X→0x%02X  RSSI:%d  SNR:%.1f\n",
                  pkt.src, pkt.dest, pkt.rssi, pkt.snr);
    Serial.printf("   Payload: %s\n", payload.c_str());

    const char* type = doc["type"] | "";
    if      (strcmp(type, "ack")     == 0) handleLoRaAck(doc, pkt.src);
    else if (strcmp(type, "power")   == 0) handleLoRaPower(doc);
    else if (strcmp(type, "ota_ack") == 0) handleLoRaOtaAck(doc);
    else    Serial.printf("⏭ Unhandled LoRa type: '%s'\n", type);
}

// =============================================================================
//  SECTION 25 — APP TASK  (Core 1, pri 1)
// =============================================================================

void appTask(void* param) {
    static uint32_t lastDiag = 0;

    for (;;) {
        // 1. Drain RX — ACKs clear pending slots
        LoRaRxPacket pkt;
        while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) {
            processRxPacket(pkt);
        }

        // 2. Retry ticker
        uint32_t now = millis();
        for (int i = 0; i < 2; i++) {
            PendingCmd& p = pendingCmd[i];
            if (!p.active)           continue;
            if (now < p.nextRetryMs) continue;

            if (p.retryCount < CMD_MAX_RETRIES) {
                p.retryCount++;
                diag.cmdRetries++;
                Serial.printf("\n🔁 [Attempt %d/3] Retry → node:0x%02X  dev:%s  act:%s\n",
                              p.retryCount + 1, p.destNode, p.deviceId, p.actuator);
                enqueueTxLo(p.destNode, NODE3_ID,
                            (const uint8_t*)p.payload, p.payloadLen);
                p.nextRetryMs = now + CMD_RETRY_MS;
            } else {
                Serial.printf("\n❌ All 3 attempts exhausted for %s (%s)\n",
                              p.deviceId, p.actuator);
                publishNoAck(i);
            }
        }

        // 2b. OTA retry ticker (single in-flight job)
        if (otaPending.active && now >= otaPending.nextRetryMs) {
            if (otaPending.retryCount < OTA_MAX_RETRIES) {
                otaPending.retryCount++;
                Serial.printf("\n🔁 [OTA attempt %d/3] Retry → node:0x%02X  dev:%s\n",
                              otaPending.retryCount + 1, otaPending.destNode, otaPending.deviceId);
                enqueueTxLo(otaPending.destNode, NODE3_ID,
                            (const uint8_t*)otaPending.payload, otaPending.payloadLen);
                otaPending.nextRetryMs = now + OTA_RETRY_MS;
            } else {
                Serial.printf("\n❌ OTA — all 3 attempts exhausted for %s\n", otaPending.deviceId);
                publishOtaAck(false, otaPending.deviceId, otaPending.nodeType, "no ack after 3 attempts");
                otaPending.active = false;
            }
        }

        // 3. Diagnostics every 5 min
        if (now - lastDiag >= 5UL * 60 * 1000) {
            lastDiag = now;
            printDiagnostics();
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// =============================================================================
//  SECTION 26 — RADIO REINIT
// =============================================================================

void reinitLoRaRadio() {
    LoRa.end();
    pinMode(LORA_RST, OUTPUT);
    digitalWrite(LORA_RST, LOW);  delay(10);
    digitalWrite(LORA_RST, HIGH); delay(10);
    if (!LoRa.begin(LORA_FREQ)) {
        Serial.println("❌ LoRa reinit failed");
        dState.loraActive = false;
        return;
    }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    LoRa.receive();
    dState.loraActive = true;
    Serial.println("✅ LoRa reinitialised — radio in RX mode");
}

// =============================================================================
//  SECTION 27 — OLED DISPLAY HELPERS
// =============================================================================

// Convert LoRa RSSI (dBm) → 0–4 bars
int rssiToLoRaBars(int rssi) {
    if (rssi >= -60)  return 4;
    if (rssi >= -75)  return 3;
    if (rssi >= -90)  return 2;
    if (rssi >= -105) return 1;
    return 0;
}

// Convert GSM CSQ (0–31) → 0–4 bars
int csmToGsmBars(int csq) {
    if (csq >= 20) return 4;
    if (csq >= 15) return 3;
    if (csq >= 10) return 2;
    if (csq >=  5) return 1;
    return 0;
}

// Draw vertical signal bar chart at (x,y), 4 bars, 16px tall total
// barsFilled: how many bars are filled (0–4)
void oledDrawSignalBars(int x, int y, int barsFilled, int maxBars = 4) {
    const int barW = 4;
    const int gap  = 2;
    const int maxH = 14;   // tallest bar height

    for (int i = 0; i < maxBars; i++) {
        int h  = 3 + (i * (maxH - 3)) / (maxBars - 1);   // 3px → 14px
        int bx = x + i * (barW + gap);
        int by = y + (maxH - h);
        if (i < barsFilled)
            display.fillRect(bx, by, barW, h, SSD1306_WHITE);
        else
            display.drawRect(bx, by, barW, h, SSD1306_WHITE);
    }
}

// =============================================================================
//  SECTION 28 — OLED RENDER
// =============================================================================

void oledRender() {
    if (!oledOK) return;

    display.clearDisplay();

    display.setTextSize(1);
    display.setTextColor(SSD1306_WHITE);
    display.setCursor(0, 0);
    display.print("LoRa");
    oledDrawSignalBars(28, 0, rssiToLoRaBars(dState.loraRSSI));

    display.setCursor(72, 0);
    display.print("GSM");
    oledDrawSignalBars(96, 0, csmToGsmBars(dState.gsmRSSI));

    display.setCursor(0, 16);
    display.print("LORA:");
    display.print(dState.loraActive ? "OK" : "FAIL");

    display.setCursor(64, 16);
    display.print("MQTT:");
    display.print(dState.mqttOnline ? "OK" : "FAIL");

    display.setCursor(0, 32);
    display.print("CMD:");
    display.print((const char*)dState.lastCmd);

    display.setCursor(0, 48);
    display.print("ERR:");
    uint16_t err = dState.errorCode;
    if (err == ERR_NONE)
        display.print("--");
    else
        display.print(err);

    display.setCursor(80, 48);
    char rssiBuf[12];
    snprintf(rssiBuf, sizeof(rssiBuf), "%ddBm", dState.loraRSSI);
    display.print(rssiBuf);

    display.display();
}
// =============================================================================
//  SECTION 29 — DISPLAY TASK  (Core 1, pri 1 — same core as appTask, lower)
// =============================================================================

void displayTask(void* param) {
    for (;;) {
        oledRender();
        vTaskDelay(pdMS_TO_TICKS(500));   // refresh 2× per second
    }
}

// =============================================================================
//  SECTION 30 — DIAGNOSTICS
// =============================================================================

void printDiagnostics() {
    Serial.println("\n══════════ GATEWAY v4.5 DIAGNOSTICS ══════════");
    Serial.printf("  LoRa RX total:     %lu\n", diag.rxTotal);
    Serial.printf("  RX dest drops:     %lu\n", diag.rxDroppedDest);
    Serial.printf("  RX JSON drops:     %lu\n", diag.rxDroppedJson);
    Serial.printf("  LoRa TX sent:      %lu\n", diag.txSent);
    Serial.printf("  TX queue full:     %lu\n", diag.txQueueFull);
    Serial.printf("  CMD retries:       %lu\n", diag.cmdRetries);
    Serial.printf("  ACK attempt 1:     %lu\n", diag.cmdAckAttempt1);
    Serial.printf("  ACK attempt 2:     %lu\n", diag.cmdAckAttempt2);
    Serial.printf("  ACK attempt 3:     %lu\n", diag.cmdAckAttempt3);
    Serial.printf("  CMD no-ack:        %lu\n", diag.cmdNoAck);
    Serial.printf("  MQTT pub OK:       %lu\n", diag.mqttPubOk);
    Serial.printf("  MQTT pub fail:     %lu\n", diag.mqttPubFail);
    Serial.printf("  MQTT reconnects:   %lu\n", diag.mqttReconnects);
    Serial.printf("  LTE watchdogs:     %lu\n", diag.lteWatchdogFires);
    Serial.printf("  Radio reinits:     %lu\n", diag.radioReinits);
    Serial.printf("  LoRa RSSI:         %d dBm\n", dState.loraRSSI);
    Serial.printf("  GSM CSQ:           %d\n", dState.gsmRSSI);
    Serial.printf("  Error code:        %d\n", dState.errorCode);
    Serial.printf("  MQTT online:       %s\n",  mqttOnline ? "YES" : "NO");
    Serial.printf("  LoRa active:       %s\n",  dState.loraActive ? "YES" : "NO");
    Serial.printf("  Free heap:         %u B\n", esp_get_free_heap_size());
    Serial.printf("  rxQueue:           %u/%d\n", uxQueueMessagesWaiting(rxQueue),      RX_QUEUE_DEPTH);
    Serial.printf("  txHiQueue:         %u/%d\n", uxQueueMessagesWaiting(txHiQueue),    TX_HI_QUEUE_DEPTH);
    Serial.printf("  txLoQueue:         %u/%d\n", uxQueueMessagesWaiting(txLoQueue),    TX_LO_QUEUE_DEPTH);
    Serial.printf("  mqttPubQueue:      %u/%d\n", uxQueueMessagesWaiting(mqttPubQueue), MQTT_PUB_QUEUE_DEPTH);
    Serial.printf("  loraTask    HWM:   %u B\n", uxTaskGetStackHighWaterMark(loraTaskHandle));
    Serial.printf("  mqttTask    HWM:   %u B\n", uxTaskGetStackHighWaterMark(mqttTaskHandle));
    Serial.printf("  appTask     HWM:   %u B\n", uxTaskGetStackHighWaterMark(appTaskHandle));
    Serial.printf("  displayTask HWM:   %u B\n", uxTaskGetStackHighWaterMark(displayTaskHandle));

    const char* names[2] = { "Valve(0xD1)", "Motor(0xD2)" };
    for (int i = 0; i < 2; i++) {
        if (pendingCmd[i].active)
            Serial.printf("  ⏳ Pending[%s]: dev=%s  attempt=%d/3  timeout_in=%lu s\n",
                          names[i], pendingCmd[i].deviceId,
                          pendingCmd[i].retryCount + 1,
                          (pendingCmd[i].nextRetryMs - millis()) / 1000);
    }
    Serial.println("═══════════════════════════════════════════════\n");
}

// =============================================================================
//  SECTION 31 — SETUP
// =============================================================================

void setup() {
    SerialMon.begin(115200);
    SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
    delay(2000);

    Wire.begin(OLED_SDA, OLED_SCL);
    if (display.begin(SSD1306_SWITCHCAPVCC, OLED_ADDR)) {
        oledOK = true;
        display.clearDisplay();
        display.setTextSize(1);
        display.setTextColor(SSD1306_WHITE);
        display.setCursor(20, 20);
        display.println("AgriInverse GW");
        display.setCursor(30, 35);
        display.println("v4.5 starting");
        display.display();
        Serial.println("✅ OLED ready");
    } else {
        Serial.println("⚠️ OLED not found — continuing without display");
    }

    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    txHiQueue    = xQueueCreate(TX_HI_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    txLoQueue    = xQueueCreate(TX_LO_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));

    if (!rxQueue || !txHiQueue || !txLoQueue || !mqttPubQueue) {
        Serial.println("❌ Queue creation failed — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
    if (!LoRa.begin(LORA_FREQ)) {
        Serial.println("❌ LoRa init failed — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    Serial.println("✅ LoRa ready — SF7/BW125/CR5/0xAB");

    xTaskCreatePinnedToCore(loraTask,    "LoRa",    6144,  NULL, 3, &loraTaskHandle,    0);
    xTaskCreatePinnedToCore(mqttTask,    "MQTT",    12288, NULL, 3, &mqttTaskHandle,    1);
    xTaskCreatePinnedToCore(appTask,     "App",     6144,  NULL, 2, &appTaskHandle,     1);
    xTaskCreatePinnedToCore(displayTask, "Display", 4096,  NULL, 1, &displayTaskHandle, 1);

    Serial.println("✅ AgriInverse gateway v4.5 started");
    vTaskSuspend(NULL);
}

void loop() {
    vTaskDelay(portMAX_DELAY);
}
