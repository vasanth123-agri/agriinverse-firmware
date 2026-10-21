// =============================================================================
//  AgriInverse — ESP32 LoRa/LTE Gateway  v5.1
//
//  CHANGE FROM v5.0:
//  Valve relay_ack now forwards the node's battery status to AWS:
//    "bat" SoC %, "bv" volts, "ib" mA (+chg/-dis), "cs" 0 dis/1 chg/2 full/3 idle
//  Fields are only added when the node sends them (valve node v4.1+), so
//  older nodes still produce the same relay_ack as before.
//
//  CHANGE FROM v4.5:
//  Valve nodes (VId 1..15, randomly placed) are now reached via flood-relay
//  mesh instead of a single fixed LoRa address. All valve command / OTA /
//  ack / ota_ack traffic transmits to BROADCAST_ID (0x00); each valve node
//  decides locally whether to consume (VId match) or relay (hop--, jitter,
//  rebroadcast) a packet. See valve_node_final.ino for the node side.
//
//  Motor/appliances/sensor/fertigation nodes are UNCHANGED — still single
//  fixed LoRa address, single in-flight slot, same as v4.5.
//
//  RADIO ARCHITECTURE:
//  Single loraTask owns radio exclusively (Core 0, pri 3).
//  TX → blocking endPacket() → immediate LoRa.receive() → no race condition.
//
//  VALVE COMMAND FLOW (mesh):
//  1. MQTT cmd received (must include VId 1-15) → build broadcast packet with
//     fresh msgId + hop=MESH_HOP_COUNT → TX to BROADCAST_ID → arm pendingValve[VId]
//  2. ACK (VId+replyTo==msgId) within 90s? → YES: publish relay_ack, done
//  3. No ACK → retry SAME msgId, attempt 2/3
//  4. ACK?    → YES: publish relay_ack, done
//  5. No ACK → retry SAME msgId, attempt 3/3
//  6. ACK?    → YES: publish relay_ack, done
//  7. No ACK → publish no_ack, done
//
//  OLED LAYOUT (128×64, SSD1306):
//  ┌──────────────────────────────┐
//  │ [LoRa▂▄▆█]  [GSM ▂▄▆█]     │  row 0  — signal bars
//  │ LoRa:ACTIVE   GSM:CONNECTED  │  row 18 — status labels
//  │ MQTT: CONNECTED              │  row 30 — MQTT status
//  │ CMD:  V7 A open               │  row 42 — last command
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
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

#include "certificate.h"
#include "privatekey.h"
#include "root_ca.h"

// =============================================================================
//  SECTION 1 — IDENTITY & PINS
// =============================================================================

#define DEVICE_ID    "66"

#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY  4

#define LORA_SS      5
#define LORA_RST     14
#define LORA_DIO0    26
#define LORA_FREQ    433E6

#define NODE3_ID     0xBB
#define NODE6_ID     0xD2   // Motor relay node
#define NODE7_ID     0xD3   // Appliances node
#define NODE8_ID     0xD4   // Sensor node
#define NODE9_ID     0xD5   // Fertigation node
// NOTE: NODE5_ID (fixed valve address 0xD1) is retired — valves are now
// addressed by VId over the mesh (see SECTION 4).

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

#define BROADCAST_ID    0xD1     // all valve-mesh traffic (cmd/ota/ack/ota_ack)
#define MESH_HOP_COUNT  6        // raise if nodes log "hop exhausted"
#define MAX_VALVES      15       // VId 1..15 → index 0..14

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
#define MAX_MQTT_FAILURES  5   // consecutive publish failures before forcing a reconnect

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

// Motor keeps the original single-slot pending model — unchanged from v4.5.
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

// NEW — one slot per valve VId (1..15), mesh-routed via BROADCAST_ID.
struct PendingValve {
    bool     active      = false;
    uint8_t  vId         = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    uint32_t msgId       = 0;      // must match ACK's "replyTo"
    char     payload[256]= {};
    uint16_t payloadLen  = 0;
    char     deviceId[32]= {};
    char     ackJson[320]= {};
};

// OTA — extended with vId/msgId for valve-mesh targets; destNode still used
// for motor/appliances/sensor/fertigation (fixed-address) targets.
struct OtaPending {
    bool     active      = false;
    uint8_t  destNode    = 0;    // unused now — kept for possible future fixed-address targets
    uint8_t  vId         = 0;    // 1..15 for valve mesh target; 0 + motorMesh=true for motor
    bool     motorMesh   = false;
    uint32_t msgId       = 0;    // matched against ack/ota_ack "replyTo"
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[256]= {};
    uint16_t payloadLen  = 0;
    char     deviceId[32]= {};
    char     nodeType[24]= {};   // "valve node" / "motor node"
    char     ackJson[320]= {};
};

// Job handed from mqttTask -> otaTask so a slow/blocking self-OTA download
// never stalls modem.mqtt_handle() (which must be pumped every 10ms or MQTT drops).
struct SelfOtaJob {
    char deviceType[24] = {};
    char uid[32]        = {};
    char ssid[64]       = {};
    char pass[64]       = {};
    char url[220]       = {};
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
    char     lastCmd[32]  = "--";    // e.g. "V7 A open"
};
static volatile DisplayState dState;

// =============================================================================
//  SECTION 9 — RTOS OBJECTS
// =============================================================================

static QueueHandle_t  rxQueue;
static QueueHandle_t  txHiQueue;
static QueueHandle_t  txLoQueue;
static QueueHandle_t  mqttPubQueue;
static QueueHandle_t  otaJobQueue;   // hands OTA work off mqttTask onto its own low-priority task

static TaskHandle_t   loraTaskHandle    = nullptr;
static TaskHandle_t   mqttTaskHandle    = nullptr;
static TaskHandle_t   appTaskHandle     = nullptr;
static TaskHandle_t   displayTaskHandle = nullptr;
static TaskHandle_t   otaTaskHandle     = nullptr;

#define PENDING_MOTOR_IDX  1
static PendingCmd pendingCmd[2];              // index 1 = motor (index 0 retired)
static PendingValve pendingValve[MAX_VALVES]; // index 0..14 = VId 1..15

// Single in-flight OTA job at a time (simple queueing model — extend to an
// array if you need concurrent OTA pushes to multiple targets).
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
void  handleLoRaAck(DynamicJsonDocument& loraDoc, uint8_t srcNode);       // motor only now
void  handleMeshAck(DynamicJsonDocument& loraDoc, uint8_t srcNode);       // valve mesh ack + ota_ack
void  handleLoRaPower(DynamicJsonDocument& doc);
void  mqttCallback(const char* topic, const uint8_t* payload, uint32_t len);
bool  mqttConnectOnce();
void  enqueueValveCommand(uint8_t vId, const String& device, const String& valve,
                          const String& cmd, const String& fid, const String& bid);
void  enqueueMotorCommand(const String& device, const String& state,
                          const String& fid, const String& bid);
void  enqueueMqttStatus(bool connected);
void  armRetry(int idx, uint8_t dest, const char* device,
               const char* actuator, const char* jsonBuf, uint16_t len);
void  publishAck(int idx);
void  publishNoAck(int idx);
void  clearPending(int idx);
void  publishValveAck(int idx);
void  publishValveNoAck(int idx);
void  valveRetryTick();
void  setError(uint16_t code);
void  clearError();
void  printDiagnostics();
int   parseVId(DynamicJsonDocument& doc);   // accepts VId as JSON number or string

// OTA
void  handleOtaCommand(DynamicJsonDocument& doc);
void  handleOtaCommandValveBranch(DynamicJsonDocument& doc);
void  handleOtaCommandMotorBranch(DynamicJsonDocument& doc);
void  otaTask(void* param);
void  publishOtaAck(bool success, const char* deviceId, const char* nodeType, const char* detail);
void  handleLoRaOtaAck(DynamicJsonDocument& loraDoc);   // non-mesh (motor/appliances/etc) only now

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

// AWS/API Gateway may serialize VId as a JSON number (7) or a JSON string
// ("7") depending on how the payload was built. Parse both forms explicitly
// instead of relying on ArduinoJson's `| 0` numeric coercion, which doesn't
// reliably convert a quoted numeric string in all cases.
int parseVId(DynamicJsonDocument& doc) {
    if (!doc.containsKey("VId")) return -1;
    JsonVariantConst v = doc["VId"];
    if (v.is<int>()) return v.as<int>();
    if (v.is<const char*>()) {
        const char* s = v.as<const char*>();
        if (!s || !strlen(s)) return -1;
        return atoi(s);
    }
    return -1;
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

            // Gateway must accept both packets addressed directly to it AND
            // BROADCAST_ID traffic (valve-mesh commands/OTA/ack/ota_ack all
            // transmit to BROADCAST_ID since intermediate relays need to
            // hear them too).
            if (rx.dest != NODE3_ID && rx.dest != BROADCAST_ID) {
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

// Valve commands now target a VId (1..15) over the mesh instead of a fixed
// physical LoRa address. Every enqueue gets a fresh msgId + hop count;
// retries reuse the SAME msgId so a node that already executed the command
// (reached via a different relay path) answers with an ack-only duplicate
// response instead of re-actuating the solenoid.
void enqueueValveCommand(uint8_t vId, const String& device, const String& valve,
                         const String& cmd, const String& fid, const String& bid) {
    if (vId < 1 || vId > MAX_VALVES) {
        Serial.printf("⚠️ enqueueValveCommand: bad VId %d\n", vId);
        return;
    }

    uint32_t msgId = (uint32_t)esp_random();

    StaticJsonDocument<256> doc;
    doc["type"]    = "command_broadcast";
    doc["UId"]     = DEVICE_ID;
    doc["VId"]     = vId;
    doc["msgId"]   = msgId;
    doc["hop"]     = MESH_HOP_COUNT;
    doc["device"]  = device;
    doc["valve"]   = valve;
    doc["command"] = cmd;
    if (fid.length()) doc["FId"] = fid;
    if (bid.length()) doc["BId"] = bid;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(doc, buf, sizeof(buf));

    if (enqueueTxLo(BROADCAST_ID, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [Attempt 1/3] VALVE CMD → VId:%d msgId:%lu dev:%s valve:%s cmd:%s\n",
                      vId, (unsigned long)msgId, device.c_str(), valve.c_str(), cmd.c_str());

        char cmdStr[32];
        snprintf(cmdStr, sizeof(cmdStr), "V%d %s %s", vId, valve.c_str(), cmd.c_str());
        strlcpy((char*)dState.lastCmd, cmdStr, sizeof(dState.lastCmd));
        clearError();

        int idx = vId - 1;
        PendingValve& p = pendingValve[idx];
        p.active      = true;
        p.vId         = vId;
        p.retryCount  = 0;
        p.msgId       = msgId;
        p.nextRetryMs = millis() + CMD_RETRY_MS;
        p.payloadLen  = n;
        memset(p.ackJson, 0, sizeof(p.ackJson));
        memcpy(p.payload, buf, n);
        strlcpy(p.deviceId, device.c_str(), sizeof(p.deviceId));

        Serial.printf("⏱ Waiting for ACK — VId:%d msgId:%lu — timeout in %lu s\n",
                      vId, (unsigned long)msgId, CMD_RETRY_MS / 1000);
    } else {
        Serial.println("❌ Valve CMD: LoRa TX queue full — could not enqueue");
    }
}

// Motor command — unchanged from v4.5 (fixed address, single slot).
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
        // Valve commands must now carry VId (1..15) — that's how the
        // gateway knows which physical node to route through the mesh.
        int vId = parseVId(doc);
        if (vId < 1 || vId > MAX_VALVES) {
            Serial.println("⚠️ command_broadcast: missing/invalid VId for valve command");
            return;
        }
        const char* valve = doc["valve"]   | "";
        const char* cmd   = doc["command"] | "";
        const char* fid   = doc["FId"]     | "";
        const char* bid   = doc["BId"]     | "";
        if (!strlen(valve)) { Serial.println("⚠️ Missing 'valve' value"); return; }
        enqueueValveCommand((uint8_t)vId, device, valve, cmd, fid, bid);

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
//    "device": "valve node",     // "valve node" | "motor node" | "VMCgateway"
//    "VId":    7,                // REQUIRED when device == "valve node" (1..15)
//    "wifi_ssid": "farm-ap",
//    "wifi_pass": "secret",
//    "url":    "https://github.com/org/repo/releases/download/v1.2.0/node.bin"
//  }
//
//  Valve and motor OTA both route over the mesh (BROADCAST_ID + VId + msgId
//  + hop) — motor uses reserved VId:0. VMCgateway is the only device type
//  that self-flashes (joins wifi_ssid/wifi_pass, downloads url, reboots).
// =============================================================================

// Valve-node OTA — mesh-routed by VId, same envelope shape as commands.
// Motor node has no VId of its own (there's only one) — reserve VId:0 as
// the sentinel that means "the motor node" in the same mesh envelope shape
// valve commands use. Valve nodes relay it (their VId never equals 0), and
// the motor node consumes it by checking for VId:0 specifically.
#define MOTOR_MESH_VID 0

void handleOtaCommandValveBranch(DynamicJsonDocument& doc) {
    const char* deviceType = doc["device"] | "";
    int vId = parseVId(doc);

    if (vId < 1 || vId > MAX_VALVES) {
        Serial.println("⚠️ OTA: missing/invalid VId for valve node target");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing VId");
        return;
    }
    if (otaPending.active) {
        Serial.println("⚠️ OTA already in progress — ignoring new request until it completes");
        return;
    }
    const char* url = doc["url"] | "";
    if (!strlen(url)) {
        Serial.println("⚠️ OTA: missing 'url' field");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing url");
        return;
    }

    uint32_t msgId  = (uint32_t)esp_random();
    const char* fid = doc["FId"] | "";
    const char* bid = doc["BId"] | "";

    StaticJsonDocument<256> pkt;
    pkt["type"]  = "ota";
    pkt["UId"]   = DEVICE_ID;
    pkt["VId"]   = vId;
    pkt["msgId"] = msgId;
    pkt["hop"]   = MESH_HOP_COUNT;
    pkt["url"]   = url;
    if (doc.containsKey("wifi_ssid")) pkt["wifi"] = doc["wifi_ssid"];
    if (doc.containsKey("wifi_pass")) pkt["pass"] = doc["wifi_pass"];
    if (strlen(fid)) pkt["FId"] = fid;
    if (strlen(bid)) pkt["BId"] = bid;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(pkt, buf, sizeof(buf));
    if (n >= sizeof(buf)) {
        Serial.println("❌ OTA packet too large (256B) — shorten the URL");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "payload too large");
        return;
    }

    if (enqueueTxLo(BROADCAST_ID, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [OTA attempt 1/3] → VId:%d msgId:%lu url:%s\n",
                      vId, (unsigned long)msgId, url);

        otaPending.active      = true;
        otaPending.destNode    = 0;
        otaPending.vId         = (uint8_t)vId;
        otaPending.motorMesh   = false;
        otaPending.msgId       = msgId;
        otaPending.retryCount  = 0;
        otaPending.nextRetryMs = millis() + OTA_RETRY_MS;
        otaPending.payloadLen  = n;
        memset(otaPending.ackJson, 0, sizeof(otaPending.ackJson));
        memcpy(otaPending.payload, buf, n);
        strlcpy(otaPending.deviceId, doc["UId"] | DEVICE_ID, sizeof(otaPending.deviceId));
        strlcpy(otaPending.nodeType, deviceType, sizeof(otaPending.nodeType));
    } else {
        Serial.println("❌ OTA: LoRa TX queue full — could not enqueue");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "tx queue full");
    }
}

// Motor OTA — same mesh envelope/relay/dedupe as valve OTA, using the
// reserved VId:0 sentinel instead of a VId 1-15. This means motor OTA now
// reaches the motor node even if it's not in direct radio range of the
// gateway, by hopping through valve nodes exactly like a valve command does.
void handleOtaCommandMotorBranch(DynamicJsonDocument& doc) {
    const char* deviceType = doc["device"] | "";

    if (otaPending.active) {
        Serial.println("⚠️ OTA already in progress — ignoring new request until it completes");
        return;
    }
    const char* url = doc["url"] | "";
    if (!strlen(url)) {
        Serial.println("⚠️ OTA: missing 'url' field");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing url");
        return;
    }

    uint32_t msgId  = (uint32_t)esp_random();
    const char* fid = doc["FId"] | "";
    const char* bid = doc["BId"] | "";

    StaticJsonDocument<256> pkt;
    pkt["type"]  = "ota";
    pkt["UId"]   = DEVICE_ID;
    pkt["VId"]   = MOTOR_MESH_VID;   // 0 — reserved sentinel for the motor node
    pkt["msgId"] = msgId;
    pkt["hop"]   = MESH_HOP_COUNT;
    pkt["url"]   = url;
    if (doc.containsKey("wifi_ssid")) pkt["wifi"] = doc["wifi_ssid"];
    if (doc.containsKey("wifi_pass")) pkt["pass"] = doc["wifi_pass"];
    if (strlen(fid)) pkt["FId"] = fid;
    if (strlen(bid)) pkt["BId"] = bid;

    char buf[256];
    uint16_t n = (uint16_t)serializeJson(pkt, buf, sizeof(buf));
    if (n >= sizeof(buf)) {
        Serial.println("❌ OTA packet too large (256B) — shorten the URL");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "payload too large");
        return;
    }

    if (enqueueTxLo(BROADCAST_ID, NODE3_ID, (const uint8_t*)buf, n)) {
        Serial.printf("📡 [OTA attempt 1/3] → MOTOR msgId:%lu url:%s\n", (unsigned long)msgId, url);

        otaPending.active      = true;
        otaPending.destNode    = 0;
        otaPending.vId         = MOTOR_MESH_VID;
        otaPending.motorMesh   = true;
        otaPending.msgId       = msgId;
        otaPending.retryCount  = 0;
        otaPending.nextRetryMs = millis() + OTA_RETRY_MS;
        otaPending.payloadLen  = n;
        memset(otaPending.ackJson, 0, sizeof(otaPending.ackJson));
        memcpy(otaPending.payload, buf, n);
        strlcpy(otaPending.deviceId, doc["UId"] | DEVICE_ID, sizeof(otaPending.deviceId));
        strlcpy(otaPending.nodeType, deviceType, sizeof(otaPending.nodeType));
    } else {
        Serial.println("❌ OTA: LoRa TX queue full — could not enqueue");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "tx queue full");
    }
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

// Self-OTA for gateway-type devices (VMCgateway, appliances gateway,
// fertigation gateway, sensor gateway) — joins wifi_ssid/wifi_pass
// separately from the LTE modem link, downloads url, and flashes itself.
void selfOtaUpdate(const char* deviceType, const char* uid,
                   const char* ssid, const char* pass, const char* url) {
    if (!ssid || !strlen(ssid) || !pass || !strlen(pass)) {
        Serial.println("❌ Gateway OTA: missing wifi_ssid/wifi_pass");
        publishOtaAck(false, uid, deviceType, "missing wifi credentials");
        return;
    }

    Serial.printf("📶 Gateway OTA: joining WiFi '%s'...\n", ssid);
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, pass);

    uint32_t deadline = millis() + 20000UL;   // 20 s to join
    while (WiFi.status() != WL_CONNECTED && millis() < deadline) {
        vTaskDelay(pdMS_TO_TICKS(500));
    }

    if (WiFi.status() != WL_CONNECTED) {
        Serial.println("❌ Gateway OTA: WiFi join failed/timed out");
        publishOtaAck(false, uid, deviceType, "wifi join failed");
        WiFi.mode(WIFI_OFF);
        return;
    }

    Serial.printf("✅ WiFi joined, IP: %s — starting download from %s\n",
                  WiFi.localIP().toString().c_str(), url);
    Serial.printf("Free heap before OTA: %u bytes\n", ESP.getFreeHeap());

    const int MAX_ATTEMPTS = 3;
    t_httpUpdate_return result = HTTP_UPDATE_FAILED;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        Serial.printf("Gateway OTA attempt %d/%d...\n", attempt, MAX_ATTEMPTS);

        WiFiClientSecure client;
        client.setInsecure();
        client.setTimeout(15000);

        httpUpdate.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
        httpUpdate.rebootOnUpdate(true);
        result = httpUpdate.update(client, url);

        if (result == HTTP_UPDATE_OK) break;

        Serial.printf("  attempt %d failed: %d %s\n", attempt,
                      httpUpdate.getLastError(), httpUpdate.getLastErrorString().c_str());
        if (attempt < MAX_ATTEMPTS) {
            vTaskDelay(pdMS_TO_TICKS(3000));
        }
    }

    switch (result) {
        case HTTP_UPDATE_FAILED:
            Serial.printf("❌ Gateway OTA failed after %d attempts: %d %s\n",
                          MAX_ATTEMPTS, httpUpdate.getLastError(),
                          httpUpdate.getLastErrorString().c_str());
            publishOtaAck(false, uid, deviceType, httpUpdate.getLastErrorString().c_str());
            break;
        case HTTP_UPDATE_NO_UPDATES:
            Serial.println("ℹ️ Gateway OTA: server reported no update needed");
            publishOtaAck(false, uid, deviceType, "no update available at url");
            break;
        case HTTP_UPDATE_OK:
            Serial.println("✅ Gateway OTA applied — rebooting");
            break;
    }

    WiFi.mode(WIFI_OFF);
}

// =============================================================================
//  SELF-OTA TASK  (Core 1, pri 1 — lowest, never competes with LoRa/MQTT)
// =============================================================================

void otaTask(void* param) {
    for (;;) {
        SelfOtaJob job;
        if (xQueueReceive(otaJobQueue, &job, portMAX_DELAY) == pdTRUE) {
            Serial.println("🚀 otaTask: starting self-OTA job");
            selfOtaUpdate(job.deviceType, job.uid, job.ssid, job.pass, job.url);
        }
    }
}

void handleOtaCommand(DynamicJsonDocument& doc) {
    const char* deviceType = doc["device"] | "";
    if (!strlen(deviceType)) {
        Serial.println("⚠️ OTA: missing 'device' field");
        return;
    }

    // Valve node — mesh-routed by VId.
    if (strcasecmp(deviceType, "valve node") == 0) {
        handleOtaCommandValveBranch(doc);
        return;
    }

    // Motor node — mesh-routed via reserved VId:0 sentinel (same broadcast
    // mesh as valve, so it reaches the motor node even out of direct range).
    if (strcasecmp(deviceType, "motor node") == 0) {
        handleOtaCommandMotorBranch(doc);
        return;
    }

    // This gateway is a VMC gateway — the only self-flash target it should
    // ever accept is "VMCgateway". Anything else is an unrecognized/typo'd
    // device string, NOT an implicit "flash me".
    if (strcasecmp(deviceType, "VMC gateway") != 0) {
        Serial.printf("⚠️ OTA: unrecognized device type '%s' — rejecting (not VMC gateway, valve node, or motor node)\n",
                      deviceType);
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "unrecognized device type");
        return;
    }

    // Hand the job to otaTask instead of calling selfOtaUpdate() here:
    // this function runs ON mqttTask (called from inside modem.mqtt_handle()),
    // and a multi-second/minute blocking OTA download here would stall the
    // 10ms mqtt_handle() pump loop and cause MQTT to drop.
    const char* url  = doc["url"] | "";
    const char* ssid = doc["wifi_ssid"] | "";
    const char* pass = doc["wifi_pass"] | "";
    if (!strlen(url)) {
        Serial.println("⚠️ Gateway OTA: missing 'url' field");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing url");
        return;
    }

    SelfOtaJob job;
    strlcpy(job.deviceType, deviceType, sizeof(job.deviceType));
    strlcpy(job.uid, doc["UId"] | DEVICE_ID, sizeof(job.uid));
    strlcpy(job.ssid, ssid, sizeof(job.ssid));
    strlcpy(job.pass, pass, sizeof(job.pass));
    strlcpy(job.url, url, sizeof(job.url));

    if (xQueueSend(otaJobQueue, &job, 0) != pdTRUE) {
        Serial.println("⚠️ otaJobQueue full — OTA already in progress, dropping new request");
        publishOtaAck(false, job.uid, deviceType, "OTA already in progress");
    } else {
        Serial.println("📨 Self-OTA job queued — handing off to otaTask");
    }
}

// Non-mesh OTA ack — retired now that motor also uses the mesh envelope.
// Kept as a stub in case a future fixed-address node type is reintroduced;
// currently unreachable since otaPending is always either valve or motor mesh.
void handleLoRaOtaAck(DynamicJsonDocument& loraDoc) {
    Serial.println("⏭ handleLoRaOtaAck: no fixed-address OTA targets configured — ignoring");
}

// =============================================================================
//  SECTION 19 — MQTT CONNECT
//  Mirrors the stable sensor-gateway reference: cert set every attempt
//  inside mqttConnectOnce (not guarded), no disconnect-before-connect call,
//  no separate fast-restart-after-N-failures path. Simpler and known-stable.
// =============================================================================

bool mqttConnectOnce() {
    modem.mqtt_set_certificate(root_ca, certificate, privateKey);
    if (!modem.mqtt_connect(0, broker, broker_port, client_id, NULL, NULL, 120)) return false;
    modem.mqtt_set_callback(mqttCallback);
    modem.mqtt_subscribe(0, subscribe_topic, 0);
    modem.mqtt_subscribe(0, ota_subscribe_topic, 0);
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

    modem.mqtt_begin(true);
    modem.sendAT("+CMQTTCFG=\"keepalive\",0,120");
    modem.waitResponse(2000);
    modem.mqtt_set_callback(mqttCallback);

    enum State { CONNECTING, CONNECTED };
    State    state         = CONNECTING;
    uint32_t retryDelay    = MQTT_RETRY_MIN_MS;
    uint32_t nextRetry     = 0;
    uint32_t lastHandleMs  = 0;
    uint32_t lastSuccessMs = millis();
    uint32_t lastCsqMs     = 0;
    bool     notified      = false;
    uint8_t  failCount     = 0;   // consecutive PUBLISH failures — matches reference's MAX_MQTT_FAILURES pattern

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
            if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                // QoS 0 for everything, matching the reference gateway.
                // QoS 1 requires a PUBACK round-trip per message; the A7670
                // logged that as an "Unhandled" URC (+CMQTTPUB: 0,0) right
                // before a disconnect, so QoS 0 removes that ack bookkeeping
                // entirely instead of trying to get PUBACK handling right.
                int qos = 0;
                if (modem.mqtt_publish(0, req.topic, req.payload, qos)) {
                    diag.mqttPubOk++;
                    failCount = 0;
                    Serial.printf("✅ Published → [%s]: %s\n", req.topic, req.payload);
                } else {
                    diag.mqttPubFail++;
                    Serial.println("❌ Publish failed — re-queuing");
                    xQueueSend(mqttPubQueue, &req, 0);
                    // Repeated publish failures while mqtt_connected() still
                    // reports true usually means the session is actually
                    // dead server-side — force a reconnect rather than
                    // spinning on publish forever, same as the reference.
                    if (++failCount >= MAX_MQTT_FAILURES) {
                        failCount          = 0;
                        notified           = false;
                        state              = CONNECTING;
                        mqttOnline         = false;
                        dState.mqttOnline  = false;
                        nextRetry          = now + retryDelay;
                    }
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
                failCount        = 0;
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

// =============================================================================
//  SECTION 21 — RETRY HELPERS  (motor — unchanged from v4.5)
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
//  SECTION 21b — RETRY HELPERS  (NEW — valve mesh, 15 slots)
// =============================================================================

void publishValveAck(int idx) {
    PendingValve& p = pendingValve[idx];
    MqttPubReq req;
    strlcpy(req.topic,   publish_topic, sizeof(req.topic));
    strlcpy(req.payload, p.ackJson,     sizeof(req.payload));
    Serial.printf("📤 relay_ack → AWS: %s\n", req.payload);
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — relay_ack dropped");
    }
    p.active = false;
}

void publishValveNoAck(int idx) {
    PendingValve& p = pendingValve[idx];
    StaticJsonDocument<256> doc;
    doc["type"]     = "no_ack";
    doc["UId"]      = DEVICE_ID;
    doc["VId"]      = p.vId;
    doc["device"]   = p.deviceId;
    doc["actuator"] = "valve";
    doc["attempts"] = CMD_MAX_RETRIES + 1;

    MqttPubReq req;
    strlcpy(req.topic, publish_topic, sizeof(req.topic));
    serializeJson(doc, req.payload, sizeof(req.payload));
    Serial.printf("❌ NO ACK — VId:%d (%s) after %d attempts → AWS: %s\n",
                  p.vId, p.deviceId, CMD_MAX_RETRIES + 1, req.payload);

    setError(ERR_NO_ACK);
    diag.cmdNoAck++;
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.println("⚠️ mqttPubQueue full — no_ack dropped");
    }
    p.active = false;
}

// =============================================================================
//  SECTION 22 — ACK HANDLERS
// =============================================================================

// Motor-only now (valve traffic goes through handleMeshAck below).
void handleLoRaAck(DynamicJsonDocument& loraDoc, uint8_t srcNode) {
    PendingCmd& p = pendingCmd[PENDING_MOTOR_IDX];

    if (!p.active) {
        Serial.printf("⏭ Duplicate/stale ACK from 0x%02X — motor slot already cleared\n", srcNode);
        return;
    }

    int attemptNum = p.retryCount + 1;
    Serial.printf("✅ ACK received on attempt %d/3 from %s (%s)\n",
                  attemptNum, p.deviceId, p.actuator);

    if      (attemptNum == 1) diag.cmdAckAttempt1++;
    else if (attemptNum == 2) diag.cmdAckAttempt2++;
    else                      diag.cmdAckAttempt3++;

    clearError();

    StaticJsonDocument<320> mqttDoc;
    mqttDoc["type"]   = "relay_ack";
    mqttDoc["UId"]    = DEVICE_ID;
    mqttDoc["device"] = loraDoc["from"];

    const char* rawStatus = loraDoc["status"] | "";
    mqttDoc["actuator"] = "motor";
    mqttDoc["node"]     = loraDoc["node"];
    if      (strcasecmp(rawStatus, "Motor On")  == 0) mqttDoc["state"] = "on";
    else if (strcasecmp(rawStatus, "Motor Off") == 0) mqttDoc["state"] = "off";
    else                                               mqttDoc["state"] = rawStatus;

    if (loraDoc.containsKey("FId")) mqttDoc["FId"] = loraDoc["FId"];
    if (loraDoc.containsKey("BId")) mqttDoc["BId"] = loraDoc["BId"];

    serializeJson(mqttDoc, p.ackJson, sizeof(p.ackJson));
    publishAck(PENDING_MOTOR_IDX);
}

// NEW — valve-mesh "ack" and "ota_ack", matched by VId + replyTo(msgId)
// instead of a fixed slot / node-label sniff.
void handleMeshAck(DynamicJsonDocument& loraDoc, uint8_t srcNode) {
    const char* type = loraDoc["type"] | "";
    int vId          = loraDoc["VId"]     | -1;
    uint32_t replyTo = loraDoc["replyTo"] | 0;

    if (vId < 0 || vId > MAX_VALVES) {
        Serial.printf("⏭ %s: bad/missing VId — dropping\n", type);
        return;
    }

    if (strcmp(type, "ack") == 0) {
        // Valve command acks only — motor's non-OTA ack goes through
        // handleLoRaAck() since regular motor commands are still fixed-address.
        if (vId < 1) {
            Serial.println("⏭ ack: VId:0 is reserved for motor OTA only — dropping");
            return;
        }
        int idx = vId - 1;
        PendingValve& p = pendingValve[idx];
        if (!p.active || p.msgId != replyTo) {
            Serial.printf("⏭ Stale/duplicate ACK VId:%d (no matching pending msgId)\n", vId);
            return;
        }

        int attemptNum = p.retryCount + 1;
        if      (attemptNum == 1) diag.cmdAckAttempt1++;
        else if (attemptNum == 2) diag.cmdAckAttempt2++;
        else                      diag.cmdAckAttempt3++;
        clearError();

        StaticJsonDocument<320> mqttDoc;
        mqttDoc["type"]   = "relay_ack";
        mqttDoc["UId"]    = DEVICE_ID;
        mqttDoc["VId"]    = vId;
        mqttDoc["device"] = loraDoc["from"];

        const char* rawStatus = loraDoc["status"] | "";
        mqttDoc["status"]   = rawStatus;
        mqttDoc["actuator"] = strcasestr(rawStatus, " B ") ? "Valve B" : "Valve A";
        mqttDoc["state"]    = strcasestr(rawStatus, "open") ? "open" : "close";
        if (loraDoc.containsKey("FId")) mqttDoc["FId"] = loraDoc["FId"];
        if (loraDoc.containsKey("BId")) mqttDoc["BId"] = loraDoc["BId"];

        // v5.1 — battery status from valve node v4.1+ (forwarded as-is):
        //   bat = SoC %  (-1 = INA219 missing)
        //   bv  = battery volts
        //   ib  = battery current mA (+ charging, - discharging)
        //   cs  = 0 discharging, 1 charging, 2 full, 3 idle
        if (loraDoc.containsKey("bat")) mqttDoc["bat"] = loraDoc["bat"];
        if (loraDoc.containsKey("bv"))  mqttDoc["bv"]  = loraDoc["bv"];
        if (loraDoc.containsKey("ib"))  mqttDoc["ib"]  = loraDoc["ib"];
        if (loraDoc.containsKey("cs"))  mqttDoc["cs"]  = loraDoc["cs"];

        size_t ackLen = serializeJson(mqttDoc, p.ackJson, sizeof(p.ackJson));
        if (ackLen >= sizeof(((MqttPubReq*)0)->payload)) {
            // Would be cut off by the 256-byte MQTT payload buffer — drop
            // the optional fields rather than publish broken JSON.
            mqttDoc.remove("bv");
            mqttDoc.remove("ib");
            serializeJson(mqttDoc, p.ackJson, sizeof(p.ackJson));
            Serial.printf("⚠️ relay_ack %u B — dropped bv/ib to fit\n", (unsigned)ackLen);
        }
        Serial.printf("✅ ACK matched — VId:%d msgId:%lu attempt:%d/3\n",
                      vId, (unsigned long)replyTo, attemptNum);
        publishValveAck(idx);
        return;
    }

    if (strcmp(type, "ota_ack") == 0) {
        // VId:0 → motor OTA job; VId 1-15 → valve OTA job.
        bool matches = otaPending.active && otaPending.msgId == replyTo &&
                       ((vId == 0 && otaPending.motorMesh) ||
                        (vId >= 1 && !otaPending.motorMesh && otaPending.vId == (uint8_t)vId));
        if (!matches) {
            Serial.printf("⏭ Stale/duplicate OTA-ACK VId:%d\n", vId);
            return;
        }
        const char* status = loraDoc["status"] | "ok";
        bool ok = (strcasecmp(status, "fail") != 0);
        const char* detail = loraDoc["detail"] | "";
        Serial.printf("✅ OTA-ACK matched — VId:%d status:%s\n", vId, status);
        publishOtaAck(ok, otaPending.deviceId, otaPending.nodeType, detail);
        otaPending.active = false;
        return;
    }
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

    if (strcmp(type, "ack") == 0) {
        // Valve-mesh acks carry VId; motor's fixed-address ack does not.
        if (doc.containsKey("VId")) handleMeshAck(doc, pkt.src);
        else                        handleLoRaAck(doc, pkt.src);
    }
    else if (strcmp(type, "ota_ack") == 0) {
        if (doc.containsKey("VId")) handleMeshAck(doc, pkt.src);
        else                        handleLoRaOtaAck(doc);
    }
    else if (strcmp(type, "power") == 0) {
        handleLoRaPower(doc);
    }
    else {
        Serial.printf("⏭ Unhandled LoRa type: '%s'\n", type);
    }
}

// =============================================================================
//  SECTION 25 — APP TASK  (Core 1, pri 1)
// =============================================================================

// NEW — valve mesh retry ticker (15 slots), reuses the SAME msgId on every
// retry so a node that already executed the command answers a re-delivery
// with an ack-only duplicate response instead of re-actuating.
void valveRetryTick() {
    uint32_t now = millis();
    for (int i = 0; i < MAX_VALVES; i++) {
        PendingValve& p = pendingValve[i];
        if (!p.active)           continue;
        if (now < p.nextRetryMs) continue;

        if (p.retryCount < CMD_MAX_RETRIES) {
            p.retryCount++;
            diag.cmdRetries++;
            Serial.printf("🔁 [Attempt %d/3] Retry → VId:%d msgId:%lu\n",
                          p.retryCount + 1, p.vId, (unsigned long)p.msgId);
            enqueueTxLo(BROADCAST_ID, NODE3_ID, (const uint8_t*)p.payload, p.payloadLen);
            p.nextRetryMs = now + CMD_RETRY_MS;
        } else {
            Serial.printf("❌ All 3 attempts exhausted — VId:%d\n", p.vId);
            publishValveNoAck(i);
        }
    }
}

void appTask(void* param) {
    static uint32_t lastDiag = 0;

    for (;;) {
        // 1. Drain RX — ACKs clear pending slots
        LoRaRxPacket pkt;
        while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) {
            processRxPacket(pkt);
        }

        uint32_t now = millis();

        // 2. Motor retry ticker (unchanged, single slot)
        {
            PendingCmd& p = pendingCmd[PENDING_MOTOR_IDX];
            if (p.active && now >= p.nextRetryMs) {
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
                    publishNoAck(PENDING_MOTOR_IDX);
                }
            }
        }

        // 2b. Valve mesh retry ticker (15 slots)
        valveRetryTick();

        // 2c. OTA retry ticker (single in-flight job; mesh or fixed-address)
        if (otaPending.active && now >= otaPending.nextRetryMs) {
            if (otaPending.retryCount < OTA_MAX_RETRIES) {
                otaPending.retryCount++;
                Serial.printf("\n🔁 [OTA attempt %d/3] Retry → %s dev:%s\n",
                              otaPending.retryCount + 1,
                              otaPending.motorMesh ? "MOTOR" : ("VId:" + String(otaPending.vId)).c_str(),
                              otaPending.deviceId);
                enqueueTxLo(BROADCAST_ID, NODE3_ID,
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

int rssiToLoRaBars(int rssi) {
    if (rssi >= -60)  return 4;
    if (rssi >= -75)  return 3;
    if (rssi >= -90)  return 2;
    if (rssi >= -105) return 1;
    return 0;
}

int csmToGsmBars(int csq) {
    if (csq >= 20) return 4;
    if (csq >= 15) return 3;
    if (csq >= 10) return 2;
    if (csq >=  5) return 1;
    return 0;
}

void oledDrawSignalBars(int x, int y, int barsFilled, int maxBars = 4) {
    const int barW = 4;
    const int gap  = 2;
    const int maxH = 14;

    for (int i = 0; i < maxBars; i++) {
        int h  = 3 + (i * (maxH - 3)) / (maxBars - 1);
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

    //display.setCursor(64, 16);
    //display.print("MQTT:");
    //display.print(dState.mqttOnline ? "OK" : "FAIL");

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
    Serial.println("\n══════════ GATEWAY v5.1 DIAGNOSTICS ══════════");
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

    if (pendingCmd[PENDING_MOTOR_IDX].active)
        Serial.printf("  ⏳ Pending[Motor(0xD2)]: dev=%s  attempt=%d/3  timeout_in=%lu s\n",
                      pendingCmd[PENDING_MOTOR_IDX].deviceId,
                      pendingCmd[PENDING_MOTOR_IDX].retryCount + 1,
                      (pendingCmd[PENDING_MOTOR_IDX].nextRetryMs - millis()) / 1000);

    int activeValves = 0;
    for (int i = 0; i < MAX_VALVES; i++) {
        if (!pendingValve[i].active) continue;
        activeValves++;
        Serial.printf("  ⏳ Pending[VId:%d]: dev=%s  msgId=%lu  attempt=%d/3  timeout_in=%lu s\n",
                      pendingValve[i].vId, pendingValve[i].deviceId,
                      (unsigned long)pendingValve[i].msgId,
                      pendingValve[i].retryCount + 1,
                      (pendingValve[i].nextRetryMs - millis()) / 1000);
    }
    if (activeValves == 0) Serial.println("  ⏳ Pending valves: none");

    if (otaPending.active) {
        if (otaPending.motorMesh)
            Serial.printf("  ⏳ Pending OTA: MOTOR msgId:%lu attempt=%d/3\n",
                          (unsigned long)otaPending.msgId, otaPending.retryCount + 1);
        else
            Serial.printf("  ⏳ Pending OTA: VId:%d msgId:%lu attempt=%d/3\n",
                          otaPending.vId, (unsigned long)otaPending.msgId, otaPending.retryCount + 1);
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
        display.println("v5.1 starting");
        display.display();
        Serial.println("✅ OLED ready");
    } else {
        Serial.println("⚠️ OLED not found — continuing without display");
    }

    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    txHiQueue    = xQueueCreate(TX_HI_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    txLoQueue    = xQueueCreate(TX_LO_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));
    otaJobQueue  = xQueueCreate(1, sizeof(SelfOtaJob));   // one in-flight self-OTA at a time

    if (!rxQueue || !txHiQueue || !txLoQueue || !mqttPubQueue || !otaJobQueue) {
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
    xTaskCreatePinnedToCore(otaTask,     "OTA",     8192,  NULL, 1, &otaTaskHandle,     1);

    Serial.println("✅ AgriInverse gateway v5.1 started");
    vTaskSuspend(NULL);
}

void loop() {
    vTaskDelay(portMAX_DELAY);
}
