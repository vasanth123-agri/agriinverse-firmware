// =============================================================================
//  AgriInverse — ESP32 LoRa/LTE Gateway  v6.2  (GSM, smart mesh, same formats)
//
//  Same as gateway v6.1 (WiFi), but AWS IoT is reached through the A7670
//  LTE modem, as in gateway v5.3. Works with valve node v4.6 (also talks to
//  v4.5 nodes — packets unchanged).
//
//  NO FORMAT CHANGES. LoRa command / OTA packets and the relay_ack, no_ack,
//  ota_ack, ota_no_ack and power messages to AWS have the same fields as
//  v5.3 / v6.0 / v6.1.
//
//  WHAT CAME FROM WHERE
//  --------------------
//  From v5.3 (LTE): modem power-up on PWKEY, network + GPRS + NTP, MQTT over
//    the modem with the AWS certificates, 10-min LTE watchdog (modem restart),
//    GSM signal bars on the OLED, gateway self-OTA by joining wifi_ssid /
//    wifi_pass from the OTA payload.
//  From v6.1 (smart mesh): one valve-mesh job at a time, fixed-address
//    traffic held while a mesh job is in flight, ≈22 s retry timeout,
//    app-resend filter, valve OTA "accepted" handling, late OTA-failure
//    reports, 384 B MQTT payloads, OLED retry line "V9 TRY 2/3 14s +3".
//
//  Required library: TinyGSM fork with A7670 MQTT support (the one v5.3
//  used — modem.mqtt_connect / mqtt_publish / mqtt_handle).
//  Set the APN in SECTION 3.
//
//  OLED LAYOUT (128×64, SSD1306):
//  ┌──────────────────────────────┐
//  │ LoRa [▂▄▆█]    GSM [▂▄▆█]    │  row 0  — signal bars
//  │ LORA:OK  MQTT:OK             │  row 16 — radio / cloud status
//  │ CMD:V7 A open                │  row 28 — last command
//  │ V7 TRY 2/3 14s +2            │  row 40 — retransmission state
//  │ ERR:--           -82dBm      │  row 52 — error code + LoRa RSSI
//  └──────────────────────────────┘
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
#include <WiFi.h>               // gateway self-OTA only
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

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

#define NODE3_ID     0xBB   // this gateway
#define NODE6_ID     0xD2   // Motor relay node

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
//  SECTION 3 — LTE NETWORK & BROKER
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

#define LTE_WATCHDOG_MS     (10UL * 60 * 1000)   // no MQTT this long → restart modem

// =============================================================================
//  SECTION 4 — LoRa RF + MESH PARAMETERS  (timing must match valve node v4.6)
// =============================================================================

#define LORA_SF        7
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xAB
#define LORA_TXPOWER   20
#define LORA_PREAMBLE  8
#define LORA_MAX_BODY  253
#define RELAY_HEADROOM 16

#define BROADCAST_ID    0xD1     // all valve-mesh traffic
#define MAX_VALVES      15
#define MOTOR_MESH_VID  0

// Hop budget put in every command (same field as before). Lower it to
// (relays your farthest node needs) + 1 for less traffic and faster retries.
#define MESH_HOP_COUNT  6

#define MESH_ATTEMPTS        3
#define FLOOD_LEVEL_MS       1550   // node: FLOOD_SLOTS × FLOOD_SLOT_MS + jitter
#define ACK_PRE_DELAY_MS     1500   // node: ACK_PRE_DELAY_MS
#define NODE_WORK_MS         900    // safe delay + pulse + tail + battery read
#define MESH_MARGIN_MS       1500
#define MESH_MIN_TIMEOUT_MS  12000  // > node duplicate window (10 s)
#define MESH_GUARD_MS        1500   // quiet time between jobs

// =============================================================================
//  SECTION 5 — QUEUE DEPTHS & TIMEOUTS
// =============================================================================

#define RX_QUEUE_DEPTH       16
#define TX_HI_QUEUE_DEPTH    8
#define TX_LO_QUEUE_DEPTH    16
#define MESH_TX_QUEUE_DEPTH  4
#define MESH_JOB_QUEUE_DEPTH 24
#define MQTT_PUB_QUEUE_DEPTH 8

#define MQTT_RETRY_MIN_MS   10000UL
#define MQTT_RETRY_MAX_MS   60000UL
#define LORA_WATCHDOG_MS    (5UL * 60 * 1000)

#define CMD_MAX_RETRIES  2          // motor (fixed address)
#define CMD_RETRY_MS     90000UL
#define MAX_MQTT_FAILURES  5

// =============================================================================
//  SECTION 6 — ERROR CODES
// =============================================================================

#define ERR_NONE          0
#define ERR_NO_ACK      100
#define ERR_LORA_REINIT 101
#define ERR_INVALID_PKT 102
#define ERR_JSON_PARSE  103

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
    char topic[64];
    char payload[384];
};

// Motor commands keep the single-slot model.
struct PendingCmd {
    bool     active      = false;
    uint8_t  destNode    = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[256]= {};
    uint16_t payloadLen  = 0;
    char     deviceId[32]= {};
    char     actuator[16]= {};
    char     ackJson[384]= {};
};

// One job on the 0xD1 mesh. The packet is built once (same JSON as v6.0)
// and the same bytes are sent on every attempt.
enum MeshKind : uint8_t { MK_CMD = 0, MK_OTA = 1 };
struct MeshJob {
    uint8_t  kind;
    uint8_t  vId;            // 1..15, or 0 with motorMesh for motor OTA
    bool     motorMesh;
    uint32_t msgId;
    char     deviceId[32];   // CMD: "device" from AWS; OTA: UId
    char     valve[4];       // normalised, for relay_ack
    char     cmd[8];
    char     nodeType[24];   // OTA: "valve node" / "motor node"
    uint16_t payloadLen;
    char     payload[256];
};

// Identity of a queued / in-flight job, used to ignore the app's resends.
// Defined here (not next to its functions) so Arduino's auto-generated
// function prototypes can see it.
struct JobKey {
    bool    used;
    uint8_t kind;
    uint8_t vId;
    bool    motorMesh;
    char    valve[4];
    char    cmd[8];
};

struct MeshTxn {
    bool     active   = false;
    MeshJob  job;
    uint8_t  attempt  = 0;
    uint32_t deadline = 0;
};

struct SelfOtaJob {
    char deviceType[24] = {};
    char uid[32]        = {};
    char ssid[64]       = {};
    char pass[64]       = {};
    char url[220]       = {};
};

// =============================================================================
//  SECTION 8 — DISPLAY STATE
// =============================================================================

struct DisplayState {
    bool     loraActive   = false;
    bool     mqttOnline   = false;
    int      loraRSSI     = -120;
    int      gsmRSSI      = 0;       // CSQ 0–31 from the modem
    uint16_t errorCode    = ERR_NONE;
    char     lastCmd[32]  = "--";
};
static volatile DisplayState dState;

// =============================================================================
//  SECTION 9 — RTOS OBJECTS
// =============================================================================

static QueueHandle_t  rxQueue;
static QueueHandle_t  txHiQueue;
static QueueHandle_t  txLoQueue;
static QueueHandle_t  meshTxQueue;
static QueueHandle_t  meshJobQueue;
static QueueHandle_t  mqttPubQueue;
static QueueHandle_t  otaJobQueue;

static TaskHandle_t   loraTaskHandle    = nullptr;
static TaskHandle_t   mqttTaskHandle    = nullptr;
static TaskHandle_t   appTaskHandle     = nullptr;
static TaskHandle_t   displayTaskHandle = nullptr;
static TaskHandle_t   otaTaskHandle     = nullptr;

#define PENDING_MOTOR_IDX  1
static PendingCmd pendingCmd[2];

static MeshTxn  meshTxn;
static volatile bool meshBusy = false;   // gates fixed-address LoRa TX
static uint32_t meshNextFree  = 0;

// =============================================================================
//  SECTION 10 — SHARED STATE
// =============================================================================

static volatile bool     mqttOnline   = false;
static volatile uint32_t lastLoRaRxMs = 0;

#define RETRY_RESULT_SHOW_MS 10000UL
static char     retryResult[22]  = "";
static uint32_t retryResultUntil = 0;

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
    uint32_t meshJobsDropped;
    uint32_t meshRepeatsIgnored;
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
void  otaTask(void*);
void  reinitLoRaRadio();
bool  enqueueTxHi(uint8_t dest, uint8_t src, const uint8_t* data, uint16_t len);
bool  enqueueTxLo(uint8_t dest, uint8_t src, const uint8_t* data, uint16_t len);
bool  enqueueTxHiJson(uint8_t dest, uint8_t src, JsonDocument& doc);
void  processRxPacket(LoRaRxPacket& pkt);
void  handleLoRaAck(DynamicJsonDocument& loraDoc, uint8_t srcNode);
void  handleMeshAck(DynamicJsonDocument& loraDoc, uint8_t srcNode);
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
void  meshTick();
void  setError(uint16_t code);
void  clearError();
void  setRetryResult(const char* text);
void  printDiagnostics();
int   parseVId(DynamicJsonDocument& doc);
void  handleOtaCommand(DynamicJsonDocument& doc);
void  publishOtaAck(bool success, const char* deviceId, const char* nodeType, const char* detail);
void  oledRender();

// =============================================================================
//  SECTION 14 — HELPERS
// =============================================================================

void setError(uint16_t code) {
    dState.errorCode = code;
    Serial.printf("⚠️ Error set: %d\n", code);
}

void clearError() {
    dState.errorCode = ERR_NONE;
}

void setRetryResult(const char* text) {
    strlcpy(retryResult, text, sizeof(retryResult));
    retryResultUntil = millis() + RETRY_RESULT_SHOW_MS;
}

uint32_t newMsgId() {
    uint32_t v;
    do { v = esp_random() & 0x7FFFFFFFUL; } while (v == 0);
    return v;
}

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

bool queueMqtt(const char* topic, const char* payload, const char* what) {
    MqttPubReq req;
    strlcpy(req.topic, topic, sizeof(req.topic));
    if (strlen(payload) >= sizeof(req.payload)) {
        Serial.printf("⚠️ %s too long for MQTT buffer — dropped\n", what);
        return false;
    }
    strlcpy(req.payload, payload, sizeof(req.payload));
    Serial.printf("📤 %s → AWS: %s\n", what, req.payload);
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diag.mqttPubFail++;
        Serial.printf("⚠️ mqttPubQueue full — %s dropped\n", what);
        return false;
    }
    return true;
}

bool queueMqttDoc(const char* topic, JsonDocument& doc, const char* what) {
    char buf[sizeof(MqttPubReq::payload)];
    if (measureJson(doc) >= sizeof(buf)) {
        Serial.printf("⚠️ %s too long for MQTT buffer — dropped\n", what);
        return false;
    }
    serializeJson(doc, buf, sizeof(buf));
    return queueMqtt(topic, buf, what);
}

// =============================================================================
//  SECTION 15 — SINGLE LoRa TASK  (Core 0, pri 3)
//  Mesh packets go out at once; fixed-address packets wait for an idle mesh.
// =============================================================================

static void loraDoTx(LoRaTxPacket& pkt) {
    LoRa.beginPacket();
    LoRa.write(pkt.dest);
    LoRa.write(pkt.src);
    LoRa.write(pkt.buf, pkt.len);
    LoRa.endPacket();
    diag.txSent++;
    Serial.printf("📡 LoRa TX → dest:0x%02X  src:0x%02X  len:%u\n",
                  pkt.dest, pkt.src, pkt.len);
    LoRa.receive();
}

void loraTask(void* param) {
    LoRa.receive();
    dState.loraActive = true;
    Serial.println("📻 LoRa task started — radio in RX mode");

    uint32_t lastWatchdog = millis();

    for (;;) {
        LoRaTxPacket pkt;
        if (xQueueReceive(meshTxQueue, &pkt, 0) == pdTRUE) { loraDoTx(pkt); continue; }
        if (!meshBusy) {
            if (xQueueReceive(txHiQueue, &pkt, 0) == pdTRUE) { loraDoTx(pkt); continue; }
            if (xQueueReceive(txLoQueue, &pkt, 0) == pdTRUE) { loraDoTx(pkt); continue; }
        }

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

            if (rx.dest != NODE3_ID && rx.dest != BROADCAST_ID) {
                diag.rxDroppedDest++;
            } else if (xQueueSend(rxQueue, &rx, 0) != pdTRUE) {
                Serial.printf("⚠️ rxQueue full — dropped src:0x%02X\n", rx.src);
            }
            LoRa.receive();
        }

        if ((millis() - lastWatchdog) >= LORA_WATCHDOG_MS) {
            Serial.println("⚠️ LoRa watchdog — reinitialising radio");
            dState.loraActive = false;
            reinitLoRaRadio();
            diag.radioReinits++;
            lastWatchdog = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(2));
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
//  SECTION 17 — MESH JOB ENGINE  (runs on appTask, one job at a time)
// =============================================================================

uint32_t meshTimeoutMs() {
    uint32_t t = 2UL * MESH_HOP_COUNT * FLOOD_LEVEL_MS + NODE_WORK_MS
               + ACK_PRE_DELAY_MS + MESH_MARGIN_MS;
    return t < MESH_MIN_TIMEOUT_MS ? MESH_MIN_TIMEOUT_MS : t;
}

// ---- Repeat filter -----------------------------------------------------------
// The app resends a command every 2 min until it gets an answer. A repeat of a
// job that is still queued or in flight is ignored; the original job sends the
// one relay_ack / no_ack (or ota_ack / ota_no_ack) the app is waiting for.
// A different command for the same valve (open after close) is still queued.

static JobKey            jobKeys[MESH_JOB_QUEUE_DEPTH + 1];   // queued + in flight
static SemaphoreHandle_t jobKeyMutex = nullptr;

static bool jobKeyMatches(const JobKey& k, const MeshJob& j) {
    if (!k.used || k.kind != j.kind || k.vId != j.vId || k.motorMesh != j.motorMesh) return false;
    if (j.kind == MK_OTA) return true;                       // one OTA per target at a time
    return strcmp(k.valve, j.valve) == 0 && strcmp(k.cmd, j.cmd) == 0;
}

// 1 = new job registered, 0 = repeat of a pending job, -1 = no room.
static int claimJobKey(const MeshJob& j) {
    int result = -1;
    xSemaphoreTake(jobKeyMutex, portMAX_DELAY);
    int freeIdx = -1;
    for (int i = 0; i <= MESH_JOB_QUEUE_DEPTH; i++) {
        if (jobKeyMatches(jobKeys[i], j)) { result = 0; break; }
        if (!jobKeys[i].used && freeIdx < 0) freeIdx = i;
    }
    if (result != 0 && freeIdx >= 0) {
        JobKey& k = jobKeys[freeIdx];
        k.used      = true;
        k.kind      = j.kind;
        k.vId       = j.vId;
        k.motorMesh = j.motorMesh;
        strlcpy(k.valve, j.valve, sizeof(k.valve));
        strlcpy(k.cmd,   j.cmd,   sizeof(k.cmd));
        result = 1;
    }
    xSemaphoreGive(jobKeyMutex);
    return result;
}

static void releaseJobKey(const MeshJob& j) {
    xSemaphoreTake(jobKeyMutex, portMAX_DELAY);
    for (int i = 0; i <= MESH_JOB_QUEUE_DEPTH; i++) {
        if (jobKeyMatches(jobKeys[i], j)) { jobKeys[i].used = false; break; }
    }
    xSemaphoreGive(jobKeyMutex);
}

// Returns false only if the job could not be queued (queue full).
bool queueMeshJob(const MeshJob& j) {
    int claim = claimJobKey(j);
    if (claim == 0) {
        diag.meshRepeatsIgnored++;
        Serial.printf("⏭ Repeat of a pending %s for %s%u %s %s — ignored, the original will answer\n",
                      j.kind == MK_OTA ? "OTA" : "command", j.motorMesh ? "MOTOR " : "VId:",
                      j.vId, j.valve, j.cmd);
        return true;
    }
    if (claim < 0 || xQueueSend(meshJobQueue, &j, 0) != pdTRUE) {
        if (claim > 0) releaseJobKey(j);
        diag.meshJobsDropped++;
        Serial.println("❌ Mesh job queue full — dropped");
        return false;
    }
    Serial.printf("📥 Queued %s for %s%u msgId:%lu (%u waiting)\n",
                  j.kind == MK_OTA ? "OTA" : "command", j.motorMesh ? "MOTOR " : "VId:",
                  j.vId, (unsigned long)j.msgId,
                  (unsigned)uxQueueMessagesWaiting(meshJobQueue));
    return true;
}

void meshFinish() {
    releaseJobKey(meshTxn.job);       // a later resend is a new command again
    meshTxn.active = false;
    meshBusy       = false;
    meshNextFree   = millis() + MESH_GUARD_MS;
}

void meshTransmit() {
    MeshJob& j = meshTxn.job;
    LoRaTxPacket pkt;
    pkt.dest = BROADCAST_ID;
    pkt.src  = NODE3_ID;
    pkt.len  = j.payloadLen;
    memcpy(pkt.buf, j.payload, j.payloadLen);
    if (xQueueSend(meshTxQueue, &pkt, pdMS_TO_TICKS(50)) != pdTRUE)
        Serial.println("⚠️ meshTxQueue full — attempt not sent");

    uint32_t to = meshTimeoutMs();
    meshTxn.deadline = millis() + to;
    Serial.printf("📡 [Attempt %u/%d] %s → %s%u msgId:%lu — timeout %lu s\n",
                  meshTxn.attempt, MESH_ATTEMPTS, j.kind == MK_OTA ? "OTA" : "VALVE CMD",
                  j.motorMesh ? "MOTOR " : "VId:", j.vId, (unsigned long)j.msgId,
                  (unsigned long)(to / 1000));
}

// Same no_ack / ota_no_ack messages as v6.0.
void meshFail() {
    MeshJob& j = meshTxn.job;
    char res[22];
    if (j.kind == MK_CMD) {
        StaticJsonDocument<256> doc;
        doc["type"]     = "no_ack";
        doc["UId"]      = DEVICE_ID;
        doc["VId"]      = j.vId;
        doc["device"]   = j.deviceId;
        doc["actuator"] = "valve";
        doc["attempts"] = CMD_MAX_RETRIES + 1;
        Serial.printf("❌ NO ACK — VId:%u (%s) after %d attempts\n", j.vId, j.deviceId, MESH_ATTEMPTS);
        queueMqttDoc(publish_topic, doc, "no_ack");
        diag.cmdNoAck++;
        setError(ERR_NO_ACK);
        snprintf(res, sizeof(res), "V%u NO ACK", j.vId);
    } else {
        Serial.printf("❌ OTA — all 3 attempts exhausted for %s\n", j.deviceId);
        publishOtaAck(false, j.deviceId, j.nodeType, "no ack after 3 attempts");
        if (j.motorMesh) snprintf(res, sizeof(res), "MOTOR OTA NO ACK");
        else             snprintf(res, sizeof(res), "OTA%u NO ACK", j.vId);
    }
    setRetryResult(res);
    meshFinish();
}

void meshTick() {
    uint32_t now = millis();

    if (!meshTxn.active) {
        if ((int32_t)(now - meshNextFree) < 0) return;
        MeshJob job;
        if (xQueueReceive(meshJobQueue, &job, 0) != pdTRUE) return;
        meshTxn.active  = true;
        meshTxn.job     = job;
        meshTxn.attempt = 1;
        meshBusy        = true;

        char cmdStr[32];
        if (job.kind == MK_CMD)  snprintf(cmdStr, sizeof(cmdStr), "V%u %s %s", job.vId, job.valve, job.cmd);
        else if (job.motorMesh)  snprintf(cmdStr, sizeof(cmdStr), "Motor OTA");
        else                     snprintf(cmdStr, sizeof(cmdStr), "V%u OTA", job.vId);
        strlcpy((char*)dState.lastCmd, cmdStr, sizeof(dState.lastCmd));
        clearError();

        meshTransmit();
        return;
    }

    if ((int32_t)(now - meshTxn.deadline) < 0) return;

    if (meshTxn.attempt >= MESH_ATTEMPTS) {
        meshFail();
        return;
    }
    meshTxn.attempt++;
    diag.cmdRetries++;
    Serial.printf("🔁 Retry with the same msgId\n");
    meshTransmit();
}

// =============================================================================
//  SECTION 18 — COMMAND HELPERS  (packets identical to v6.0)
// =============================================================================

void enqueueValveCommand(uint8_t vId, const String& device, const String& valve,
                         const String& cmd, const String& fid, const String& bid) {
    if (vId < 1 || vId > MAX_VALVES) {
        Serial.printf("⚠️ enqueueValveCommand: bad VId %d\n", vId);
        return;
    }

    MeshJob j = {};
    j.kind  = MK_CMD;
    j.vId   = vId;
    j.msgId = newMsgId();

    StaticJsonDocument<256> doc;
    doc["type"]    = "command_broadcast";
    doc["UId"]     = DEVICE_ID;
    doc["VId"]     = vId;
    doc["msgId"]   = j.msgId;
    doc["hop"]     = MESH_HOP_COUNT;
    doc["device"]  = device;
    doc["valve"]   = valve;
    doc["command"] = cmd;
    if (fid.length()) doc["FId"] = fid;
    if (bid.length()) doc["BId"] = bid;

    if (measureJson(doc) > LORA_MAX_BODY - RELAY_HEADROOM) {
        Serial.println("❌ Valve CMD too long for LoRa — dropped");
        return;
    }
    j.payloadLen = (uint16_t)serializeJson(doc, j.payload, sizeof(j.payload));

    strlcpy(j.deviceId, device.c_str(), sizeof(j.deviceId));
    String valveNorm = valve; valveNorm.toUpperCase();
    String cmdNorm   = cmd;   cmdNorm.toLowerCase();
    strlcpy(j.valve, valveNorm.c_str(), sizeof(j.valve));
    strlcpy(j.cmd,   cmdNorm.c_str(),   sizeof(j.cmd));

    queueMeshJob(j);
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
    enqueueTxHiJson(NODE6_ID, NODE3_ID, doc);
}

// =============================================================================
//  SECTION 19 — MQTT CALLBACK  (runs inside modem.mqtt_handle() on mqttTask)
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
//  SECTION 19b — OTA HANDLING  (same packets and messages as v6.0)
//
//  "OTA/mode" payload from AWS:
//  {
//    "UId": "66", "FId": "...", "BId": "...",
//    "device": "valve node" | "motor node" | "VMC gateway",
//    "VId": 7,                       // valve node only (1..15)
//    "wifi_ssid": "farm-ap",         // forwarded to the node
//    "wifi_pass": "secret",
//    "url": "https://.../node.bin"
//  }
// =============================================================================

void publishOtaAck(bool success, const char* deviceId, const char* nodeType, const char* detail) {
    StaticJsonDocument<256> doc;
    doc["type"]   = success ? "ota_ack" : "ota_no_ack";
    doc["UId"]    = DEVICE_ID;
    doc["device"] = deviceId;
    doc["node"]   = nodeType;
    if (detail && strlen(detail)) doc["detail"] = detail;
    queueMqttDoc(publish_ota_topic, doc, success ? "ota_ack" : "ota_no_ack");
}

// Builds the v6.0 "ota" packet (VId 1..15 for a valve, 0 for the motor).
void enqueueMeshOta(DynamicJsonDocument& doc, int vId, bool motorMesh) {
    const char* deviceType = doc["device"] | "";
    const char* url = doc["url"] | "";
    if (!strlen(url)) {
        Serial.println("⚠️ OTA: missing 'url' field");
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing url");
        return;
    }

    MeshJob j = {};
    j.kind      = MK_OTA;
    j.vId       = (uint8_t)vId;
    j.motorMesh = motorMesh;
    j.msgId     = newMsgId();
    strlcpy(j.deviceId, doc["UId"] | DEVICE_ID, sizeof(j.deviceId));
    strlcpy(j.nodeType, deviceType, sizeof(j.nodeType));

    const char* fid = doc["FId"] | "";
    const char* bid = doc["BId"] | "";

    StaticJsonDocument<384> pkt;
    pkt["type"]  = "ota";
    pkt["UId"]   = DEVICE_ID;
    pkt["VId"]   = vId;
    pkt["msgId"] = j.msgId;
    pkt["hop"]   = MESH_HOP_COUNT;
    pkt["url"]   = url;
    if (doc.containsKey("wifi_ssid")) pkt["wifi"] = doc["wifi_ssid"];
    if (doc.containsKey("wifi_pass")) pkt["pass"] = doc["wifi_pass"];
    if (strlen(fid)) pkt["FId"] = fid;
    if (strlen(bid)) pkt["BId"] = bid;

    if (measureJson(pkt) > LORA_MAX_BODY - RELAY_HEADROOM) {
        Serial.printf("❌ OTA packet %u B — too large to relay, shorten the URL\n",
                      (unsigned)measureJson(pkt));
        publishOtaAck(false, j.deviceId, deviceType, "payload too large");
        return;
    }
    j.payloadLen = (uint16_t)serializeJson(pkt, j.payload, sizeof(j.payload));

    if (!queueMeshJob(j))
        publishOtaAck(false, j.deviceId, deviceType, "tx queue full");
}

// Gateway self-OTA: joins wifi_ssid / wifi_pass from the OTA payload (the LTE
// link stays up for MQTT), downloads url and reboots. Runs on otaTask so the
// download never stalls modem.mqtt_handle().
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

    uint32_t deadline = millis() + 20000UL;
    while (WiFi.status() != WL_CONNECTED && millis() < deadline) vTaskDelay(pdMS_TO_TICKS(500));

    if (WiFi.status() != WL_CONNECTED) {
        Serial.println("❌ Gateway OTA: WiFi join failed/timed out");
        publishOtaAck(false, uid, deviceType, "wifi join failed");
        WiFi.mode(WIFI_OFF);
        return;
    }

    Serial.printf("✅ WiFi joined, IP: %s — downloading %s\n",
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
        if (attempt < MAX_ATTEMPTS) vTaskDelay(pdMS_TO_TICKS(3000));
    }

    if (result == HTTP_UPDATE_FAILED)
        publishOtaAck(false, uid, deviceType, httpUpdate.getLastErrorString().c_str());
    else if (result == HTTP_UPDATE_NO_UPDATES)
        publishOtaAck(false, uid, deviceType, "no update available at url");

    WiFi.mode(WIFI_OFF);
}

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

    if (strcasecmp(deviceType, "valve node") == 0) {
        int vId = parseVId(doc);
        if (vId < 1 || vId > MAX_VALVES) {
            Serial.println("⚠️ OTA: missing/invalid VId for valve node target");
            publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing VId");
            return;
        }
        enqueueMeshOta(doc, vId, false);
        return;
    }

    if (strcasecmp(deviceType, "motor node") == 0) {
        enqueueMeshOta(doc, MOTOR_MESH_VID, true);
        return;
    }

    if (strcasecmp(deviceType, "VMC gateway") != 0 &&
        strcasecmp(deviceType, "VMCgateway")  != 0) {
        Serial.printf("⚠️ OTA: unrecognized device type '%s' — rejecting\n", deviceType);
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "unrecognized device type");
        return;
    }

    const char* url = doc["url"] | "";
    if (!strlen(url)) {
        publishOtaAck(false, doc["UId"] | DEVICE_ID, deviceType, "missing url");
        return;
    }

    SelfOtaJob job;
    strlcpy(job.deviceType, deviceType, sizeof(job.deviceType));
    strlcpy(job.uid,  doc["UId"] | DEVICE_ID,  sizeof(job.uid));
    strlcpy(job.ssid, doc["wifi_ssid"] | "",   sizeof(job.ssid));
    strlcpy(job.pass, doc["wifi_pass"] | "",   sizeof(job.pass));
    strlcpy(job.url,  url,                     sizeof(job.url));

    if (xQueueSend(otaJobQueue, &job, 0) != pdTRUE)
        publishOtaAck(false, job.uid, deviceType, "OTA already in progress");
    else
        Serial.println("📨 Self-OTA job queued — handing off to otaTask");
}

// =============================================================================
//  SECTION 20 — MQTT CONNECT  (A7670, as v5.3)
//  Certificate set on every attempt, no disconnect-before-connect.
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
//  SECTION 21 — MQTT TASK  (Core 1, pri 3)
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

    modem.sendAT("+CNMP=38");          // LTE only
    modem.waitResponse();

    Serial.println("📶 Waiting for network...");
    uint32_t netDeadline = millis() + 90000UL;
    while (!modem.waitForNetwork(5000)) {
        Serial.println("⏳ Registering...");
        dState.gsmRSSI = modem.getSignalQuality();
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

    modem.NTPServerSync("pool.ntp.org", 20);   // TLS needs a correct clock
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
    uint8_t  failCount     = 0;   // consecutive publish failures

    for (;;) {
        uint32_t now = millis();

        // Pump the modem every 10 ms — incoming commands arrive here.
        if (now - lastHandleMs >= 10) {
            lastHandleMs = now;
            modem.mqtt_handle();
        }

        if (now - lastCsqMs >= 10000UL) {
            lastCsqMs      = now;
            dState.gsmRSSI = modem.getSignalQuality();   // CSQ 0–31
        }

        // Drain one publish per pass (QoS 0, as v5.3).
        if (state == CONNECTED) {
            MqttPubReq req;
            if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                if (modem.mqtt_publish(0, req.topic, req.payload, 0)) {
                    diag.mqttPubOk++;
                    failCount = 0;
                    Serial.printf("✅ Published → [%s]: %s\n", req.topic, req.payload);
                } else {
                    diag.mqttPubFail++;
                    Serial.println("❌ Publish failed — re-queuing");
                    xQueueSendToFront(mqttPubQueue, &req, 0);   // keep the order
                    if (++failCount >= MAX_MQTT_FAILURES) {
                        failCount         = 0;
                        notified          = false;
                        state             = CONNECTING;
                        mqttOnline        = false;
                        dState.mqttOnline = false;
                        nextRetry         = now + retryDelay;
                    }
                }
            }
        }

        switch (state) {
        case CONNECTING:
            if (now < nextRetry) break;

            if ((now - lastSuccessMs) > LTE_WATCHDOG_MS) {
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
                state             = CONNECTED;
                retryDelay        = MQTT_RETRY_MIN_MS;
                lastSuccessMs     = millis();
                notified          = false;
                failCount         = 0;
                mqttOnline        = true;
                dState.mqttOnline = true;
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
//  SECTION 22 — MOTOR RETRY HELPERS  (unchanged)
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
    queueMqtt(publish_topic, pendingCmd[idx].ackJson, "relay_ack");
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

    Serial.printf("❌ NO ACK — %s (%s) after %d attempts\n",
                  p.deviceId, p.actuator, CMD_MAX_RETRIES + 1);
    queueMqttDoc(publish_topic, doc, "no_ack");

    setError(ERR_NO_ACK);
    setRetryResult("MOTOR NO ACK");
    diag.cmdNoAck++;
    clearPending(idx);
}

void clearPending(int idx) {
    pendingCmd[idx].active = false;
}

// =============================================================================
//  SECTION 23 — ACK HANDLERS
// =============================================================================

// Motor (fixed address) — unchanged.
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

    char result[22];
    snprintf(result, sizeof(result), "MOTOR ACK %d/%d", attemptNum, CMD_MAX_RETRIES + 1);
    setRetryResult(result);

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

// Valve-mesh "ack" and "ota_ack" (VId + replyTo), matched to the job in flight.
void handleMeshAck(DynamicJsonDocument& loraDoc, uint8_t srcNode) {
    const char* type = loraDoc["type"] | "";
    int vId          = loraDoc["VId"]  | -1;
    uint32_t replyTo = loraDoc["replyTo"].as<uint32_t>();

    if (vId < 0 || vId > MAX_VALVES) {
        Serial.printf("⏭ %s: bad/missing VId — dropping\n", type);
        return;
    }

    MeshTxn& x = meshTxn;
    MeshJob& j = x.job;

    if (strcmp(type, "ack") == 0) {
        if (vId < 1) {
            Serial.println("⏭ ack: VId:0 is reserved for motor OTA only — dropping");
            return;
        }
        if (!x.active || j.kind != MK_CMD || j.vId != vId || j.msgId != replyTo) {
            Serial.printf("⏭ Stale/duplicate ACK VId:%d (no matching command in flight)\n", vId);
            return;
        }

        if      (x.attempt == 1) diag.cmdAckAttempt1++;
        else if (x.attempt == 2) diag.cmdAckAttempt2++;
        else                     diag.cmdAckAttempt3++;
        clearError();

        char result[22];
        snprintf(result, sizeof(result), "V%d ACK %u/%d", vId, x.attempt, MESH_ATTEMPTS);
        setRetryResult(result);

        // Same relay_ack as v6.0.
        StaticJsonDocument<448> mqttDoc;
        mqttDoc["type"]   = "relay_ack";
        mqttDoc["UId"]    = DEVICE_ID;
        mqttDoc["VId"]    = vId;
        mqttDoc["device"] = loraDoc["from"];

        const char* rawStatus = loraDoc["status"] | "";
        mqttDoc["status"]   = rawStatus;
        char actuator[12];
        snprintf(actuator, sizeof(actuator), "Valve %s", j.valve);
        mqttDoc["actuator"] = (const char*)actuator;
        mqttDoc["state"]    = strcasestr(rawStatus, "rejected") ? "rejected" : j.cmd;
        if (loraDoc.containsKey("FId")) mqttDoc["FId"] = loraDoc["FId"];
        if (loraDoc.containsKey("BId")) mqttDoc["BId"] = loraDoc["BId"];

        if (loraDoc.containsKey("bat")) mqttDoc["bat"] = loraDoc["bat"];
        if (loraDoc.containsKey("bv"))  mqttDoc["bv"]  = loraDoc["bv"];
        if (loraDoc.containsKey("ib"))  mqttDoc["ib"]  = loraDoc["ib"];
        if (loraDoc.containsKey("cs"))  mqttDoc["cs"]  = loraDoc["cs"];

        Serial.printf("✅ ACK matched — VId:%d msgId:%lu attempt:%u/3\n",
                      vId, (unsigned long)replyTo, x.attempt);
        if (!loraDoc.containsKey("bv"))
            Serial.printf("⚠️ VId:%d sent no battery readings — check its INA219\n", vId);
        queueMqttDoc(publish_topic, mqttDoc, "relay_ack");
        meshFinish();
        return;
    }

    if (strcmp(type, "ota_ack") == 0) {
        const char* status = loraDoc["status"] | "ok";
        bool ok = (strcasecmp(status, "fail") != 0);
        const char* detail = loraDoc["detail"] | "";

        bool matches = x.active && j.kind == MK_OTA && j.msgId == replyTo &&
                       ((vId == 0 && j.motorMesh) || (vId >= 1 && !j.motorMesh && j.vId == vId));
        if (!matches) {
            // Node v4.6 reports a failed download after the job has finished.
            if (!ok) {
                Serial.printf("⚠️ Late OTA failure from VId:%d — %s\n", vId, detail);
                publishOtaAck(false, DEVICE_ID, vId == 0 ? "motor node" : "valve node", detail);
                setRetryResult("OTA FAILED");
            } else {
                Serial.printf("⏭ Stale/duplicate OTA-ACK VId:%d\n", vId);
            }
            return;
        }

        Serial.printf("✅ OTA-ACK matched — VId:%d status:%s %s\n", vId, status, detail);
        publishOtaAck(ok, j.deviceId, j.nodeType, detail);
        char result[22];
        if (j.motorMesh) snprintf(result, sizeof(result), ok ? "MOTOR OTA OK" : "MOTOR OTA FAIL");
        else             snprintf(result, sizeof(result), ok ? "OTA%d OK" : "OTA%d FAIL", vId);
        setRetryResult(result);
        meshFinish();
        return;
    }
}

// =============================================================================
//  SECTION 24 — POWER HANDLER  (unchanged)
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

    queueMqttDoc(publish_power_topic, mqttDoc, "power");
}

// =============================================================================
//  SECTION 25 — PACKET PROCESSING
// =============================================================================

void processRxPacket(LoRaRxPacket& pkt) {
    String payload((char*)pkt.buf, pkt.len);
    payload.trim();

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

    const char* type = doc["type"] | "";

    // Our own commands / OTA relayed back by the valve nodes — nothing to do.
    if (strcmp(type, "command_broadcast") == 0 || strcmp(type, "ota") == 0) return;

    Serial.printf("\n📥 LoRa 0x%02X→0x%02X  RSSI:%d  SNR:%.1f\n",
                  pkt.src, pkt.dest, pkt.rssi, pkt.snr);
    Serial.printf("   Payload: %s\n", payload.c_str());

    if (strcmp(type, "ack") == 0) {
        if (doc.containsKey("VId")) handleMeshAck(doc, pkt.src);
        else                        handleLoRaAck(doc, pkt.src);
    }
    else if (strcmp(type, "ota_ack") == 0) {
        if (doc.containsKey("VId")) handleMeshAck(doc, pkt.src);
        else Serial.println("⏭ ota_ack without VId — ignoring");
    }
    else if (strcmp(type, "power") == 0) {
        handleLoRaPower(doc);
    }
    else {
        Serial.printf("⏭ Unhandled LoRa type: '%s'\n", type);
    }
}

// =============================================================================
//  SECTION 26 — APP TASK  (Core 1, pri 2)
// =============================================================================

void appTask(void* param) {
    static uint32_t lastDiag = 0;

    for (;;) {
        LoRaRxPacket pkt;
        while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) processRxPacket(pkt);

        uint32_t now = millis();

        // Valve mesh + motor OTA — one job at a time.
        meshTick();

        // Motor command retries (fixed address).
        {
            PendingCmd& p = pendingCmd[PENDING_MOTOR_IDX];
            if (p.active && now >= p.nextRetryMs) {
                if (p.retryCount < CMD_MAX_RETRIES) {
                    p.retryCount++;
                    diag.cmdRetries++;
                    Serial.printf("\n🔁 [Attempt %d/3] Retry → node:0x%02X  dev:%s  act:%s\n",
                                  p.retryCount + 1, p.destNode, p.deviceId, p.actuator);
                    enqueueTxLo(p.destNode, NODE3_ID, (const uint8_t*)p.payload, p.payloadLen);
                    p.nextRetryMs = now + CMD_RETRY_MS;
                } else {
                    Serial.printf("\n❌ All 3 attempts exhausted for %s (%s)\n",
                                  p.deviceId, p.actuator);
                    publishNoAck(PENDING_MOTOR_IDX);
                }
            }
        }

        if (now - lastDiag >= 5UL * 60 * 1000) {
            lastDiag = now;
            printDiagnostics();
        }

        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

// =============================================================================
//  SECTION 27 — RADIO REINIT
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
//  SECTION 28 — OLED
// =============================================================================

int rssiToLoRaBars(int rssi) {
    if (rssi >= -60)  return 4;
    if (rssi >= -75)  return 3;
    if (rssi >= -90)  return 2;
    if (rssi >= -105) return 1;
    return 0;
}

int csqToGsmBars(int csq) {
    if (csq == 99 || csq <= 0) return 0;   // 99 = not known
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
        if (i < barsFilled) display.fillRect(bx, by, barW, h, SSD1306_WHITE);
        else                display.drawRect(bx, by, barW, h, SSD1306_WHITE);
    }
}

// "V9 TRY 2/3 14s +3" while a mesh job runs; motor countdown; else last result.
void buildRetryLine(char* line, size_t size) {
    uint32_t now = millis();
    unsigned waiting = (unsigned)uxQueueMessagesWaiting(meshJobQueue);

    if (meshTxn.active) {
        const MeshJob& j = meshTxn.job;
        int32_t left = (int32_t)(meshTxn.deadline - now);
        long secs = left <= 0 ? 0 : (left + 999) / 1000;
        char who[8];
        if (j.motorMesh)          snprintf(who, sizeof(who), "MOTA");
        else if (j.kind == MK_OTA) snprintf(who, sizeof(who), "OTA%u", j.vId);
        else                       snprintf(who, sizeof(who), "V%u", j.vId);
        int n = snprintf(line, size, "%s TRY %u/%d %lds", who, meshTxn.attempt, MESH_ATTEMPTS, secs);
        if (waiting && n > 0 && (size_t)n < size) snprintf(line + n, size - n, " +%u", waiting);
        return;
    }
    if (waiting) {
        snprintf(line, size, "MESH WAIT +%u", waiting);
        return;
    }
    const PendingCmd& mp = pendingCmd[PENDING_MOTOR_IDX];
    if (mp.active) {
        int32_t left = (int32_t)(mp.nextRetryMs - now);
        snprintf(line, size, "MOTOR TRY %u/%d %lds", mp.retryCount + 1, CMD_MAX_RETRIES + 1,
                 left <= 0 ? 0L : (long)((left + 999) / 1000));
        return;
    }
    if (retryResult[0] && (int32_t)(retryResultUntil - now) > 0) {
        strlcpy(line, retryResult, size);
        return;
    }
    strlcpy(line, "RETRY:--", size);
}

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
    oledDrawSignalBars(96, 0, csqToGsmBars(dState.gsmRSSI));

    display.setCursor(0, 16);
    display.print("LORA:");
    display.print(dState.loraActive ? "OK" : "FAIL");
    display.setCursor(64, 16);
    display.print("MQTT:");
    display.print(dState.mqttOnline ? "OK" : "--");

    display.setCursor(0, 28);
    display.print("CMD:");
    display.print((const char*)dState.lastCmd);

    char retryLine[22];
    buildRetryLine(retryLine, sizeof(retryLine));
    display.setCursor(0, 40);
    display.print(retryLine);

    display.setCursor(0, 52);
    display.print("ERR:");
    uint16_t err = dState.errorCode;
    if (err == ERR_NONE) display.print("--");
    else                 display.print(err);

    display.setCursor(80, 52);
    char rssiBuf[12];
    snprintf(rssiBuf, sizeof(rssiBuf), "%ddBm", dState.loraRSSI);
    display.print(rssiBuf);

    display.display();
}

void displayTask(void* param) {
    for (;;) {
        oledRender();
        vTaskDelay(pdMS_TO_TICKS(500));
    }
}

// =============================================================================
//  SECTION 29 — DIAGNOSTICS
// =============================================================================

void printDiagnostics() {
    Serial.println("\n══════════ GATEWAY v6.2 (LTE, smart mesh) DIAGNOSTICS ══════════");
    Serial.printf("  LoRa RX total:     %lu\n", diag.rxTotal);
    Serial.printf("  RX dest drops:     %lu\n", diag.rxDroppedDest);
    Serial.printf("  RX JSON drops:     %lu\n", diag.rxDroppedJson);
    Serial.printf("  LoRa TX sent:      %lu\n", diag.txSent);
    Serial.printf("  TX queue full:     %lu\n", diag.txQueueFull);
    Serial.printf("  Retries:           %lu\n", diag.cmdRetries);
    Serial.printf("  ACK attempt 1/2/3: %lu / %lu / %lu\n",
                  diag.cmdAckAttempt1, diag.cmdAckAttempt2, diag.cmdAckAttempt3);
    Serial.printf("  No-ack:            %lu\n", diag.cmdNoAck);
    Serial.printf("  Mesh jobs waiting: %u/%d (dropped %lu)\n",
                  (unsigned)uxQueueMessagesWaiting(meshJobQueue), MESH_JOB_QUEUE_DEPTH,
                  diag.meshJobsDropped);
    Serial.printf("  App repeats ignored: %lu\n", diag.meshRepeatsIgnored);
    Serial.printf("  Mesh timeout:      %lu ms per attempt\n", (unsigned long)meshTimeoutMs());
    Serial.printf("  MQTT pub OK/fail:  %lu / %lu\n", diag.mqttPubOk, diag.mqttPubFail);
    Serial.printf("  MQTT reconnects:   %lu\n", diag.mqttReconnects);
    Serial.printf("  LTE watchdogs:     %lu\n", diag.lteWatchdogFires);
    Serial.printf("  Radio reinits:     %lu\n", diag.radioReinits);
    Serial.printf("  LoRa RSSI:         %d dBm\n", dState.loraRSSI);
    Serial.printf("  GSM CSQ:           %d\n", dState.gsmRSSI);
    Serial.printf("  Free heap:         %u B\n", esp_get_free_heap_size());
    Serial.printf("  Stack HWM lora/mqtt/app/disp: %u / %u / %u / %u\n",
                  uxTaskGetStackHighWaterMark(loraTaskHandle),
                  uxTaskGetStackHighWaterMark(mqttTaskHandle),
                  uxTaskGetStackHighWaterMark(appTaskHandle),
                  uxTaskGetStackHighWaterMark(displayTaskHandle));
    if (meshTxn.active)
        Serial.printf("  ⏳ In flight: %s %s%u msgId:%lu attempt %u/%d\n",
                      meshTxn.job.kind == MK_OTA ? "OTA" : "CMD",
                      meshTxn.job.motorMesh ? "MOTOR " : "VId:", meshTxn.job.vId,
                      (unsigned long)meshTxn.job.msgId, meshTxn.attempt, MESH_ATTEMPTS);
    if (pendingCmd[PENDING_MOTOR_IDX].active)
        Serial.printf("  ⏳ Motor command: attempt %d/3\n",
                      pendingCmd[PENDING_MOTOR_IDX].retryCount + 1);
    Serial.println("═══════════════════════════════════════════════════════════\n");
}

// =============================================================================
//  SECTION 30 — SETUP
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
        display.setCursor(14, 35);
        display.println("v6.2 LTE mesh");
        display.display();
        Serial.println("✅ OLED ready");
    } else {
        Serial.println("⚠️ OLED not found — continuing without display");
    }

    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    txHiQueue    = xQueueCreate(TX_HI_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    txLoQueue    = xQueueCreate(TX_LO_QUEUE_DEPTH,    sizeof(LoRaTxPacket));
    meshTxQueue  = xQueueCreate(MESH_TX_QUEUE_DEPTH,  sizeof(LoRaTxPacket));
    meshJobQueue = xQueueCreate(MESH_JOB_QUEUE_DEPTH, sizeof(MeshJob));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));
    otaJobQueue  = xQueueCreate(1, sizeof(SelfOtaJob));
    jobKeyMutex  = xSemaphoreCreateMutex();
    memset(jobKeys, 0, sizeof(jobKeys));

    if (!rxQueue || !txHiQueue || !txLoQueue || !meshTxQueue || !meshJobQueue ||
        !mqttPubQueue || !otaJobQueue || !jobKeyMutex) {
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
    xTaskCreatePinnedToCore(appTask,     "App",     8192,  NULL, 2, &appTaskHandle,     1);
    xTaskCreatePinnedToCore(displayTask, "Display", 4096,  NULL, 1, &displayTaskHandle, 1);
    xTaskCreatePinnedToCore(otaTask,     "OTA",     8192,  NULL, 1, &otaTaskHandle,     1);

    Serial.println("✅ AgriInverse gateway v6.2 (LTE, smart mesh) started");
    vTaskSuspend(NULL);
}

void loop() {
    vTaskDelay(portMAX_DELAY);
}
