/* ================================================================
   Agri Inverse — Fertigation Gateway  v1.4  (OTA added)
   Node: Fert1 (0x0A)  ←→  Gateway (0xBB)

   Hardware : ESP32 DevKit (38-pin)
              SX1278 Ra-02  433 MHz
              SIM A7670 GSM modem

   LoRa RF  : SF9  BW125  CR5  SyncWord=0xF3  Preamble=8
              *** MUST match Fert1 node v2.2/v2.3 exactly ***

   FreeRTOS split:
     loraTask  (Core 0, pri 3) — sole radio owner; RX + TX via txQueue
     mqttTask  (Core 1, pri 3) — modem pump; drains mqttPubQueue
     appTask   (Core 1, pri 2) — parse RX, route packets, handle cloud cmds
     otaTask   (Core 1, pri 1) — runs self-OTA download, kept OFF mqttTask
                                  so it never stalls modem.mqtt_handle()

   v1.4 changes vs v1.3:
     - NEW: OTA support via OTA/mode MQTT topic.
       device == "Fertigation gateway" -> self-OTA over WiFi (this board).
       device == "Fertigation"         -> relayed to Fert1 node over LoRa,
                                           in the {"type":"ota",...} format
                                           the node's v2.3 firmware expects.
     - GW_DEVICE_ID now checked against incoming "UId" for OTA targeting
       (previously unused for cloud-side identity — commands were only
       ever targeted at the Fert1 node's own identity fields).
   v1.3 changes vs v1.2:
     - LoraTxPacket buffer increased 512 → 768 bytes
     - enqueueLoRaTx now checks payload size before copy
     - forwardCmdToFert1 doc increased to 1024, buf to 768
     - Identity defines corrected: FERT_NODE_ID=Fert1, FERT_UNIT_ID=32
   ================================================================ */

// ================================================================
//  0. MODEM DEFINE — MUST be first
// ================================================================
#define TINY_GSM_MODEM_A7670
#define SerialMon  Serial
#define SerialAT   Serial1
#define TINY_GSM_DEBUG SerialMon
#define GSM_PIN    ""

// ================================================================
//  1. INCLUDES
// ================================================================
#include <TinyGsmClient.h>
#include <ArduinoJson.h>
#include <SPI.h>
#include <LoRa.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

// AWS IoT TLS certificates
#include "certificate.h"
#include "privatekey.h"
#include "root_ca.h"

// ================================================================
//  2. IDENTITY & CREDENTIALS
// ================================================================
#define GW_DEVICE_ID    "FertGW1"                  // this gateway's UId, for OTA self-targeting
#define FERT_NODE_ID    "Fert1"                     // Did field
#define FERT_UNIT_ID    "32"                        // uid field
#define FERT_FARM_ID    "cveghvjhvejhvjehvjehv"
#define FERT_BOARD_ID   "dufggcgirfighvcehvceh"

// LoRa addressing — must match Fert1 node exactly
#define ADDR_GW        0xBB    // this gateway
#define ADDR_FERT1     0x0A    // Fert1 node

// ⚠️ EDIT THESE for your actual SIM/APN and AWS IoT endpoint before flashing.
const char apn[]       = "iot.com";
const char gprsUser[]  = "";
const char gprsPass[]  = "";
const char broker[]    = "a2nvakoqnh614u-ats.iot.us-east-1.amazonaws.com";
const int  broker_port = 8883;
const char client_id[] = GW_DEVICE_ID;

// MQTT topics
#define TOPIC_TELE    "fert/data"
#define TOPIC_ACK     "fert/ack"
#define TOPIC_STATUS  "fert/status"
#define TOPIC_ALERT   "fert/error"
#define TOPIC_CMD     "fert/cmd"    // GW subscribes
#define TOPIC_OTA     "OTA/mode"    // GW subscribes — OTA commands from cloud

// ================================================================
//  3. PIN DEFINITIONS
// ================================================================
#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY   4

#define LORA_SS       5
#define LORA_RST     14
#define LORA_DIO0    26
#define LORA_FREQ    433E6

// ================================================================
//  4. LoRa RF PARAMETERS — must match Fert1 v2.2/v2.3 exactly
// ================================================================
#define LORA_SF        9
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xF3
#define LORA_TXPOWER   17
#define LORA_PREAMBLE  8

// ================================================================
//  5. TIMING
// ================================================================
#define MQTT_RETRY_MIN_MS      10000UL
#define MQTT_RETRY_MAX_MS      60000UL
#define LORA_WATCHDOG_MS     (5UL  * 60 * 1000)
#define LTE_WATCHDOG_MS      (10UL * 60 * 1000)
#define MAX_MQTT_FAILURES      5
#define MQTT_NOTIFY_DEBOUNCE   5000UL
#define MQTT_KEEPALIVE_SEC     60
#define HEARTBEAT_INTERVAL_MS  50000UL

// ================================================================
//  6. QUEUE DEPTHS
// ================================================================
#define RX_QUEUE_DEPTH        8
#define MQTT_PUB_QUEUE_DEPTH  8
#define TX_QUEUE_DEPTH        4

// ================================================================
//  7. STRUCTS
// ================================================================
struct LoRaRxPacket {
    uint8_t  src;
    uint8_t  dest;
    uint8_t  buf[512];
    uint16_t len;
    int      rssi;
    float    snr;
};

struct MqttPubReq {
    char topic[64];
    char payload[640];
};

struct LoraTxPacket {
    char payload[1024];  // FIX v1.3: increased from 512 for large cmds like set_pi
};

// Job handed from mqttTask -> otaTask so a slow/blocking self-OTA download
// never stalls modem.mqtt_handle() (must be pumped every 10ms or MQTT drops).
struct SelfOtaJob {
    char ssid[64] = {};
    char pass[64] = {};
    char url[220] = {};
};

// ================================================================
//  8. RTOS OBJECTS & GLOBAL STATE
// ================================================================
static QueueHandle_t     rxQueue;
static QueueHandle_t     mqttPubQueue;
static QueueHandle_t     txQueue;
static QueueHandle_t     otaJobQueue;
static SemaphoreHandle_t dataMutex;

static TaskHandle_t loraTaskHandle = nullptr;
static TaskHandle_t mqttTaskHandle = nullptr;
static TaskHandle_t appTaskHandle  = nullptr;
static TaskHandle_t otaTaskHandle  = nullptr;

TinyGsm modem(SerialAT);

// MQTT / node state
static volatile bool     mqttOnline     = false;
static volatile bool     fert1Received  = false;
static volatile uint32_t lastFert1Ms    = 0;

// Pending publish buffer (when MQTT offline)
static char  pendingBuf[640]  = {};
static char  pendingTopic[64] = {};
static bool  pendingData      = false;

// Last known MQTT online state sent to Fert1
static bool     lastNotifiedOnline = false;
static uint32_t lastMqttNotifyMs   = 0;

// Diagnostics
static uint32_t diagRxTotal=0,  diagRxBad=0;
static uint32_t diagPubOk=0,    diagPubFail=0;
static uint32_t diagReconnects=0, diagRadioReinits=0;
static uint32_t diagCmdFwd=0,   diagHeartbeats=0;
static uint32_t diagOtaFwd=0,   diagOtaSelf=0;

// ================================================================
//  10. FLUSH +CMQTTPUB URC
// ================================================================
static void flushCmqttPub() {
    uint32_t deadline = millis() + 150;
    String   buf = "";
    while (millis() < deadline) {
        while (SerialAT.available()) buf += (char)SerialAT.read();
        if (buf.indexOf("+CMQTTPUB") >= 0) break;
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    if (buf.length())
        SerialMon.printf("🧹 URC: %s\n", buf.c_str());
}

// ================================================================
//  11. INTERCEPT +CMQTTCONNLOST URC
// ================================================================
static bool checkCmqttConnLost() {
    if (!SerialAT.available()) return false;

    String line = "";
    uint32_t deadline = millis() + 20;
    while (millis() < deadline) {
        while (SerialAT.available()) line += (char)SerialAT.read();
        if (line.indexOf('\n') >= 0) break;
        vTaskDelay(pdMS_TO_TICKS(2));
    }
    line.trim();
    if (line.length() == 0) return false;

    if (line.indexOf("+CMQTTCONNLOST") >= 0) {
        SerialMon.printf("⚠️  URC intercepted: %s\n", line.c_str());
        return true;
    }
    if (line.length() > 1)
        SerialMon.printf("📟 URC: %s\n", line.c_str());
    return false;
}

// ================================================================
//  12. ENQUEUE HELPERS
// ================================================================
static void enqueueMqttPublish(const char* topic, const char* payload) {
    MqttPubReq req;
    strlcpy(req.topic,   topic,   sizeof(req.topic));
    strlcpy(req.payload, payload, sizeof(req.payload));
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diagPubFail++;
        SerialMon.println("⚠️  mqttPubQueue full — dropped");
    }
}

static void enqueueLoRaTx(const char* json) {
    LoraTxPacket pkt;
    // FIX v1.3: check size before copy to prevent silent truncation
    size_t jsonLen = strlen(json);
    if (jsonLen >= sizeof(pkt.payload)) {
        SerialMon.printf("⚠️  TX payload too large (%u bytes) — dropped\n", jsonLen);
        return;
    }
    strlcpy(pkt.payload, json, sizeof(pkt.payload));
    if (xQueueSend(txQueue, &pkt, pdMS_TO_TICKS(50)) != pdTRUE) {
        SerialMon.println("⚠️  txQueue full — cmd dropped");
    }
}

// ================================================================
//  13. MQTT CONNECTIVITY NOTIFICATION → Fert1
// ================================================================
static void notifyFert1MqttState(bool online) {
    uint32_t now = millis();
    if (online == lastNotifiedOnline &&
        (now - lastMqttNotifyMs) < MQTT_NOTIFY_DEBOUNCE) return;

    lastNotifiedOnline = online;
    lastMqttNotifyMs   = now;

    StaticJsonDocument<128> doc;
    doc["type"]  = "mqtt";
    doc["Did"]   = FERT_NODE_ID;
    doc["state"] = online ? "connected" : "failed";
    String out;
    serializeJson(doc, out);
    enqueueLoRaTx(out.c_str());
    SerialMon.printf("[GW→Fert1] mqtt state: %s\n", online ? "connected" : "failed");
}

// ================================================================
//  13b. OTA — publish ack to cloud
// ================================================================
static void publishOtaAck(bool success, const char* target, const char* detail) {
    StaticJsonDocument<220> doc;
    doc["type"]   = success ? "ota_ack" : "ota_no_ack";
    doc["UId"]    = GW_DEVICE_ID;
    doc["target"] = target;
    if (detail && strlen(detail)) doc["detail"] = detail;
    char buf[220];
    serializeJson(doc, buf, sizeof(buf));
    enqueueMqttPublish(TOPIC_ALERT, buf);
    SerialMon.printf("📤 %s -> AWS: %s\n", success ? "ota_ack" : "ota_no_ack", buf);
}

// ================================================================
//  13c. Self-OTA over WiFi — runs on otaTask, NOT mqttTask
// ================================================================
static void selfOtaUpdate(const char* ssid, const char* pass, const char* url) {
    if (!ssid || !strlen(ssid) || !pass || !strlen(pass)) {
        SerialMon.println("❌ Gateway OTA: missing wifi_ssid/wifi_pass");
        publishOtaAck(false, "Fertigation gateway", "missing wifi credentials");
        return;
    }

    SerialMon.printf("📶 Gateway OTA: joining WiFi '%s'...\n", ssid);
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, pass);

    uint32_t deadline = millis() + 20000UL;
    while (WiFi.status() != WL_CONNECTED && millis() < deadline) vTaskDelay(pdMS_TO_TICKS(500));

    if (WiFi.status() != WL_CONNECTED) {
        SerialMon.println("❌ Gateway OTA: WiFi join failed/timed out");
        publishOtaAck(false, "Fertigation gateway", "wifi join failed");
        WiFi.mode(WIFI_OFF);
        return;
    }
    SerialMon.printf("✅ WiFi joined, IP: %s — downloading %s\n",
                      WiFi.localIP().toString().c_str(), url);

    const int MAX_ATTEMPTS = 3;
    t_httpUpdate_return result = HTTP_UPDATE_FAILED;

    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        SerialMon.printf("Gateway OTA attempt %d/%d...\n", attempt, MAX_ATTEMPTS);
        WiFiClientSecure client;
        client.setInsecure();
        client.setTimeout(15000);
        httpUpdate.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
        httpUpdate.rebootOnUpdate(true);
        result = httpUpdate.update(client, url);
        if (result == HTTP_UPDATE_OK) break;
        SerialMon.printf("  attempt %d failed: %d %s\n", attempt,
                          httpUpdate.getLastError(), httpUpdate.getLastErrorString().c_str());
        if (attempt < MAX_ATTEMPTS) vTaskDelay(pdMS_TO_TICKS(3000));
    }

    switch (result) {
        case HTTP_UPDATE_FAILED:
            publishOtaAck(false, "Fertigation gateway", httpUpdate.getLastErrorString().c_str());
            break;
        case HTTP_UPDATE_NO_UPDATES:
            publishOtaAck(false, "Fertigation gateway", "no update available at url");
            break;
        case HTTP_UPDATE_OK:
            // rebootOnUpdate(true) means we never reach here on success
            break;
    }
    WiFi.mode(WIFI_OFF);
}

// ================================================================
//  13d. otaTask (Core 1, pri 1 — lowest, never blocks LoRa/MQTT)
// ================================================================
void otaTask(void* param) {
    for (;;) {
        SelfOtaJob job;
        if (xQueueReceive(otaJobQueue, &job, portMAX_DELAY) == pdTRUE) {
            SerialMon.println("🚀 otaTask: starting self-OTA job");
            diagOtaSelf++;
            selfOtaUpdate(job.ssid, job.pass, job.url);
        }
    }
}

// ================================================================
//  13e. OTA — handle incoming OTA/mode message (called from mqttCallback)
//
//  Device-type routing (matches the OTA config page's dropdown labels):
//    "Fertigation gateway" -> self-OTA, THIS board, over WiFi
//    "Fertigation"         -> relayed over LoRa to the Fert1 node,
//                              in the exact format its v2.3 firmware expects
// ================================================================
void handleOtaCommand(const char* payload, uint32_t len) {
    StaticJsonDocument<512> doc;
    char buf[512];
    uint32_t n = (len < sizeof(buf) - 1) ? len : sizeof(buf) - 1;
    memcpy(buf, payload, n); buf[n] = '\0';

    if (deserializeJson(doc, buf) != DeserializationError::Ok) {
        SerialMon.println("❌ OTA JSON parse error");
        return;
    }

    const char* uid = doc["UId"] | "";
    if (strcmp(uid, GW_DEVICE_ID) != 0) {
        SerialMon.println("⏭  OTA UId mismatch — ignoring");
        return;
    }

    const char* deviceType = doc["device"] | "";
    const char* url        = doc["url"] | "";
    const char* ssid       = doc["wifi_ssid"] | "";
    const char* pass       = doc["wifi_pass"] | "";

    if (!strlen(url)) {
        SerialMon.println("⚠️  OTA: missing url");
        return;
    }

    if (strcasecmp(deviceType, "fertigation gateway") == 0) {
        SelfOtaJob job;
        strlcpy(job.ssid, ssid, sizeof(job.ssid));
        strlcpy(job.pass, pass, sizeof(job.pass));
        strlcpy(job.url,  url,  sizeof(job.url));
        if (xQueueSend(otaJobQueue, &job, 0) != pdTRUE) {
            SerialMon.println("⚠️  otaJobQueue full — OTA already in progress");
            publishOtaAck(false, "Fertigation gateway", "OTA already in progress");
        } else {
            SerialMon.println("📨 Self-OTA job queued -> otaTask");
        }
        return;
    }

    if (strcasecmp(deviceType, "fertigation") == 0 ||
        strcasecmp(deviceType, "fertigation node") == 0) {
        // Build the exact packet format Fert1's OTA handler expects
        StaticJsonDocument<300> pkt;
        pkt["type"] = "ota";
        pkt["Did"]  = FERT_NODE_ID;
        pkt["uid"]  = FERT_UNIT_ID;
        pkt["url"]  = url;
        if (strlen(ssid)) pkt["wifi"] = ssid;
        if (strlen(pass)) pkt["pass"] = pass;

        String out;
        serializeJson(pkt, out);
        enqueueLoRaTx(out.c_str());
        diagOtaFwd++;
        SerialMon.printf("[OTA->Fert1] %s\n", out.c_str());
        return;
    }

    SerialMon.printf("⏭  OTA: unrecognised device type '%s'\n", deviceType);
}

// ================================================================
//  14. PACKET HANDLERS
// ================================================================
void handleNodeTelemetry(const char* body, int rssi, float snr) {
    SerialMon.printf("[TELE] RSSI=%d SNR=%.1f  %s\n", rssi, snr, body);
    StaticJsonDocument<640> doc;
    if (deserializeJson(doc, body) == DeserializationError::Ok) {
        doc["gw_rssi"] = rssi;
        doc["gw_snr"]  = serialized(String(snr, 1));
        String enriched;
        serializeJson(doc, enriched);
        enqueueMqttPublish(TOPIC_TELE, enriched.c_str());
    } else {
        enqueueMqttPublish(TOPIC_TELE, body);
    }
}

void handleNodeAck(const char* body, int rssi, float snr) {
    SerialMon.printf("[ACK ] RSSI=%d SNR=%.1f  %s\n", rssi, snr, body);
    enqueueMqttPublish(TOPIC_ACK, body);
}

void handleNodeStatus(const char* body, int rssi, float snr) {
    SerialMon.printf("[STAT] RSSI=%d SNR=%.1f  %s\n", rssi, snr, body);
    enqueueMqttPublish(TOPIC_STATUS, body);
}

void handleNodeAlert(const char* body, int rssi, float snr) {
    SerialMon.printf("[ALRT] RSSI=%d SNR=%.1f  %s\n", rssi, snr, body);
    enqueueMqttPublish(TOPIC_ALERT, body);
}

// ================================================================
//  15. processRxPacket
// ================================================================
void processRxPacket(const LoRaRxPacket& pkt) {
    String body((char*)pkt.buf, pkt.len);
    body.trim();

    int js = body.indexOf('{');
    if (js < 0 || !body.endsWith("}")) {
        diagRxBad++;
        SerialMon.printf("❌ No JSON in packet from 0x%02X\n", pkt.src);
        return;
    }
    if (js > 0) body = body.substring(js);

    StaticJsonDocument<128> hdr;
    if (deserializeJson(hdr, body) != DeserializationError::Ok) {
        diagRxBad++;
        SerialMon.printf("❌ JSON parse fail from 0x%02X\n", pkt.src);
        return;
    }

    fert1Received = true;
    lastFert1Ms   = millis();

    const char* type = hdr["type"] | "";

    if      (strcmp(type, "ack")    == 0) handleNodeAck   (body.c_str(), pkt.rssi, pkt.snr);
    else if (strcmp(type, "status") == 0) handleNodeStatus(body.c_str(), pkt.rssi, pkt.snr);
    else if (strcmp(type, "alert")  == 0) handleNodeAlert (body.c_str(), pkt.rssi, pkt.snr);
    else                                   handleNodeTelemetry(body.c_str(), pkt.rssi, pkt.snr);
}

// ================================================================
//  16. DOWNLINK — cloud cmd → Fert1
// ================================================================
void forwardCmdToFert1(const char* payload, uint32_t len) {
    // FIX v1.3: increased doc and buf sizes for large cmds like set_pi
    StaticJsonDocument<1024> doc;
    char buf[1024];
    uint32_t n = (len < sizeof(buf)-1) ? len : sizeof(buf)-1;
    memcpy(buf, payload, n); buf[n] = '\0';

    if (deserializeJson(doc, buf) != DeserializationError::Ok) {
        SerialMon.printf("❌ CMD parse fail: %s\n", buf);
        return;
    }
    if (!doc.containsKey("cmd")) {
        SerialMon.println("❌ CMD missing 'cmd' key — ignored");
        return;
    }

    // ── IDENTITY FILTER — all 4 must match Fert1 exactly ────────
    const char* did = doc["Did"] | "";
    const char* uid = doc["uid"] | "";
    const char* fid = doc["fid"] | "";
    const char* bid = doc["bid"] | "";

    if (strcmp(did, FERT_NODE_ID)  != 0 ||
        strcmp(uid, FERT_UNIT_ID)  != 0 ||
        strcmp(fid, FERT_FARM_ID)  != 0 ||
        strcmp(bid, FERT_BOARD_ID) != 0) {
        SerialMon.printf("❌ CMD identity mismatch — ignored "
                         "Did=%s uid=%s fid=%s bid=%s\n",
                         did, uid, fid, bid);
        return;
    }

    // ── Passed — forward as-is over LoRa ────────────────────────
    if (!doc.containsKey("type")) doc["type"] = "cmd";
    String out;
    serializeJson(doc, out);
    enqueueLoRaTx(out.c_str());
    diagCmdFwd++;
    SerialMon.printf("[CMD→Fert1] %s\n", out.c_str());
}

// ================================================================
//  17. LORA TASK  (Core 0, pri 3)
// ================================================================
void reinitLoRaRadio() {
    LoRa.end();
    pinMode(LORA_RST, OUTPUT);
    digitalWrite(LORA_RST, LOW);  delay(10);
    digitalWrite(LORA_RST, HIGH); delay(10);
    if (!LoRa.begin(LORA_FREQ)) {
        SerialMon.println("❌ LoRa reinit failed");
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
    diagRadioReinits++;
    SerialMon.println("✅ LoRa reinitialised");
}

void loraTask(void* param) {
    LoRa.receive();
    SerialMon.println("📻 loraTask — RX mode");
    uint32_t lastWatchdog = millis();

    for (;;) {
        // ── TX drain (priority over RX) ───────────────────────
        LoraTxPacket txPkt;
        while (xQueueReceive(txQueue, &txPkt, 0) == pdTRUE) {
            SerialMon.printf("[LORA TX→0x%02X] %s\n", ADDR_FERT1, txPkt.payload);
            LoRa.beginPacket();
            LoRa.write(ADDR_FERT1);
            LoRa.write(ADDR_GW);
            LoRa.print(txPkt.payload);
            LoRa.endPacket();
            LoRa.receive();
            lastWatchdog = millis();
            vTaskDelay(pdMS_TO_TICKS(50));
        }

        // ── RX poll ───────────────────────────────────────────
        int packetSize = LoRa.parsePacket();
        if (packetSize >= 2) {
            LoRaRxPacket rx;
            rx.dest = LoRa.read();
            rx.src  = LoRa.read();
            int bodyLen = packetSize - 2;
            if (bodyLen > (int)(sizeof(rx.buf) - 1)) bodyLen = sizeof(rx.buf) - 1;
            rx.len  = (uint16_t)LoRa.readBytes(rx.buf, bodyLen);
            rx.buf[rx.len] = '\0';
            rx.rssi = LoRa.packetRssi();
            rx.snr  = LoRa.packetSnr();
            diagRxTotal++;
            lastWatchdog = millis();

            SerialMon.printf("[LORA RX dest=0x%02X src=0x%02X RSSI=%d] %s\n",
                             rx.dest, rx.src, rx.rssi, (char*)rx.buf);

            if (rx.dest == ADDR_GW && rx.src == ADDR_FERT1) {
                if (xQueueSend(rxQueue, &rx, 0) != pdTRUE)
                    SerialMon.println("⚠️  rxQueue full — dropped");
            } else {
                SerialMon.printf("⏭  Not for us (dest=0x%02X src=0x%02X) — ignored\n",
                                 rx.dest, rx.src);
            }
            LoRa.receive();
        }

        // ── Watchdog ──────────────────────────────────────────
        if ((millis() - lastWatchdog) >= LORA_WATCHDOG_MS) {
            SerialMon.println("⚠️  LoRa watchdog — reinitialising");
            reinitLoRaRadio();
            lastWatchdog = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

// ================================================================
//  18. MQTT CALLBACK — receives commands from cloud
// ================================================================
void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len) {
    char preview[64];
    uint32_t plen = (len < sizeof(preview)-1) ? len : sizeof(preview)-1;
    memcpy(preview, payload, plen); preview[plen] = '\0';
    SerialMon.printf("📥 MQTT IN [%s]: %s\n", topic, preview);

    if (strcmp(topic, TOPIC_CMD) == 0) {
        forwardCmdToFert1((const char*)payload, len);
        return;
    }
    if (strcmp(topic, TOPIC_OTA) == 0) {
        handleOtaCommand((const char*)payload, len);
        return;
    }
}

// ================================================================
//  19. MQTT CONNECT
// ================================================================
bool mqttConnectOnce() {
    modem.mqtt_set_certificate(root_ca, certificate, privateKey);
    if (!modem.mqtt_connect(0, broker, broker_port, client_id, NULL, NULL, MQTT_KEEPALIVE_SEC))
        return false;
    modem.mqtt_subscribe(0, TOPIC_CMD, 0);
    modem.mqtt_subscribe(0, TOPIC_OTA, 1);
    SerialMon.printf("📡 Subscribed to %s and %s\n", TOPIC_CMD, TOPIC_OTA);
    return true;
}

// ================================================================
//  20. MQTT TASK  (Core 1, pri 3)
// ================================================================
void mqttTask(void* param) {
    pinMode(MODEM_PWKEY, OUTPUT);
    digitalWrite(MODEM_PWKEY, LOW);
    vTaskDelay(pdMS_TO_TICKS(1500));
    digitalWrite(MODEM_PWKEY, HIGH);
    vTaskDelay(pdMS_TO_TICKS(5000));

    SerialMon.println("📶 Modem booting...");
    modem.init();
    vTaskDelay(pdMS_TO_TICKS(500));
    SerialMon.println("📶 " + modem.getModemInfo());

    modem.sendAT("+CNMP=38");
    modem.waitResponse();

    uint32_t netDeadline = millis() + 90000UL;
    while (!modem.waitForNetwork(5000)) {
        SerialMon.println("⏳ Registering...");
        if (millis() > netDeadline) {
            modem.restart();
            vTaskDelay(pdMS_TO_TICKS(5000));
            netDeadline = millis() + 90000UL;
        }
    }
    SerialMon.println("✅ Network registered");

    while (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
        SerialMon.println("⏳ GPRS retry...");
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
    SerialMon.println("✅ GPRS | IP: " + modem.localIP().toString());

    modem.mqtt_begin(false);
    modem.mqtt_set_callback(mqttCallback);
    SerialMon.println("✅ MQTT client initialised");

    enum State { CONNECTING, CONNECTED } state = CONNECTING;
    uint32_t retryDelay    = MQTT_RETRY_MIN_MS;
    uint32_t nextRetry     = 0;
    uint32_t lastHandleMs  = 0;
    uint32_t lastSuccessMs = millis();
    bool     notified      = false;
    uint8_t  failCount     = 0;

    for (;;) {
        uint32_t now = millis();

        if (now - lastHandleMs >= 10) {
            lastHandleMs = now;
            modem.mqtt_handle();
        }

        if (state == CONNECTED && checkCmqttConnLost()) {
            SerialMon.println("⚠️  +CMQTTCONNLOST intercepted — reconnecting immediately");
            mqttOnline = false;
            notified   = false;
            state      = CONNECTING;
            notifyFert1MqttState(false);
            nextRetry  = now;
        }

        if (state == CONNECTED) {
            MqttPubReq req;
            if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                if (modem.mqtt_publish(0, req.topic, req.payload, 0)) {
                    diagPubOk++;
                    failCount = 0;
                    SerialMon.printf("✅ Published [%s]: %s\n", req.topic, req.payload);
                    flushCmqttPub();
                } else {
                    diagPubFail++;
                    SerialMon.printf("❌ Publish FAILED [%s] — re-queuing\n", req.topic);
                    xQueueSend(mqttPubQueue, &req, 0);
                    if (++failCount >= MAX_MQTT_FAILURES) {
                        failCount  = 0;
                        notified   = false;
                        state      = CONNECTING;
                        mqttOnline = false;
                        notifyFert1MqttState(false);
                        nextRetry  = now + retryDelay;
                    }
                }
            }
        }

        switch (state) {
        case CONNECTING:
            if (now < nextRetry) break;

            if (lastSuccessMs > 0 && (now - lastSuccessMs) > LTE_WATCHDOG_MS) {
                SerialMon.println("🔄 LTE watchdog — restarting modem");
                modem.restart();
                vTaskDelay(pdMS_TO_TICKS(5000));
                modem.waitForNetwork(15000);
                modem.gprsConnect(apn, gprsUser, gprsPass);
                lastSuccessMs = millis();
                retryDelay    = MQTT_RETRY_MIN_MS;
                nextRetry     = 0;
                break;
            }

            SerialMon.println("🔌 MQTT connecting...");
            if (mqttConnectOnce()) {
                state         = CONNECTED;
                retryDelay    = MQTT_RETRY_MIN_MS;
                lastSuccessMs = millis();
                notified      = false;
                failCount     = 0;
                mqttOnline    = true;
                diagReconnects++;
                SerialMon.println("✅ MQTT connected");
            } else {
                mqttOnline = false;
                SerialMon.printf("❌ MQTT failed — retry in %lu s\n", retryDelay/1000);
                nextRetry  = now + retryDelay;
                retryDelay = min(retryDelay * 2, (uint32_t)MQTT_RETRY_MAX_MS);
            }
            break;

        case CONNECTED:
            if (!modem.mqtt_connected()) {
                SerialMon.println("⚠️  MQTT dropped (poll) — reconnecting");
                mqttOnline = false;
                notified   = false;
                state      = CONNECTING;
                notifyFert1MqttState(false);
                nextRetry  = now + retryDelay;
                break;
            }

            if (!notified) {
                notified = true;
                SerialMon.println("📡 MQTT online");
                notifyFert1MqttState(true);
                if (pendingData) {
                    pendingData = false;
                    enqueueMqttPublish(pendingTopic, pendingBuf);
                    SerialMon.println("📤 Buffered data published after reconnect");
                }
            }
            lastSuccessMs = now;
            break;
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ================================================================
//  21. APP TASK  (Core 1, pri 2)
// ================================================================
void appTask(void* param) {
    static uint32_t lastDiag      = 0;
    static uint32_t lastHeartbeat = 0;

    for (;;) {
        uint32_t now = millis();

        LoRaRxPacket pkt;
        while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) {
            processRxPacket(pkt);

            if (!mqttOnline) {
                String body((char*)pkt.buf, pkt.len);
                body.trim();
                int js = body.indexOf('{');
                if (js >= 0 && body.endsWith("}")) {
                    if (js > 0) body = body.substring(js);
                    strlcpy(pendingBuf,   body.c_str(), sizeof(pendingBuf));
                    strlcpy(pendingTopic, TOPIC_TELE,   sizeof(pendingTopic));
                    pendingData = true;
                    SerialMon.println("⚠️  MQTT offline — telemetry buffered");
                }
            }
        }

        if (mqttOnline && (now - lastHeartbeat) >= HEARTBEAT_INTERVAL_MS) {
            lastHeartbeat = now;
            StaticJsonDocument<128> hb;
            hb["gw"]     = GW_DEVICE_ID;
            hb["heap"]   = (uint32_t)esp_get_free_heap_size();
            hb["uptime"] = now / 1000;
            hb["type"]   = "heartbeat";
            String hbStr;
            serializeJson(hb, hbStr);
            enqueueMqttPublish(TOPIC_STATUS, hbStr.c_str());
            diagHeartbeats++;
            SerialMon.printf("[HB] %s\n", hbStr.c_str());
        }

        if (now - lastDiag >= 5UL * 60 * 1000) {
            lastDiag = now;
            SerialMon.println("\n══════ FERT-GW DIAGNOSTICS ══════");
            SerialMon.printf("  LoRa RX total:  %lu\n", diagRxTotal);
            SerialMon.printf("  RX bad:         %lu\n", diagRxBad);
            SerialMon.printf("  MQTT pub OK:    %lu\n", diagPubOk);
            SerialMon.printf("  MQTT pub fail:  %lu\n", diagPubFail);
            SerialMon.printf("  Reconnects:     %lu\n", diagReconnects);
            SerialMon.printf("  Radio reinits:  %lu\n", diagRadioReinits);
            SerialMon.printf("  Cmds fwd→Fert1: %lu\n", diagCmdFwd);
            SerialMon.printf("  OTA fwd→Fert1:  %lu\n", diagOtaFwd);
            SerialMon.printf("  OTA self:       %lu\n", diagOtaSelf);
            SerialMon.printf("  Heartbeats:     %lu\n", diagHeartbeats);
            SerialMon.printf("  Fert1 active:   %s\n",
                             (fert1Received && (millis()-lastFert1Ms) < 120000UL) ? "YES" : "NO");
            SerialMon.printf("  MQTT online:    %s\n",  mqttOnline ? "YES" : "NO");
            SerialMon.printf("  Free heap:      %u B\n", esp_get_free_heap_size());
            SerialMon.printf("  loraTask HWM:   %u B\n", uxTaskGetStackHighWaterMark(loraTaskHandle));
            SerialMon.printf("  mqttTask HWM:   %u B\n", uxTaskGetStackHighWaterMark(mqttTaskHandle));
            SerialMon.printf("  appTask  HWM:   %u B\n", uxTaskGetStackHighWaterMark(appTaskHandle));
            SerialMon.println("══════════════════════════════════\n");
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ================================================================
//  22. SETUP
// ================================================================
void setup() {
    SerialMon.begin(115200);
    SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
    delay(2000);

    SerialMon.println("\n[BOOT] Fert Gateway v1.4 (OTA)");

    dataMutex    = xSemaphoreCreateMutex();
    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));
    txQueue      = xQueueCreate(TX_QUEUE_DEPTH,        sizeof(LoraTxPacket));
    otaJobQueue  = xQueueCreate(1, sizeof(SelfOtaJob));

    if (!dataMutex || !rxQueue || !mqttPubQueue || !txQueue || !otaJobQueue) {
        SerialMon.println("❌ RTOS resource init failed — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
    if (!LoRa.begin(LORA_FREQ)) {
        SerialMon.println("❌ LoRa init FAILED — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    SerialMon.printf("✅ LoRa ready — SF%d BW125 CR%d SyncWord=0x%02X\n",
                     LORA_SF, LORA_CR, LORA_SYNCWORD);
    SerialMon.printf("   GW=0x%02X  Fert1=0x%02X\n", ADDR_GW, ADDR_FERT1);

    xTaskCreatePinnedToCore(loraTask, "LoRa", 4096,  NULL, 3, &loraTaskHandle, 0);
    xTaskCreatePinnedToCore(mqttTask, "MQTT", 10240, NULL, 3, &mqttTaskHandle, 1);
    xTaskCreatePinnedToCore(appTask,  "App",  6144,  NULL, 2, &appTaskHandle,  1);
    xTaskCreatePinnedToCore(otaTask,  "OTA",  8192,  NULL, 1, &otaTaskHandle,  1);

    SerialMon.println("✅ Fertigation gateway v1.4 started — tasks running");
    vTaskSuspend(NULL);
}

void loop() { vTaskDelay(portMAX_DELAY); }