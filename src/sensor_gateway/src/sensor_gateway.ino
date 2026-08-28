/* ================================================================
   Dual-Node Sensor Gateway  (Node1 0xAA + Node2 0xCC)  — with OTA
   GSM (A7670) + AWS IoT MQTT + LoRa 433 MHz SF7 BW125
   0.96" SSD1306 OLED status display (128×64, I2C 0x3C)
   ----------------------------------------------------------------
   FreeRTOS task split:
     loraTask  (Core 0, pri 3) — radio owner, RX/TX only
     mqttTask  (Core 1, pri 3) — modem pump + publish drain
     appTask   (Core 1, pri 2) — parse RX, build publish, idle check,
                                  LoRa OTA retry ticker + OLED refresh
     otaTask   (Core 1, pri 1) — runs the actual self-OTA download,
                                  kept OFF mqttTask so a slow/blocking
                                  OTA can never stall modem.mqtt_handle()
   ----------------------------------------------------------------
   OTA/mode payload (self-OTA — targets this gateway):
     { "UId":"51", "device":"sensor gateway",
       "wifi_ssid":"...", "wifi_pass":"...", "url":"..." }

   OTA/mode payload (relay to a LoRa node — targets Node1 or Node2):
     { "UId":"51", "device":"sensor node", "NodeNum":1,
       "wifi_ssid":"...", "wifi_pass":"...", "url":"..." }
   NodeNum 1 -> NODE1_ID (0xAA), NodeNum 2 -> NODE2_ID (0xCC).
   Node firmware must reply with {"type":"ota_ack","status":"ok"/"fail"}
   addressed back to GATEWAY_ID, matching this gateway's existing
   direct-addressing model (not the mesh/broadcast model used by the
   valve+motor gateway's nodes).
   ----------------------------------------------------------------
   Dependencies (Library Manager / platformio.ini):
     adafruit/Adafruit SSD1306 @ ^2.5.7
     adafruit/Adafruit GFX Library @ ^1.11.9
   ================================================================ */

// ================= MODEM DEFINE - MUST BE FIRST =================
#define TINY_GSM_MODEM_A7670
#define SerialMon  Serial
#define SerialAT   Serial1
#define TINY_GSM_DEBUG SerialMon
#define GSM_PIN    ""

// ======================== INCLUDES ==============================
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
#include <Preferences.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

#include "certificate.h"
#include "privatekey.h"
#include "root_ca.h"

// ======================== IDENTITY & PINS =======================
// ⚠️ EDIT THIS LINE for each customer's board before compiling/flashing.
#define DEVICE_ID    "51"
const char client_id[]         = DEVICE_ID;

#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY  4
#define LORA_SS      5
#define LORA_RST     14
#define LORA_DIO0    26
#define LORA_FREQ    433E6

// ======================== NODE IDs ==============================
#define GATEWAY_ID   0xBB
#define NODE1_ID     0xAA
#define NODE2_ID     0xCC

// ======================== NETWORK & BROKER =======================
// ⚠️ EDIT these for your actual SIM/APN and AWS IoT endpoint before flashing.
const char apn[]      = "iot.com";
const char gprsUser[] = "";
const char gprsPass[] = "";

const char broker[]              = "a2nvakoqnh614u-ats.iot.us-east-1.amazonaws.com";
const int  broker_port           = 8883;

const char subscribe_topic[]     = "device/cmd";
const char publish_topic[]       = "device/data";

// ======================== LoRa RF ===============================
#define LORA_SF        7
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xAB
#define LORA_TXPOWER   20
#define LORA_PREAMBLE  8

// ======================== TIMING ================================
#define IDLE_TIMEOUT         120000UL
#define SEND_TIMEOUT          15000UL
#define MQTT_RETRY_MIN_MS     10000UL
#define MQTT_RETRY_MAX_MS     60000UL
#define LORA_WATCHDOG_MS   (5UL * 60 * 1000)
#define LTE_WATCHDOG_MS   (10UL * 60 * 1000)
#define MAX_MQTT_FAILURES      5

#define OTA_MAX_RETRIES  2
#define OTA_RETRY_MS     90000UL

// ======================== QUEUE DEPTHS ==========================
#define RX_QUEUE_DEPTH        8
#define MQTT_PUB_QUEUE_DEPTH  8

// ======================== OLED CONFIG ===========================
#define OLED_W         128
#define OLED_H         64
#define OLED_RESET     -1
#define OLED_I2C_ADDR  0x3C
#define OLED_ROW0_Y    0
#define OLED_ROW1_Y    18
#define OLED_ROW2_Y    30
#define OLED_ROW3_Y    48

// ======================== STRUCTS ===============================
struct LoRaRxPacket {
    uint8_t  src;
    uint8_t  dest;
    uint8_t  buf[256];
    uint16_t len;
    int      rssi;
    float    snr;
};

struct MqttPubReq {
    char payload[512];
};

// OTA state for whichever LoRa node is currently being updated.
struct OtaPending {
    bool     active      = false;
    uint8_t  destNode    = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[220]= {};
    uint16_t payloadLen  = 0;
    char     nodeLabel[16]= {};   // "Node1" / "Node2"
};

// Job handed from mqttTask -> otaTask so a slow/blocking self-OTA
// download never stalls modem.mqtt_handle() (must be pumped every
// 10ms or MQTT drops).
struct SelfOtaJob {
    char ssid[64] = {};
    char pass[64] = {};
    char url[220] = {};
};

// ======================== RTOS OBJECTS ==========================
static QueueHandle_t     rxQueue;
static QueueHandle_t     mqttPubQueue;
static QueueHandle_t     otaJobQueue;
static SemaphoreHandle_t dataMutex;

static TaskHandle_t loraTaskHandle = nullptr;
static TaskHandle_t mqttTaskHandle = nullptr;
static TaskHandle_t appTaskHandle  = nullptr;
static TaskHandle_t otaTaskHandle  = nullptr;

static OtaPending nodeOta;   // one LoRa-node OTA in flight at a time

// ======================== GSM ===================================
TinyGsm modem(SerialAT);

// ======================== OLED ==================================
static Adafruit_SSD1306 oled(OLED_W, OLED_H, &Wire, OLED_RESET);
static bool oledReady = false;

// ======================== SHARED STATE ==========================
static volatile bool  mqttOnline    = false;
static volatile bool  node1Received = false;
static volatile bool  node2Received = false;
static volatile bool  idleSent      = false;

static char pendingBuf[512] = {};
static bool pendingData     = false;

static volatile unsigned long lastNode1      = 0;
static volatile unsigned long lastNode2      = 0;
static volatile unsigned long lastPublishMs  = 0;
static volatile int           lastLoraRssi   = -120;
static volatile uint8_t       simSignalBars  = 0;

// Node1 (soil)
static float n1_N=0,   n1_P=0,   n1_K=0;
static float n1_ST=0,  n1_SM=0,  n1_EC=0;

// Node2 (env + spectral)
static float n2_CO2=0, n2_WS=0,  n2_par=0;
static float n2_CT=0,  n2_CH=0;
static float n2_ST=0,  n2_SM=0,  n2_EC=0,  n2_Ph=0;
static float n2_F1=0,  n2_F2=0,  n2_F3=0,  n2_F4=0;
static float n2_F5=0,  n2_F6=0,  n2_F7=0,  n2_F8=0;
static float n2_NIR=0, n2_Clear=0;

// ======================== DIAGNOSTICS ===========================
static uint32_t diagRxTotal=0,    diagRxBad=0;
static uint32_t diagPubOk=0,      diagPubFail=0;
static uint32_t diagReconnects=0, diagRadioReinits=0;
static uint32_t diagIdleSent=0;

// ================================================================
//  HELPERS
// ================================================================
bool isNode1Sending() { return node1Received && (millis()-lastNode1 <= SEND_TIMEOUT); }
bool isNode2Sending() { return node2Received && (millis()-lastNode2 <= SEND_TIMEOUT); }
bool isNode1Active()  { return node1Received && (millis()-lastNode1 <= IDLE_TIMEOUT); }
bool isNode2Active()  { return node2Received && (millis()-lastNode2 <= IDLE_TIMEOUT); }
bool eitherActive()   { return isNode1Active() || isNode2Active(); }

// ================================================================
//  OLED — internal helpers
// ================================================================
static void oledDrawBars(int16_t x, int16_t bottomY, uint8_t level) {
    const uint8_t heights[4] = { 3, 5, 7, 9 };
    for (uint8_t i = 0; i < 4; i++) {
        int16_t h  = heights[i];
        int16_t bx = x + i * 4;
        int16_t by = bottomY - h + 1;
        if (i < level) oled.fillRect(bx, by, 3, h, SSD1306_WHITE);
        else            oled.drawRect(bx, by, 3, h, SSD1306_WHITE);
    }
}
static constexpr int16_t BARS_W = 15;

static void oledFmtAgo(char* buf, size_t sz, uint16_t sec) {
    if (sec == 0xFFFF) { snprintf(buf, sz, "--"); return; }
    if (sec < 60)      { snprintf(buf, sz, "%us ago", sec); return; }
    uint16_t m = sec / 60, s = sec % 60;
    if (s > 0) snprintf(buf, sz, "%um %us ago", m, s);
    else       snprintf(buf, sz, "%um ago", m);
}

static void oledRefresh() {
    if (!oledReady) return;
    unsigned long now = millis();

    uint8_t loraBars;
    int rssi = lastLoraRssi;
    if      (rssi >= -70)  loraBars = 4;
    else if (rssi >= -85)  loraBars = 3;
    else if (rssi >= -100) loraBars = 2;
    else if (rssi > -120)  loraBars = 1;
    else                   loraBars = 0;

    uint16_t n2Sec = node2Received
        ? (uint16_t)min((now - lastNode2) / 1000UL, 0xFFFEUL) : 0xFFFF;
    uint16_t n1Sec = node1Received
        ? (uint16_t)min((now - lastNode1) / 1000UL, 0xFFFEUL) : 0xFFFF;
    uint16_t cloudSec = (lastPublishMs > 0)
        ? (uint16_t)min((now - lastPublishMs) / 1000UL, 0xFFFEUL) : 0xFFFF;

    oled.clearDisplay();
    oled.setTextSize(1);
    oled.setTextColor(SSD1306_WHITE);

    char tmp[24];
    int16_t barBottomY = OLED_ROW0_Y + 9;

    oledDrawBars(0, barBottomY, loraBars);
    oled.setCursor(BARS_W + 2, OLED_ROW0_Y + 1);
    oled.print("LoRa");

    int16_t simX = OLED_W - BARS_W;
    oledDrawBars(simX, barBottomY, simSignalBars);
    oled.setCursor(simX - 20, OLED_ROW0_Y + 1);
    oled.print("SIM");

    oled.drawFastHLine(0, OLED_ROW0_Y + 12, OLED_W, SSD1306_WHITE);

    oledFmtAgo(tmp, sizeof(tmp), n2Sec);
    oled.setCursor(0, OLED_ROW1_Y);
    oled.print("N2:"); oled.print(tmp);

    oledFmtAgo(tmp, sizeof(tmp), n1Sec);
    oled.setCursor(0, OLED_ROW2_Y);
    oled.print("N1:"); oled.print(tmp);

    oled.drawFastHLine(0, OLED_ROW3_Y - 4, OLED_W, SSD1306_WHITE);

    oled.setCursor(0, OLED_ROW3_Y);
    oled.print("Cloud:");
    if (!mqttOnline) {
        oled.print("offline");
    } else {
        oledFmtAgo(tmp, sizeof(tmp), cloudSec);
        oled.print(tmp);
    }
    oled.display();
}

// ================================================================
//  FLUSH +CMQTTPUB URC
// ================================================================
static void flushCmqttPub() {
    uint32_t deadline = millis() + 150;
    String   buf = "";
    while (millis() < deadline) {
        while (SerialAT.available()) buf += (char)SerialAT.read();
        if (buf.indexOf("+CMQTTPUB") >= 0) break;
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    if (buf.length()) Serial.printf("URC: %s\n", buf.c_str());
}

// ================================================================
//  BUILD DATA / IDLE JSON
// ================================================================
static uint16_t buildDataJson(char* out, size_t outSize) {
    DynamicJsonDocument doc(1024);
    doc["FId"] = "cmqkj27ni0005xiw2grjdvsd4";
    doc["Id"]  = "M1";
    doc["UId"] = DEVICE_ID;
    doc["BId"] = "cmqkj27va0007xiw295un2g43";
    doc["DId"] = "D1";

    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
        bool n2ok = isNode2Active();
        doc["CO2"]=n2ok?n2_CO2:0; doc["WS"]=n2ok?n2_WS:0; doc["par"]=n2ok?n2_par:0;
        doc["CT"]=n2ok?n2_CT:0;   doc["CH"]=n2ok?n2_CH:0;
        doc["N"]=0; doc["P"]=0; doc["K"]=0;
        doc["ST"]=n2ok?n2_ST:0;   doc["SM"]=n2ok?n2_SM:0; doc["EC"]=n2ok?n2_EC:0; doc["Ph"]=n2ok?n2_Ph:0;
        doc["F1"]=n2ok?n2_F1:0; doc["F2"]=n2ok?n2_F2:0; doc["F3"]=n2ok?n2_F3:0; doc["F4"]=n2ok?n2_F4:0;
        doc["F5"]=n2ok?n2_F5:0; doc["F6"]=n2ok?n2_F6:0; doc["F7"]=n2ok?n2_F7:0; doc["F8"]=n2ok?n2_F8:0;
        doc["NIR"]=n2ok?n2_NIR:0; doc["Clear"]=n2ok?n2_Clear:0;

        bool n1ok = isNode1Active();
        JsonObject n1o = doc.createNestedObject("Node1");
        n1o["N"]=n1ok?n1_N:0; n1o["P"]=n1ok?n1_P:0; n1o["K"]=n1ok?n1_K:0;
        n1o["ST"]=n1ok?n1_ST:0; n1o["SM"]=n1ok?n1_SM:0; n1o["EC"]=n1ok?n1_EC:0;

        xSemaphoreGive(dataMutex);
    }
    return (uint16_t)serializeJson(doc, out, outSize);
}

static uint16_t buildIdleJson(char* out, size_t outSize) {
    StaticJsonDocument<200> doc;
    doc["type"]  = "idle";
    doc["UId"]   = DEVICE_ID;
    doc["DId"]   = "D1";
    doc["Node1"] = isNode1Active() ? "active" : "not received";
    doc["Node2"] = isNode2Active() ? "active" : "not received";
    return (uint16_t)serializeJson(doc, out, outSize);
}

static void enqueuePublish(const char* payload) {
    MqttPubReq req;
    strlcpy(req.payload, payload, sizeof(req.payload));
    if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
        diagPubFail++;
        Serial.println("[!] mqttPubQueue full — dropped");
    }
}

// ================================================================
//  OTA — publish an ota_ack/ota_no_ack up to AWS
// ================================================================
static void publishOtaAck(bool success, const char* target, const char* detail) {
    StaticJsonDocument<220> doc;
    doc["type"]   = success ? "ota_ack" : "ota_no_ack";
    doc["UId"]    = DEVICE_ID;
    doc["target"] = target;
    if (detail && strlen(detail)) doc["detail"] = detail;
    char buf[220];
    serializeJson(doc, buf, sizeof(buf));
    enqueuePublish(buf);
    Serial.printf("OTA ack -> AWS: %s\n", buf);
}

// ================================================================
//  LoRa TX helper (this gateway uses simple direct addressing —
//  no mesh/broadcast, unlike the valve+motor gateway's nodes)
// ================================================================
static void loraSend(uint8_t dest, const uint8_t* data, uint16_t len) {
    LoRa.beginPacket();
    LoRa.write(dest);
    LoRa.write(GATEWAY_ID);
    LoRa.write(data, len);
    LoRa.endPacket();
    LoRa.receive();
}

// ================================================================
//  OTA — arm/retry a LoRa node OTA job
// ================================================================
static void armNodeOtaRetry(uint8_t dest, const char* label,
                            const char* jsonBuf, uint16_t len) {
    nodeOta.active      = true;
    nodeOta.destNode    = dest;
    nodeOta.retryCount  = 0;
    nodeOta.nextRetryMs = millis() + OTA_RETRY_MS;
    nodeOta.payloadLen  = len;
    memcpy(nodeOta.payload, jsonBuf, len);
    strlcpy(nodeOta.nodeLabel, label, sizeof(nodeOta.nodeLabel));
    Serial.printf("OTA armed -> %s (0x%02X) — ack timeout %lus\n",
                  label, dest, OTA_RETRY_MS / 1000);
}

// ================================================================
//  Self-OTA over WiFi — runs on otaTask, NOT mqttTask, so a slow
//  download can never stall modem.mqtt_handle().
// ================================================================
static void selfOtaUpdate(const char* ssid, const char* pass, const char* url) {
    if (!ssid || !strlen(ssid) || !pass || !strlen(pass)) {
        Serial.println("[X] Gateway OTA: missing wifi_ssid/wifi_pass");
        publishOtaAck(false, "sensor gateway", "missing wifi credentials");
        return;
    }

    Serial.printf("Gateway OTA: joining WiFi '%s'...\n", ssid);
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, pass);

    uint32_t deadline = millis() + 20000UL;
    while (WiFi.status() != WL_CONNECTED && millis() < deadline) delay(500);

    if (WiFi.status() != WL_CONNECTED) {
        Serial.println("[X] Gateway OTA: WiFi join failed/timed out");
        publishOtaAck(false, "sensor gateway", "wifi join failed");
        WiFi.mode(WIFI_OFF);
        return;
    }

    Serial.printf("WiFi joined, IP: %s — downloading %s\n",
                  WiFi.localIP().toString().c_str(), url);

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
        if (attempt < MAX_ATTEMPTS) delay(3000);
    }

    switch (result) {
        case HTTP_UPDATE_FAILED:
            publishOtaAck(false, "sensor gateway", httpUpdate.getLastErrorString().c_str());
            break;
        case HTTP_UPDATE_NO_UPDATES:
            publishOtaAck(false, "sensor gateway", "no update available at url");
            break;
        case HTTP_UPDATE_OK:
            Serial.println("[OK] Gateway OTA applied — rebooting");
            break;
    }
    WiFi.mode(WIFI_OFF);
}

// ================================================================
//  otaTask (Core 1, pri 1 — lowest, never blocks LoRa/MQTT)
// ================================================================
void otaTask(void* param) {
    for (;;) {
        SelfOtaJob job;
        if (xQueueReceive(otaJobQueue, &job, portMAX_DELAY) == pdTRUE) {
            Serial.println("otaTask: starting self-OTA job");
            selfOtaUpdate(job.ssid, job.pass, job.url);
        }
    }
}

// ================================================================
//  LoRa TASK  (Core 0, pri 3)
// ================================================================
void reinitLoRaRadio() {
    LoRa.end();
    pinMode(LORA_RST, OUTPUT);
    digitalWrite(LORA_RST, LOW);  delay(10);
    digitalWrite(LORA_RST, HIGH); delay(10);
    if (!LoRa.begin(LORA_FREQ)) { Serial.println("[X] LoRa reinit failed"); return; }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    LoRa.receive();
    diagRadioReinits++;
    Serial.println("[OK] LoRa reinitialised");
}

void loraTask(void* param) {
    LoRa.receive();
    Serial.println("LoRa task — RX mode");
    uint32_t lastWatchdog = millis();

    for (;;) {
        int packetSize = LoRa.parsePacket();
        if (packetSize >= 2) {
            LoRaRxPacket rx;
            rx.dest = LoRa.read();
            rx.src  = LoRa.read();
            int bodyLen = packetSize - 2;
            if (bodyLen > (int)(sizeof(rx.buf)-1)) bodyLen = sizeof(rx.buf)-1;
            rx.len  = (uint16_t)LoRa.readBytes(rx.buf, bodyLen);
            rx.buf[rx.len] = '\0';
            rx.rssi = LoRa.packetRssi();
            rx.snr  = LoRa.packetSnr();
            lastLoraRssi = rx.rssi;
            lastWatchdog = millis();
            diagRxTotal++;

            if (rx.dest == GATEWAY_ID) {
                if (xQueueSend(rxQueue, &rx, 0) != pdTRUE)
                    Serial.printf("[!] rxQueue full — dropped 0x%02X\n", rx.src);
            }
            LoRa.receive();
        }

        if ((millis() - lastWatchdog) >= LORA_WATCHDOG_MS) {
            Serial.println("[!] LoRa watchdog — reinitialising");
            reinitLoRaRadio();
            lastWatchdog = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

// ================================================================
//  MQTT CALLBACK  — runs ON mqttTask. Keep this FAST: OTA work is
//  queued off to otaTask, never run inline here.
// ================================================================
void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len) {
    char buf[512];
    uint32_t n = (len < sizeof(buf)-1) ? len : sizeof(buf)-1;
    memcpy(buf, payload, n); buf[n] = '\0';
    Serial.printf("MQTT IN [%s]: %s\n", topic, buf);

    if (strcmp(topic, "OTA/mode") != 0) return;

    DynamicJsonDocument doc(512);
    if (deserializeJson(doc, buf) != DeserializationError::Ok) {
        Serial.println("[!] OTA JSON parse error");
        return;
    }

    const char* uid = doc["UId"] | "";
    if (strcmp(uid, DEVICE_ID) != 0) {
        Serial.println("[skip] OTA UId mismatch");
        return;
    }

    const char* deviceType = doc["device"] | "";
    const char* url        = doc["url"]    | "";
    const char* ssid        = doc["wifi_ssid"] | "";
    const char* pass        = doc["wifi_pass"] | "";

    if (!strlen(url)) {
        Serial.println("[!] OTA: missing url");
        return;
    }

    if (strcasecmp(deviceType, "sensor gateway") == 0 ||
        strcasecmp(deviceType, "sensorgateway")  == 0 ||
        strcasecmp(deviceType, "sensor node gateway") == 0) {   // OTA page dropdown sends this exact string
        // Self-OTA — hand off to otaTask, don't block mqttTask.
        SelfOtaJob job;
        strlcpy(job.ssid, ssid, sizeof(job.ssid));
        strlcpy(job.pass, pass, sizeof(job.pass));
        strlcpy(job.url,  url,  sizeof(job.url));
        if (xQueueSend(otaJobQueue, &job, 0) != pdTRUE) {
            Serial.println("[!] otaJobQueue full — OTA already in progress");
            publishOtaAck(false, "sensor gateway", "OTA already in progress");
        } else {
            Serial.println("Self-OTA job queued");
        }
        return;
    }

    if (strcasecmp(deviceType, "sensor node") == 0) {
        if (nodeOta.active) {
            Serial.println("[!] Node OTA already in progress — ignoring new request");
            return;
        }
        int nodeNum = doc["NodeNum"] | 0;
        uint8_t dest;
        const char* label;
        if      (nodeNum == 1) { dest = NODE1_ID; label = "Node1"; }
        else if (nodeNum == 2) { dest = NODE2_ID; label = "Node2"; }
        else {
            Serial.println("[!] OTA: invalid/missing NodeNum (must be 1 or 2)");
            publishOtaAck(false, "sensor node", "invalid NodeNum");
            return;
        }

        StaticJsonDocument<220> pkt;
        pkt["type"] = "ota";
        pkt["url"]  = url;
        if (strlen(ssid)) pkt["wifi"] = ssid;
        if (strlen(pass)) pkt["pass"] = pass;

        char pbuf[220];
        uint16_t plen = (uint16_t)serializeJson(pkt, pbuf, sizeof(pbuf));
        if (plen >= sizeof(pbuf)) {
            Serial.println("[X] OTA packet too large for LoRa payload — shorten the url");
            return;
        }

        loraSend(dest, (const uint8_t*)pbuf, plen);
        Serial.printf("[OTA attempt 1/3] -> %s (0x%02X)\n", label, dest);
        armNodeOtaRetry(dest, label, pbuf, plen);
        return;
    }

    Serial.printf("[skip] OTA: unrecognised device type '%s'\n", deviceType);
}

// ================================================================
//  MQTT CONNECT
// ================================================================
bool mqttConnectOnce() {
    modem.mqtt_set_certificate(root_ca, certificate, privateKey);
    if (!modem.mqtt_connect(0, broker, broker_port, client_id, NULL, NULL, 120)) return false;
    modem.mqtt_set_callback(mqttCallback);
    modem.mqtt_subscribe(0, subscribe_topic, 0);
    modem.mqtt_subscribe(0, "OTA/mode", 1);
    return true;
}

// ================================================================
//  MQTT TASK  (Core 1, pri 3)
// ================================================================
void mqttTask(void* param) {
    pinMode(MODEM_PWKEY, OUTPUT);
    digitalWrite(MODEM_PWKEY, LOW);
    vTaskDelay(pdMS_TO_TICKS(1500));
    digitalWrite(MODEM_PWKEY, HIGH);
    vTaskDelay(pdMS_TO_TICKS(5000));

    Serial.println("Modem booting...");
    modem.init();
    vTaskDelay(pdMS_TO_TICKS(500));
    Serial.println(modem.getModemInfo());

    modem.sendAT("+CNMP=38");
    modem.waitResponse();

    uint32_t netDeadline = millis() + 90000UL;
    while (!modem.waitForNetwork(5000)) {
        Serial.println("Registering...");
        if (millis() > netDeadline) {
            modem.restart(); vTaskDelay(pdMS_TO_TICKS(5000));
            netDeadline = millis() + 90000UL;
        }
    }
    Serial.println("[OK] Network registered");

    while (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
        Serial.println("GPRS retry...");
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
    Serial.println("[OK] GPRS | IP: " + modem.localIP().toString());

    modem.mqtt_begin(true);
    modem.sendAT("+CMQTTCFG=\"keepalive\",0,120");
    modem.waitResponse(2000);
    modem.mqtt_set_callback(mqttCallback);

    enum State { CONNECTING, CONNECTED } state = CONNECTING;
    uint32_t retryDelay    = MQTT_RETRY_MIN_MS;
    uint32_t nextRetry     = 0;
    uint32_t lastHandleMs  = 0;
    uint32_t lastSuccessMs = millis();
    uint32_t lastSigPoll   = 0;
    bool     notified      = false;
    uint8_t  failCount     = 0;

    for (;;) {
        uint32_t now = millis();

        if (now - lastHandleMs >= 10) {
            lastHandleMs = now;
            modem.mqtt_handle();
        }

        if (state == CONNECTED && now - lastSigPoll >= 30000UL) {
            lastSigPoll = now;
            int q = modem.getSignalQuality();
            if      (q == 99 || q < 0) simSignalBars = 0;
            else if (q >= 20)           simSignalBars = 4;
            else if (q >= 14)           simSignalBars = 3;
            else if (q >= 8)            simSignalBars = 2;
            else if (q >= 2)            simSignalBars = 1;
            else                        simSignalBars = 0;
        }

        if (state == CONNECTED) {
            MqttPubReq req;
            if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                if (modem.mqtt_publish(0, publish_topic, req.payload, 0)) {
                    diagPubOk++;
                    failCount     = 0;
                    lastPublishMs = millis();
                    Serial.println("[OK] Published: " + String(req.payload));
                    flushCmqttPub();
                } else {
                    diagPubFail++;
                    Serial.println("[X] Publish FAILED — re-queuing");
                    xQueueSend(mqttPubQueue, &req, 0);
                    if (++failCount >= MAX_MQTT_FAILURES) {
                        failCount  = 0;
                        notified   = false;
                        state      = CONNECTING;
                        mqttOnline = false;
                        nextRetry  = now + retryDelay;
                    }
                }
            }
        }

        switch (state) {
        case CONNECTING:
            if (now < nextRetry) break;

            if (lastSuccessMs > 0 && (now - lastSuccessMs) > LTE_WATCHDOG_MS) {
                Serial.println("LTE watchdog — restarting modem");
                modem.restart();
                vTaskDelay(pdMS_TO_TICKS(5000));
                modem.waitForNetwork(15000);
                modem.gprsConnect(apn, gprsUser, gprsPass);
                lastSuccessMs = millis();
                retryDelay    = MQTT_RETRY_MIN_MS;
                nextRetry     = 0;
                break;
            }

            Serial.println("MQTT connecting...");
            if (mqttConnectOnce()) {
                state         = CONNECTED;
                retryDelay    = MQTT_RETRY_MIN_MS;
                lastSuccessMs = millis();
                notified      = false;
                failCount     = 0;
                mqttOnline    = true;
                diagReconnects++;
                Serial.println("[OK] MQTT connected");
            } else {
                mqttOnline = false;
                Serial.printf("[X] MQTT failed — retry in %lus\n", retryDelay/1000);
                nextRetry  = now + retryDelay;
                retryDelay = min(retryDelay * 2, (uint32_t)MQTT_RETRY_MAX_MS);
            }
            break;

        case CONNECTED:
            if (!modem.mqtt_connected()) {
                Serial.println("[!] MQTT dropped — reconnecting");
                mqttOnline = false;
                notified   = false;
                state      = CONNECTING;
                nextRetry  = now + retryDelay;
                break;
            }
            if (!notified) {
                notified = true;
                Serial.println("MQTT online");
                if (pendingData) {
                    pendingData = false;
                    enqueuePublish(pendingBuf);
                    Serial.println("Buffered data published after reconnect");
                }
            }
            lastSuccessMs = now;
            break;
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ================================================================
//  APP TASK  (Core 1, pri 2)
// ================================================================
void appTask(void* param) {
    static uint32_t lastIdleCheck = 0;
    static uint32_t lastOledMs    = 0;
    static uint32_t lastDiag      = 0;

    for (;;) {
        uint32_t now = millis();

        LoRaRxPacket pkt;
        while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) {

            String payload((char*)pkt.buf, pkt.len);
            payload.trim();

            int js = payload.indexOf('{');
            if (js < 0 || !payload.endsWith("}")) {
                diagRxBad++;
                Serial.printf("[X] Invalid packet 0x%02X\n", pkt.src);
                continue;
            }
            if (js > 0) payload = payload.substring(js);

            DynamicJsonDocument tmp(512);
            if (deserializeJson(tmp, payload) != DeserializationError::Ok) {
                diagRxBad++;
                Serial.printf("[X] JSON parse fail 0x%02X\n", pkt.src);
                continue;
            }

            Serial.printf("\nLoRa 0x%02X->0x%02X  RSSI:%d  SNR:%.1f\n",
                          pkt.src, pkt.dest, pkt.rssi, pkt.snr);

            // ---- ota_ack from the node currently being updated ----
            const char* type = tmp["type"] | "";
            if (strcmp(type, "ota_ack") == 0) {
                if (nodeOta.active && pkt.src == nodeOta.destNode) {
                    const char* status = tmp["status"] | "fail";
                    bool ok = (strcasecmp(status, "fail") != 0);
                    Serial.printf("OTA ack from %s — status:%s\n", nodeOta.nodeLabel, status);
                    publishOtaAck(ok, nodeOta.nodeLabel, status);
                    nodeOta.active = false;
                } else {
                    Serial.println("[skip] stale/unexpected ota_ack");
                }
                continue;
            }

            bool parsed = false;

            if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(50)) == pdTRUE) {

                if (pkt.src == NODE1_ID) {
                    n1_N  = tmp["N"]  | 0.0f;
                    n1_P  = tmp["P"]  | 0.0f;
                    n1_K  = tmp["K"]  | 0.0f;
                    n1_ST = tmp["ST"] | 0.0f;
                    n1_SM = tmp["SM"] | 0.0f;
                    n1_EC = tmp["EC"] | 0.0f;
                    node1Received = true;
                    lastNode1     = millis();
                    idleSent      = false;
                    parsed        = true;
                    Serial.printf("Node1 -> N=%.0f P=%.0f K=%.0f ST=%.1f SM=%.0f EC=%.1f\n",
                                  n1_N, n1_P, n1_K, n1_ST, n1_SM, n1_EC);

                } else if (pkt.src == NODE2_ID) {
                    n2_CO2   = tmp["CO2"]  | 0.0f;
                    n2_WS    = tmp["WS"]   | 0.0f;
                    n2_par   = tmp["PAR"]  | 0.0f;
                    n2_CT    = tmp["CT"]   | 0.0f;
                    n2_CH    = tmp["CH"]   | 0.0f;
                    n2_ST    = tmp["ST"]   | 0.0f;
                    n2_SM    = tmp["SM"]   | 0.0f;
                    n2_EC    = tmp["TDS"]  | 0.0f;
                    n2_Ph    = tmp["Ph"]   | 0.0f;
                    if (n2_SM < 0) n2_SM = 0;
                    if (n2_EC < 0) n2_EC = 0;
                    n2_F1    = tmp["F1"]   | 0.0f;
                    n2_F2    = tmp["F2"]   | 0.0f;
                    n2_F3    = tmp["F3"]   | 0.0f;
                    n2_F4    = tmp["F4"]   | 0.0f;
                    n2_F5    = tmp["F5"]   | 0.0f;
                    n2_F6    = tmp["F6"]   | 0.0f;
                    n2_F7    = tmp["F7"]   | 0.0f;
                    n2_F8    = tmp["F8"]   | 0.0f;
                    n2_NIR   = tmp["NIR"]  | 0.0f;
                    n2_Clear = tmp["Clear"]| 0.0f;
                    node2Received = true;
                    lastNode2     = millis();
                    idleSent      = false;
                    parsed        = true;
                    Serial.printf("Node2 -> CO2=%.0f par=%.1f CT=%.1f CH=%.1f ST=%.1f SM=%.0f TDS->EC=%.1f Ph=%.1f\n",
                                  n2_CO2, n2_par, n2_CT, n2_CH, n2_ST, n2_SM, n2_EC, n2_Ph);

                } else {
                    Serial.printf("[skip] Unknown 0x%02X\n", pkt.src);
                }

                xSemaphoreGive(dataMutex);
            } else {
                Serial.println("[!] dataMutex timeout");
            }

            if (parsed) {
                char buf[512];
                buildDataJson(buf, sizeof(buf));
                if (mqttOnline) {
                    enqueuePublish(buf);
                } else {
                    strlcpy(pendingBuf, buf, sizeof(pendingBuf));
                    pendingData = true;
                    Serial.println("[!] MQTT offline — buffered for retry");
                }
                oledRefresh();
            }
        }

        if (now - lastOledMs >= 1000UL) {
            lastOledMs = now;
            oledRefresh();
        }

        if (now - lastIdleCheck >= 10000UL) {
            lastIdleCheck = now;
            if (!eitherActive() && !idleSent &&
                (node1Received || node2Received) && mqttOnline) {
                idleSent = true;
                diagIdleSent++;
                char buf[200];
                buildIdleJson(buf, sizeof(buf));
                enqueuePublish(buf);
                Serial.println("Idle enqueued");
            }
        }

        // ---- LoRa node OTA retry ticker ----
        if (nodeOta.active && now >= nodeOta.nextRetryMs) {
            if (nodeOta.retryCount < OTA_MAX_RETRIES) {
                nodeOta.retryCount++;
                Serial.printf("[OTA attempt %d/3] retry -> %s\n",
                              nodeOta.retryCount + 1, nodeOta.nodeLabel);
                loraSend(nodeOta.destNode, (const uint8_t*)nodeOta.payload, nodeOta.payloadLen);
                nodeOta.nextRetryMs = now + OTA_RETRY_MS;
            } else {
                Serial.printf("[X] OTA — all 3 attempts exhausted for %s\n", nodeOta.nodeLabel);
                publishOtaAck(false, nodeOta.nodeLabel, "no ack after 3 attempts");
                nodeOta.active = false;
            }
        }

        if (now - lastDiag >= 5UL * 60 * 1000) {
            lastDiag = now;
            Serial.println("\n====== DIAGNOSTICS ======");
            Serial.printf("  LoRa RX:       %lu\n",  diagRxTotal);
            Serial.printf("  RX bad:        %lu\n",  diagRxBad);
            Serial.printf("  MQTT pub OK:   %lu\n",  diagPubOk);
            Serial.printf("  MQTT pub fail: %lu\n",  diagPubFail);
            Serial.printf("  Reconnects:    %lu\n",  diagReconnects);
            Serial.printf("  Radio reinits: %lu\n",  diagRadioReinits);
            Serial.printf("  Idle sent:     %lu\n",  diagIdleSent);
            Serial.printf("  Node1 active:  %s\n",   isNode1Active()?"YES":"NO");
            Serial.printf("  Node2 active:  %s\n",   isNode2Active()?"YES":"NO");
            Serial.printf("  MQTT online:   %s\n",   mqttOnline?"YES":"NO");
            Serial.printf("  Free heap:     %u B\n", esp_get_free_heap_size());
            if (nodeOta.active)
                Serial.printf("  OTA pending:   %s attempt %d/3\n", nodeOta.nodeLabel, nodeOta.retryCount + 1);
            Serial.println("==========================\n");
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

// ================================================================
//  SETUP
// ================================================================
void setup() {
    SerialMon.begin(115200);
    SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
    delay(2000);

    Wire.begin(21, 22);
    if (oled.begin(SSD1306_SWITCHCAPVCC, OLED_I2C_ADDR)) {
        oledReady = true;
        oled.clearDisplay();
        oled.setTextSize(1);
        oled.setTextColor(SSD1306_WHITE);
        oled.setCursor(20, 24);
        oled.print("Gateway booting");
        oled.display();
        Serial.println("[OK] OLED ready");
    } else {
        Serial.println("[!] OLED not found — continuing without display");
    }

    dataMutex    = xSemaphoreCreateMutex();
    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));
    otaJobQueue  = xQueueCreate(1, sizeof(SelfOtaJob));

    if (!dataMutex || !rxQueue || !mqttPubQueue || !otaJobQueue) {
        Serial.println("[X] Resource init failed — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
    if (!LoRa.begin(LORA_FREQ)) {
        Serial.println("[X] LoRa init FAILED — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    Serial.println("[OK] LoRa ready — SF7 BW125 CR5 0xAB");

    xTaskCreatePinnedToCore(loraTask, "LoRa", 4096,  NULL, 3, &loraTaskHandle, 0);
    xTaskCreatePinnedToCore(mqttTask, "MQTT", 10240, NULL, 3, &mqttTaskHandle, 1);
    xTaskCreatePinnedToCore(appTask,  "App",  6144,  NULL, 2, &appTaskHandle,  1);
    xTaskCreatePinnedToCore(otaTask,  "OTA",  8192,  NULL, 1, &otaTaskHandle,  1);

    Serial.println("[OK] Sensor gateway started (with OTA)");
    vTaskSuspend(NULL);
}

void loop() { vTaskDelay(portMAX_DELAY); }
