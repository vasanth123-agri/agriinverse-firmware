/* ================================================================
   AgriNode Gateway v8 (4-node build) — TWO device types on one gateway:
     Node1 — one NPK soil sensor, OLD simple format (dest, src),
             embedded as a nested "Node1" object in every publish
     Node2 — 4 AgriNode boards running v7.2/v5.1 firmware, NEW
             broadcast format (origin, ttl, seq — no fid/bid/uid),
             published one flat object at a time as each device's
             reading arrives
   GSM (A7670) + AWS IoT MQTT + LoRa 433 MHz SF7 BW125
   0.96" SSD1306 OLED status display (128x64, I2C 0x3C)
   ----------------------------------------------------------------
   How incoming packets are told apart (loraTask):
   - First byte == GATEWAY_ID  -> old format -> Node1 (NPK)
   - First byte != GATEWAY_ID  -> new format -> one of the 8 Node2
     boards, broadcasting openly (first byte is that board's own
     NODE_ID, matching the #define NODE_ID in its firmware)
   ----------------------------------------------------------------
   v8 CHANGE FROM v7 GATEWAY:
   The v7 gateway expected an 8-byte Node2 header with fid/bid/uid
   fields. The v7.2/v5.1 node firmware only ever sends a 4-byte
   header — origin, ttl, seq_lo, seq_hi — with NO fid/bid/uid at
   all; a board's identity is just its NODE_ID (the origin byte).
   So this version:
     - reads only PKT_HDR_LEN=4 bytes of header for Node2 packets
     - indexes the node table by NODE_ID (origin) instead of FId,
       via a small linear-search table (cloudIds[]) that now also
       carries each board's NODE_ID
     - carries the node firmware's new "TDSV"/"SoilV" raw-voltage
       fields through into the published JSON
   Node1 (NPK) handling is untouched — it never had fid/bid/uid to
   begin with.

   v9 ADDITIONS (this file):
   1. BATTERY — the AgriNode boards' INA219 fields "VB","IB","BP","CHG"
      are parsed per board and published as a nested "Bat" object
      (same layout as the Dual-Node OTA gateway):
        "Bat":{"VB":3.28,"IB":-412,"BP":67,"CHG":1}
      IB > 0 = discharging, IB < 0 = charging. CHG: 1 chg, -1 dischg,
      0 idle/full, -2 INA219 offline. A board that doesn't send them
      (or has gone stale) publishes BP = -1 / CHG = -2, never a
      plausible-looking 0.
   2. OTA (ported from the Dual-Node OTA gateway), topic "OTA/mode":
      Self-OTA of THIS gateway over WiFi:
        {"UId":"55","device":"sensor gateway",
         "wifi_ssid":"...","wifi_pass":"...","url":"https://.../fw.bin"}
      Relay to the NPK Node1 over LoRa (NodeNum 1 only):
        {"UId":"55","device":"sensor node","NodeNum":1,
         "wifi_ssid":"...","wifi_pass":"...","url":"..."}
      Node1 must reply {"type":"ota_ack","status":"ok"/"fail"} to
      GATEWAY_ID. 3 attempts, 90 s apart, then "ota_no_ack" to AWS.
      The STM32 AgriNode boards (0xC1..) have no OTA receiver, so any
      other NodeNum is refused with an ota_no_ack.
   3. All LoRa TX now goes through a queue owned by loraTask — the
      radio is only ever touched from one task.
   4. Buffers raised to the reference gateway's sizes (RX parse doc
      1024, publish doc 1536, payload 768): the old 512-byte RX doc
      was too small for the ~26-key AgriNode packet.
   5. HEALTH + ACTIVE-CHECK
      device/health — every 15 min, and once after each MQTT
      (re)connect:
        {"type":"health","UId":"66","status":"active",
         "gsm":{"csq":18,"rssi_dbm":-77,"bars":3},
         "lora":{"last_rssi":-82,
                 "nodes":[{"id":"0xC1","rssi":-80,"age_s":12,"active":true},...],
                 "n1_rssi":-75,"n1_age_s":30},
         "NodesActive":3,"Node1":"active","mqtt":"online",
         "uptime_s":12345,"free_heap":123456}
      device/active — sent IMMEDIATELY when the app publishes on
      device/cmd:
        {"UId":"66","cmd":"check_active"}
      Same body as health, with "type":"active".
      AWS IoT policy must allow iot:Publish on device/health and
      device/active, and iot:Subscribe/Receive on device/cmd.

   SETUP REQUIRED: fill in each board's actual NODE_ID in
   cloudIds[] below (must match that board's #define NODE_ID in
   the node firmware) — the placeholders here (0xC1..0xC8) are
   NOT real addresses.
   ================================================================ */

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
#include "freertos/semphr.h"


#include "certificate.h"
#include "privatekey.h"
#include "root_ca.h"

#define DEVICE_ID    "66"
#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY  4
#define LORA_SS      5
#define LORA_RST     14
#define LORA_DIO0    26
#define LORA_FREQ    433E6

#define GATEWAY_ID   0xBB
#define NODE1_ID     0xAA        /* the separate NPK device — old simple
                                     2-byte (dest, src) format, unrelated
                                     to the Node2 boards below */
#define NODE_COUNT     4         /* 4 physical Node2 boards */

/* ================================================================
 *  CLOUD IDENTITY LOOKUP — fill in your actual platform IDs AND
 *  each board's real NODE_ID here. These are NOT sent over LoRa
 *  (they're far too long for that, and NODE_ID is the only
 *  identity the radio carries) — the gateway translates the
 *  incoming origin byte into your platform's real record IDs
 *  right before publishing. REPLACE every placeholder below with
 *  your real values before deploying.
 *
 *  UId is shared across your whole deployment (one account/owner
 *  ID), so it's a single constant, not part of the per-node table.
 * ================================================================ */
#define SHARED_UID   "66"

struct CloudIdentity {
    uint8_t     nodeId; /* MUST match that board's #define NODE_ID  */
    const char* FId;    /* your platform's Field record ID          */
    const char* Id;     /* e.g. "M1" — device/model slot ID          */
    const char* BId;    /* your platform's Block record ID           */
    const char* DId;    /* e.g. "D1" — deployment ID                 */
};
static const CloudIdentity cloudIds[NODE_COUNT] = {
    /* NODE_ID  FId                            Id    BId                            DId */
    { 0xC1, "cmswv2gnx001342b0b6bj1bwd", "M1", "cmswv2i62001742b00omppsyk", "D1" },
    { 0xC2, "cmwbv2x1u0001tmm4vlz5whfz", "M2", "cmwbv2x1u0003tmm43z3xvpgh", "D2" },
    { 0xC3, "cmswukkav000d420ine0pfxmw", "M3", "cmswuklzh000h420ih0axdrgj", "D3" },
    { 0xC4, "cmswtv2k0004542uw2emsoa1b", "M4", "cmswtv44v004942uwbaslh168", "D4" },
};

#define LORA_SF        7
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xAB
#define LORA_TXPOWER   20
#define LORA_PREAMBLE  8

#define IDLE_TIMEOUT         120000UL
#define SEND_TIMEOUT          15000UL
#define MQTT_RETRY_MIN_MS     10000UL
#define MQTT_RETRY_MAX_MS     60000UL
#define LORA_WATCHDOG_MS   (5UL * 60 * 1000)
#define LTE_WATCHDOG_MS   (10UL * 60 * 1000)
#define MAX_MQTT_FAILURES      5
#define HEALTH_INTERVAL_MS (15UL * 60 * 1000)   /* device/health every 15 min */

/* Health / active-check topics. broker, publish_topic and
 * subscribe_topic still come from awscerts.h.                      */
static const char health_topic[] = "device/health";
static const char active_topic[] = "device/active";
static const char cmd_topic[]    = "device/cmd";     /* check_active arrives here */

#define RX_QUEUE_DEPTH        8
#define MQTT_PUB_QUEUE_DEPTH  8
#define LORA_TX_QUEUE_DEPTH   4

/* Buffer sizes — from the reference OTA gateway. serializeJson()
 * truncates silently, so undersized buffers show up as malformed
 * JSON at AWS rather than as an error here. Don't shrink these.  */
#define PUB_PAYLOAD_SIZE     768
#define RX_JSON_DOC_SIZE    1024
#define PUB_JSON_DOC_SIZE   1536

#define OTA_MAX_RETRIES  2          /* + first send = 3 attempts */
#define OTA_RETRY_MS     90000UL

#define OLED_W         128
#define OLED_H         64
#define OLED_RESET     -1
#define OLED_I2C_ADDR  0x3C
#define OLED_ROW0_Y    0
#define OLED_ROW1_Y    15   /* Nodes active x/4                    */
#define OLED_ROW2_Y    25   /* rotating board: last-seen           */
#define OLED_ROWB_Y    35   /* same board: battery                 */
#define OLED_ROWN1_Y   45   /* NPK Node1                           */
#define OLED_ROW3_Y    56   /* Cloud (last 8 px: 56-63)            */

/* v8: Node2 header is now origin,ttl,seq_lo,seq_hi ONLY — no
 * fid/bid/uid, matching the v7.2/v5.1 node firmware's
 * LoRa_SendPacket()/PKT_HDR_LEN. */
#define PKT_HDR_LEN    4

struct LoRaRxPacket {
    bool     isNode1;   /* true = old 2-byte format (dest,src), the NPK device */
    uint8_t  origin;    /* Node1: its src address. Node2: its NODE_ID */
    uint8_t  ttl;
    uint16_t seq;
    uint8_t  buf[256];
    uint16_t len;
    int      rssi;
    float    snr;
};

struct MqttPubReq {
    char topic[40];                  /* device/data, device/health, device/active */
    char payload[PUB_PAYLOAD_SIZE];
};

/* One outgoing LoRa frame: [dest][GATEWAY_ID][data...] */
struct LoRaTxReq {
    uint8_t  dest;
    uint8_t  data[220];
    uint16_t len;
};

/* OTA state for the LoRa node currently being updated */
struct OtaPending {
    bool     active      = false;
    uint8_t  destNode    = 0;
    uint8_t  retryCount  = 0;
    uint32_t nextRetryMs = 0;
    char     payload[220]= {};
    uint16_t payloadLen  = 0;
    char     nodeLabel[16]= {};
};

/* Job handed from mqttTask -> otaTask so a slow self-OTA download
 * never stalls modem.mqtt_handle() (must be pumped every 10 ms).  */
struct SelfOtaJob {
    char ssid[64] = {};
    char pass[64] = {};
    char url[220] = {};
};

/* One slot per configured board — index = position in cloudIds[].
 * Node identity is the NODE_ID (radio origin byte) itself now;
 * there's no separate fid/bid/uid layer in the v7.2/v5.1 packet. */
struct NodeState {
    bool     everSeen  = false;
    uint8_t  nodeId    = 0;
    uint16_t lastSeq   = 0;
    unsigned long lastSeen = 0;
    int      lastRssi  = -120;   /* RSSI of this board's last packet */

    float CO2=0, CT=0, CH=0, ST=0, SM=0, SoilV=0, TDS=0, TDSV=0, PAR=0;
    float F1=0, F2=0, F3=0, F4=0, F5=0, F6=0, F7=0, F8=0, NIR=0, Clear=0;

    /* Battery (INA219) — defaults mean "unknown", not "flat" */
    float VB  = 0;     /* bus voltage, V              */
    float IB  = 0;     /* mA, + = discharging         */
    int   BP  = -1;    /* %, -1 = unknown             */
    int   CHG = -2;    /* 1 chg, -1 dischg, 0 idle, -2 offline */
};
static NodeState nodeTable[NODE_COUNT];

/* Node1 — the separate NPK device. Just one of these exists, so it's
 * plain shared state (mutex-protected, same as nodeTable), not part
 * of the node table. Its latest reading gets embedded as the "Node1"
 * nested object in every Node2 device's publish, same as before. */
static bool          n1_everSeen = false;
static unsigned long n1_lastSeen = 0;
static int           n1_lastRssi = -120;
static float n1_N=0, n1_P=0, n1_K=0, n1_ST=0, n1_SM=0, n1_EC=0;

bool isNode1Active() {
    return n1_everSeen && (millis() - n1_lastSeen <= IDLE_TIMEOUT);
}

/* ================================================================
 *  EXPLICIT FORWARD DECLARATIONS
 *
 *  Arduino IDE auto-generates function prototypes for you via a
 *  ctags-based scan of the sketch. That scanner is fragile: if it
 *  miscounts a brace/paren ANYWHERE earlier in the sketch (in this
 *  file or another .ino tab in the same folder — Arduino
 *  concatenates all tabs before compiling), every function
 *  definition after that point gets misread as nested inside the
 *  previous one, producing errors like "cannot declare static
 *  function inside another function" on code that is actually
 *  fine at global scope.
 *
 *  Declaring everything explicitly here means the auto-generator
 *  has nothing left to guess at for these functions, which
 *  sidesteps the bug regardless of what triggered it elsewhere.
 * ================================================================ */
static int  nodeIndex(uint8_t nodeId);
bool        isNodeActive(int idx);
bool        anyNodeActive();
static void oledDrawBars(int16_t x, int16_t bottomY, uint8_t level);
static void oledFmtAgo(char* buf, size_t sz, uint16_t sec);
static void oledRefresh();
static void flushCmqttPub();
static uint16_t buildDeviceJson(char* out, size_t outSize, int idx);
static uint16_t buildIdleJson(char* out, size_t outSize);
static void enqueuePublish(const char* payload);
static void enqueuePublishTo(const char* topic, const char* payload, bool urgent);
static int  csqToDbm(int csq);
static uint8_t csqToBars(int q);
static void pollSignal();
static uint16_t buildHealthJson(char* out, size_t outSize, const char* type);
void reinitLoRaRadio();
void loraTask(void* param);
void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len);
bool mqttConnectOnce();
void clearError();
void mqttTask(void* param);
void appTask(void* param);
void otaTask(void* param);
static const char* chgLabel(int chg);
static void publishOtaAck(bool success, const char* target, const char* detail);
static void loraSend(uint8_t dest, const uint8_t* data, uint16_t len);
static void armNodeOtaRetry(uint8_t dest, const char* label,
                            const char* jsonBuf, uint16_t len);
static void selfOtaUpdate(const char* ssid, const char* pass, const char* url);

static QueueHandle_t     rxQueue;
static QueueHandle_t     mqttPubQueue;
static QueueHandle_t     loraTxQueue;
static QueueHandle_t     otaJobQueue;
static SemaphoreHandle_t dataMutex;

static TaskHandle_t loraTaskHandle = nullptr;
static TaskHandle_t mqttTaskHandle = nullptr;
static TaskHandle_t appTaskHandle  = nullptr;
static TaskHandle_t otaTaskHandle  = nullptr;

static OtaPending nodeOta;   /* one LoRa-node OTA in flight at a time */

TinyGsm modem(SerialAT);

static Adafruit_SSD1306 oled(OLED_W, OLED_H, &Wire, OLED_RESET);
static bool oledReady = false;

static volatile bool  mqttOnline    = false;
static volatile bool  idleSent      = false;
static char pendingBuf[PUB_PAYLOAD_SIZE] = {};
static bool pendingData     = false;
static volatile unsigned long lastPublishMs  = 0;
static volatile int           lastLoraRssi   = -120;
static volatile uint8_t       simSignalBars  = 0;
static volatile int           simCsq         = 99;   /* raw AT+CSQ, 99 = unknown */

/* Raised by mqttCallback on device/cmd check_active, serviced by
 * mqttTask. The callback runs inside mqtt_handle(), so it must not
 * send AT+CSQ itself.                                              */
static volatile bool activeCheckRequested = false;

static uint32_t diagRxTotal=0,    diagRxBad=0;
static uint32_t diagPubOk=0,      diagPubFail=0;
static uint32_t diagReconnects=0, diagRadioReinits=0;
static uint32_t diagIdleSent=0;

/* ================================================================
 *  Node table helpers — this is the only place node identity
 *  logic lives; no board's cloud IDs are ever hardcoded elsewhere.
 * ================================================================ */
static int nodeIndex(uint8_t nodeId) {
    for (int i = 0; i < NODE_COUNT; i++)
        if (cloudIds[i].nodeId == nodeId) return i;
    return -1;   /* origin not in cloudIds[] — unconfigured board */
}
bool isNodeActive(int idx) {
    if (idx < 0) return false;
    return nodeTable[idx].everSeen &&
           (millis() - nodeTable[idx].lastSeen <= IDLE_TIMEOUT);
}
bool anyNodeActive() {
    for (int i = 0; i < NODE_COUNT; i++) if (isNodeActive(i)) return true;
    return false;
}

/* Short charge-state label for OLED / serial */
static const char* chgLabel(int chg) {
    switch (chg) {
        case  1: return "CHG";
        case -1: return "DIS";
        case  0: return "IDL";
        default: return "N/A";   /* -2 = INA219 not responding */
    }
}

/* ================================================================
 *  OLED helpers
 * ================================================================ */
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

    /* Line 1: overall summary. */
    int activeCount = 0;
    for (int i = 0; i < NODE_COUNT; i++) if (isNodeActive(i)) activeCount++;

    oled.setCursor(0, OLED_ROW1_Y);
    oled.print("Nodes active: ");
    oled.print(activeCount);
    oled.print("/");
    oled.print(NODE_COUNT);

    /* Line 2: one Node2 board's own timing at a time, rotating every
     * 2 seconds through all configured boards — "Xs ago" if
     * received, "--" if that board has never reported since the
     * gateway booted. Labeled with its NODE_ID (hex), since that's
     * the board's actual on-air identity now (no more FID). */
    {
        int i = (int)((now / 2000UL) % NODE_COUNT);
        uint16_t sec = nodeTable[i].everSeen
            ? (uint16_t)min((now - nodeTable[i].lastSeen) / 1000UL, 0xFFFEUL)
            : 0xFFFF;

        oledFmtAgo(tmp, sizeof(tmp), sec);
        oled.setCursor(0, OLED_ROW2_Y);
        oled.print("0x");
        if (cloudIds[i].nodeId < 0x10) oled.print("0");
        oled.print(cloudIds[i].nodeId, HEX);
        oled.print(": ");
        oled.print(tmp);

        /* Battery of the same board, so the two lines always match */
        oled.setCursor(0, OLED_ROWB_Y);
        const NodeState &n = nodeTable[i];
        if (!isNodeActive(i)) {
            oled.print("  B:--");
        } else if (n.CHG == -2) {
            oled.print("  B:INA219 offline");
        } else if (n.BP >= 0) {
            snprintf(tmp, sizeof(tmp), "  B:%.2fV %d%% %s",
                     n.VB, n.BP, chgLabel(n.CHG));
            oled.print(tmp);
        } else {
            snprintf(tmp, sizeof(tmp), "  B:%.2fV %s", n.VB, chgLabel(n.CHG));
            oled.print(tmp);
        }
    }

    /* The separate NPK Node1 */
    {
        uint16_t n1Sec = n1_everSeen
            ? (uint16_t)min((now - n1_lastSeen) / 1000UL, 0xFFFEUL) : 0xFFFF;
        oledFmtAgo(tmp, sizeof(tmp), n1Sec);
        oled.setCursor(0, OLED_ROWN1_Y);
        oled.print("N1: ");
        oled.print(tmp);
    }

    oled.drawFastHLine(0, OLED_ROW3_Y - 2, OLED_W, SSD1306_WHITE);

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

/* ================================================================
 *  FLUSH +CMQTTPUB URC
 * ================================================================ */
static void flushCmqttPub() {
    uint32_t deadline = millis() + 150;
    String   buf = "";
    while (millis() < deadline) {
        while (SerialAT.available()) buf += (char)SerialAT.read();
        if (buf.indexOf("+CMQTTPUB") >= 0) break;
        vTaskDelay(pdMS_TO_TICKS(5));
    }
    if (buf.length())
        Serial.printf("URC: %s\n", buf.c_str());
}

/* ================================================================
 *  BUILD DEVICE JSON — one flat object per device, matching your
 *  platform's existing schema. Called once per incoming reading,
 *  so devices publish one at a time as their data arrives — not
 *  batched together. v8: now also carries TDSV/SoilV (raw pin
 *  voltages) straight through from the node firmware's payload.
 * ================================================================ */
static uint16_t buildDeviceJson(char* out, size_t outSize, int idx) {
    DynamicJsonDocument doc(PUB_JSON_DOC_SIZE);
    const CloudIdentity &cid = cloudIds[idx];
    NodeState &n = nodeTable[idx];

    doc["FId"] = cid.FId;
    doc["Id"]  = cid.Id;
    doc["UId"] = SHARED_UID;
    doc["BId"] = cid.BId;
    doc["DId"] = cid.DId;

    doc["CO2"] = n.CO2;
    doc["WS"]  = 0;          /* wind speed — not measured by this fleet */
    doc["par"] = n.PAR;
    doc["CT"]  = n.CT;
    doc["CH"]  = n.CH;
    doc["N"]   = 0;          /* soil nutrients — not measured by this fleet */
    doc["P"]   = 0;
    doc["K"]   = 0;
    doc["ST"]  = n.ST;
    doc["SM"]  = n.SM;
    doc["SoilV"] = n.SoilV;  /* raw soil-pin voltage, v7.2/v5.1 nodes only */
    doc["EC"]  = n.TDS;      /* TDS probe is this fleet's EC measurement */
    doc["TDSV"] = n.TDSV;    /* raw TDS-pin voltage, v7.2/v5.1 nodes only */
    doc["Ph"]  = 0;          /* not measured by this fleet */
    doc["F1"]    = n.F1;
    doc["F2"]    = n.F2;
    doc["F3"]    = n.F3;
    doc["F4"]    = n.F4;
    doc["F5"]    = n.F5;
    doc["F6"]    = n.F6;
    doc["F7"]    = n.F7;
    doc["F8"]    = n.F8;
    doc["NIR"]   = n.NIR;
    doc["Clear"] = n.Clear;

    /* Battery (INA219) — nested "Bat", same as the reference gateway.
     * A stale board publishes the "unknown" markers, so the cloud can
     * tell "no recent data" apart from "cell flat at 0 V".          */
    JsonObject bat = doc.createNestedObject("Bat");
    bool live = isNodeActive(idx);
    bat["VB"]  = live ? n.VB  : 0;
    bat["IB"]  = live ? n.IB  : 0;
    bat["BP"]  = live ? n.BP  : -1;
    bat["CHG"] = live ? n.CHG : -2;

    /* Node1 (NPK device) — its own separate, latest cached reading,
     * embedded here same as before. Zeros if it's never reported or
     * has gone quiet past IDLE_TIMEOUT. */
    JsonObject node1 = doc.createNestedObject("Node1");
    bool n1ok = isNode1Active();
    node1["N"]  = n1ok ? n1_N  : 0;
    node1["P"]  = n1ok ? n1_P  : 0;
    node1["K"]  = n1ok ? n1_K  : 0;
    node1["ST"] = n1ok ? n1_ST : 0;
    node1["SM"] = n1ok ? n1_SM : 0;
    node1["EC"] = n1ok ? n1_EC : 0;

    size_t len = serializeJson(doc, out, outSize);
    if (doc.overflowed() || len >= outSize - 1)
        Serial.printf("[!] Publish JSON near/over buffer: %u of %u bytes\n",
                      (unsigned)len, (unsigned)outSize);
    return (uint16_t)len;
}

static uint16_t buildIdleJson(char* out, size_t outSize) {
    StaticJsonDocument<300> doc;
    doc["type"] = "idle";
    doc["GatewayUId"] = DEVICE_ID;
    JsonArray active = doc.createNestedArray("ActiveNodes");
    for (int i = 0; i < NODE_COUNT; i++)
        if (isNodeActive(i)) active.add(cloudIds[i].nodeId);
    return (uint16_t)serializeJson(doc, out, outSize);
}

static void enqueuePublishTo(const char* topic, const char* payload, bool urgent) {
    MqttPubReq req;
    strlcpy(req.topic,   topic,   sizeof(req.topic));
    strlcpy(req.payload, payload, sizeof(req.payload));
    /* urgent = front of queue (the user's "is it active?" click) */
    BaseType_t ok = urgent
        ? xQueueSendToFront(mqttPubQueue, &req, pdMS_TO_TICKS(100))
        : xQueueSend       (mqttPubQueue, &req, pdMS_TO_TICKS(100));
    if (ok != pdTRUE) {
        diagPubFail++;
        Serial.printf("mqttPubQueue full — dropped (%s)\n", topic);
    }
}

/* Default: sensor data topic — all existing call sites unchanged */
static void enqueuePublish(const char* payload) {
    enqueuePublishTo(publish_topic, payload, false);
}

/* ================================================================
 *  GSM signal helpers
 * ================================================================ */
static int csqToDbm(int csq) {
    if (csq < 0 || csq == 99) return 0;     /* 0 = unknown */
    return -113 + 2 * csq;
}

static uint8_t csqToBars(int q) {
    if      (q == 99 || q < 0) return 0;
    else if (q >= 20)          return 4;
    else if (q >= 14)          return 3;
    else if (q >= 8)           return 2;
    else if (q >= 2)           return 1;
    return 0;
}

/* Sends AT+CSQ — call ONLY from mqttTask, never from mqttCallback */
static void pollSignal() {
    int q = modem.getSignalQuality();
    simCsq        = q;
    simSignalBars = csqToBars(q);
}

/* ================================================================
 *  BUILD HEALTH / ACTIVE JSON (device/health, device/active)
 * ================================================================ */
static uint16_t buildHealthJson(char* out, size_t outSize, const char* type) {
    StaticJsonDocument<1024> doc;
    unsigned long now = millis();

    doc["type"]   = type;               /* "health" or "active" */
    doc["UId"]    = DEVICE_ID;
    doc["status"] = "active";

    JsonObject gsm = doc.createNestedObject("gsm");
    int csq = simCsq;
    gsm["csq"]      = csq;
    gsm["rssi_dbm"] = csqToDbm(csq);
    gsm["bars"]     = simSignalBars;

    JsonObject lora = doc.createNestedObject("lora");
    lora["last_rssi"] = lastLoraRssi;

    int activeCount = 0;
    JsonArray nodes = lora.createNestedArray("nodes");
    if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
        for (int i = 0; i < NODE_COUNT; i++) {
            char idHex[6];
            snprintf(idHex, sizeof(idHex), "0x%02X", cloudIds[i].nodeId);
            JsonObject o = nodes.createNestedObject();
            o["id"] = idHex;                          /* copied by ArduinoJson */
            bool act = isNodeActive(i);
            if (act) activeCount++;
            o["active"] = act;
            if (nodeTable[i].everSeen) {
                o["rssi"]  = nodeTable[i].lastRssi;
                o["age_s"] = (now - nodeTable[i].lastSeen) / 1000UL;
            }
        }
        if (n1_everSeen) {
            lora["n1_rssi"]  = n1_lastRssi;
            lora["n1_age_s"] = (now - n1_lastSeen) / 1000UL;
        }
        xSemaphoreGive(dataMutex);
    }

    doc["NodesActive"] = activeCount;
    doc["Node1"]       = isNode1Active() ? "active" : "not received";
    doc["mqtt"]        = mqttOnline ? "online" : "offline";
    doc["uptime_s"]    = now / 1000UL;
    doc["free_heap"]   = esp_get_free_heap_size();

    size_t len = serializeJson(doc, out, outSize);
    if (doc.overflowed() || len >= outSize - 1)
        Serial.printf("[!] Health JSON near/over buffer: %u of %u bytes\n",
                      (unsigned)len, (unsigned)outSize);
    return (uint16_t)len;
}

/* ================================================================
 *  OTA — publish an ota_ack / ota_no_ack up to AWS
 * ================================================================ */
static void publishOtaAck(bool success, const char* target, const char* detail) {
    StaticJsonDocument<256> doc;
    doc["type"]   = success ? "ota_ack" : "ota_no_ack";
    doc["UId"]    = DEVICE_ID;
    doc["target"] = target;
    if (detail && strlen(detail)) doc["detail"] = detail;
    char buf[256];
    serializeJson(doc, buf, sizeof(buf));
    enqueuePublish(buf);
    Serial.printf("OTA ack -> AWS: %s\n", buf);
}

/* ================================================================
 *  LoRa TX — queued; loraTask is the only task that touches the
 *  radio. Frame: [dest][GATEWAY_ID][data...] (Node1's old format).
 * ================================================================ */
static void loraSend(uint8_t dest, const uint8_t* data, uint16_t len) {
    LoRaTxReq req;
    req.dest = dest;
    if (len > sizeof(req.data)) len = sizeof(req.data);
    memcpy(req.data, data, len);
    req.len = len;
    if (xQueueSend(loraTxQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE)
        Serial.println("[!] loraTxQueue full — TX dropped");
}

/* ================================================================
 *  OTA — arm the ack-wait / retry for a LoRa node OTA job
 * ================================================================ */
static void armNodeOtaRetry(uint8_t dest, const char* label,
                            const char* jsonBuf, uint16_t len) {
    if (len > sizeof(nodeOta.payload)) len = sizeof(nodeOta.payload);
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

/* ================================================================
 *  Self-OTA over WiFi — runs on otaTask, never on mqttTask
 * ================================================================ */
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

/* ================================================================
 *  otaTask (Core 1, pri 1 — lowest, never blocks LoRa/MQTT)
 * ================================================================ */
void otaTask(void* param) {
    for (;;) {
        SelfOtaJob job;
        if (xQueueReceive(otaJobQueue, &job, portMAX_DELAY) == pdTRUE) {
            Serial.println("otaTask: starting self-OTA job");
            selfOtaUpdate(job.ssid, job.pass, job.url);
        }
    }
}

/* ================================================================
 *  LORA TASK  (Core 0, pri 3) — v8: Node2 header is now just
 *  origin,ttl,seq (4 bytes) — no fid/bid/uid to parse.
 * ================================================================ */
void reinitLoRaRadio() {
    LoRa.end();
    pinMode(LORA_RST, OUTPUT);
    digitalWrite(LORA_RST, LOW);  delay(10);
    digitalWrite(LORA_RST, HIGH); delay(10);
    if (!LoRa.begin(LORA_FREQ)) { Serial.println("LoRa reinit failed"); return; }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    LoRa.receive();
    diagRadioReinits++;
    Serial.println("LoRa reinitialised");
}

void loraTask(void* param) {
    LoRa.receive();
    Serial.println("LoRa task — RX mode");
    uint32_t lastWatchdog = millis();

    for (;;) {
        int packetSize = LoRa.parsePacket();
        if (packetSize >= 2) {
            /* First byte tells us which format this is:
             * Node1 (old firmware) always sends its first byte as
             * GATEWAY_ID — it addresses the gateway directly, point
             * to point. Node2 boards never send GATEWAY_ID first;
             * their first byte is their own NODE_ID, since they
             * broadcast openly rather than addressing anyone. */
            uint8_t b0 = LoRa.read();
            LoRaRxPacket rx;

            if (b0 == GATEWAY_ID) {
                /* ---- Node1: old simple format (dest, src) ---- */
                if (packetSize < 2) { LoRa.receive(); continue; }
                rx.isNode1 = true;
                rx.origin  = LoRa.read();   /* src — should be NODE1_ID */
                rx.ttl = 0; rx.seq = 0;

                int bodyLen = packetSize - 2;
                if (bodyLen > (int)(sizeof(rx.buf)-1)) bodyLen = sizeof(rx.buf)-1;
                rx.len = (uint16_t)LoRa.readBytes(rx.buf, bodyLen);
            } else {
                /* ---- Node2 boards: v7.2/v5.1 4-byte broadcast header ---- */
                if (packetSize < PKT_HDR_LEN) { LoRa.receive(); continue; }
                rx.isNode1 = false;
                rx.origin  = b0;
                rx.ttl     = LoRa.read();
                uint8_t sl = LoRa.read();
                uint8_t sh = LoRa.read();
                rx.seq     = (uint16_t)(sl | (sh << 8));

                int bodyLen = packetSize - PKT_HDR_LEN;
                if (bodyLen > (int)(sizeof(rx.buf)-1)) bodyLen = sizeof(rx.buf)-1;
                rx.len = (uint16_t)LoRa.readBytes(rx.buf, bodyLen);
            }
            rx.buf[rx.len] = '\0';
            rx.rssi = LoRa.packetRssi();
            rx.snr  = LoRa.packetSnr();
            lastLoraRssi = rx.rssi;
            lastWatchdog = millis();
            diagRxTotal++;

            if (xQueueSend(rxQueue, &rx, 0) != pdTRUE)
                Serial.printf("rxQueue full — dropped origin 0x%02X\n", rx.origin);
            LoRa.receive();
        }

        /* Outgoing frames (OTA to Node1) — sent from this task only */
        LoRaTxReq tx;
        if (xQueueReceive(loraTxQueue, &tx, 0) == pdTRUE) {
            LoRa.beginPacket();
            LoRa.write(tx.dest);
            LoRa.write(GATEWAY_ID);
            LoRa.write(tx.data, tx.len);
            LoRa.endPacket();
            LoRa.receive();
            Serial.printf("LoRa TX -> 0x%02X (%u bytes)\n", tx.dest, tx.len);
        }

        if ((millis() - lastWatchdog) >= LORA_WATCHDOG_MS) {
            Serial.println("LoRa watchdog — reinitialising");
            reinitLoRaRadio();
            lastWatchdog = millis();
        }

        vTaskDelay(pdMS_TO_TICKS(5));
    }
}

/* ================================================================
 *  MQTT
 * ================================================================ */
/* Runs ON mqttTask — keep it fast: self-OTA is queued to otaTask,
 * node OTA is queued to loraTask. Nothing blocks here.            */
void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len) {
    char buf[512];
    uint32_t n = (len < sizeof(buf)-1) ? len : sizeof(buf)-1;
    memcpy(buf, payload, n); buf[n] = '\0';
    Serial.printf("MQTT IN [%s]: %s\n", topic, buf);

    /* ---- device/cmd : "is it active?" button from the app ---- */
    if (strcmp(topic, cmd_topic) == 0) {
        StaticJsonDocument<256> cmd;
        if (deserializeJson(cmd, buf) != DeserializationError::Ok) {
            Serial.println("[!] device/cmd JSON parse error");
            return;
        }
        const char* uid = cmd["UId"] | "";
        if (strcmp(uid, DEVICE_ID) != 0) return;          /* not for us */

        const char* c = cmd["cmd"] | "";
        if (strcasecmp(c, "check_active") == 0 ||
            strcasecmp(c, "active") == 0) {
            activeCheckRequested = true;   /* mqttTask replies on device/active */
            Serial.println("Active-check requested");
        }
        return;
    }

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

    const char* deviceType = doc["device"]    | "";
    const char* url        = doc["url"]       | "";
    const char* ssid       = doc["wifi_ssid"] | "";
    const char* pass       = doc["wifi_pass"] | "";

    if (!strlen(url)) {
        Serial.println("[!] OTA: missing url");
        return;
    }

    /* ---- Self-OTA of this gateway ---- */
    if (strcasecmp(deviceType, "sensor gateway") == 0 ||
        strcasecmp(deviceType, "sensorgateway")  == 0 ||
        strcasecmp(deviceType, "sensor node gateway") == 0) {
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

    /* ---- Relay OTA to a LoRa node ---- */
    if (strcasecmp(deviceType, "sensor node") == 0) {
        if (nodeOta.active) {
            Serial.println("[!] Node OTA already in progress — ignoring new request");
            publishOtaAck(false, "sensor node", "node OTA already in progress");
            return;
        }
        int nodeNum = doc["NodeNum"] | 0;
        if (nodeNum != 1) {
            /* The STM32 AgriNode boards have no OTA receiver: sending
             * them an OTA frame would only be relayed around the mesh
             * as junk. Only the NPK Node1 accepts LoRa OTA.          */
            Serial.printf("[!] OTA: NodeNum %d not supported (only 1 = NPK Node1)\n", nodeNum);
            publishOtaAck(false, "sensor node",
                          "NodeNum not supported - only 1 (NPK Node1) has LoRa OTA");
            return;
        }

        StaticJsonDocument<256> pkt;
        pkt["type"] = "ota";
        pkt["url"]  = url;
        if (strlen(ssid)) pkt["wifi"] = ssid;
        if (strlen(pass)) pkt["pass"] = pass;

        char pbuf[220];
        size_t plen = serializeJson(pkt, pbuf, sizeof(pbuf));
        if (plen == 0 || plen >= sizeof(pbuf) - 1) {
            Serial.println("[X] OTA packet too large for LoRa payload — shorten the url");
            publishOtaAck(false, "Node1", "OTA packet too large - shorten url");
            return;
        }

        loraSend(NODE1_ID, (const uint8_t*)pbuf, (uint16_t)plen);
        Serial.printf("[OTA attempt 1/3] -> Node1 (0x%02X)\n", NODE1_ID);
        armNodeOtaRetry(NODE1_ID, "Node1", pbuf, (uint16_t)plen);
        return;
    }

    Serial.printf("[skip] OTA: unrecognised device type '%s'\n", deviceType);
}

bool mqttConnectOnce() {
    modem.mqtt_set_certificate(root_ca, certificate, privateKey);
    if (!modem.mqtt_connect(0, broker, broker_port, client_id, NULL, NULL, 120)) return false;
    modem.mqtt_set_callback(mqttCallback);
    modem.mqtt_subscribe(0, subscribe_topic, 0);
    if (strcmp(subscribe_topic, cmd_topic) != 0)      /* don't subscribe twice */
        modem.mqtt_subscribe(0, cmd_topic, 0);
    modem.mqtt_subscribe(0, "OTA/mode", 1);
    return true;
}

void clearError() {}

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
    Serial.println("Network registered");

    while (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
        Serial.println("GPRS retry...");
        vTaskDelay(pdMS_TO_TICKS(5000));
    }
    Serial.println("GPRS | IP: " + modem.localIP().toString());

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
    uint32_t lastHealthMs  = 0;
    bool     healthDue     = false;   /* set on (re)connect -> send one right away */

    for (;;) {
        uint32_t now = millis();

        if (now - lastHandleMs >= 10) {
            lastHandleMs = now;
            modem.mqtt_handle();
        }

        if (state == CONNECTED && now - lastSigPoll >= 30000UL) {
            lastSigPoll = now;
            pollSignal();
        }

        /* ---- device/active : user clicked "check active" ---- */
        if (state == CONNECTED && activeCheckRequested) {
            activeCheckRequested = false;
            pollSignal();                       /* fresh CSQ */
            lastSigPoll = millis();
            char hb[PUB_PAYLOAD_SIZE];
            buildHealthJson(hb, sizeof(hb), "active");
            enqueuePublishTo(active_topic, hb, true);   /* front of queue */
            Serial.printf("Active reply enqueued: %s\n", hb);
        }

        /* ---- device/health : every 15 min + once after (re)connect ---- */
        if (state == CONNECTED &&
            (healthDue || now - lastHealthMs >= HEALTH_INTERVAL_MS)) {
            healthDue    = false;
            lastHealthMs = now;
            pollSignal();
            lastSigPoll = millis();
            char hb[PUB_PAYLOAD_SIZE];
            buildHealthJson(hb, sizeof(hb), "health");
            enqueuePublishTo(health_topic, hb, false);
            Serial.printf("Health enqueued: %s\n", hb);
        }

        if (state == CONNECTED) {
            MqttPubReq req;
            if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
                if (modem.mqtt_publish(0, req.topic, req.payload, 0)) {
                    diagPubOk++;
                    failCount     = 0;
                    lastPublishMs = millis();
                    Serial.printf("Published [%s]: %s\n", req.topic, req.payload);
                    flushCmqttPub();
                } else {
                    diagPubFail++;
                    Serial.println("Publish FAILED — re-queuing");
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
                Serial.println("MQTT connected");
            } else {
                mqttOnline = false;
                Serial.printf("MQTT failed — retry in %lu s\n", retryDelay/1000);
                nextRetry  = now + retryDelay;
                retryDelay = min(retryDelay * 2, (uint32_t)MQTT_RETRY_MAX_MS);
            }
            break;

        case CONNECTED:
            if (!modem.mqtt_connected()) {
                Serial.println("MQTT dropped — reconnecting");
                mqttOnline = false;
                notified   = false;
                state      = CONNECTING;
                nextRetry  = now + retryDelay;
                break;
            }
            if (!notified) {
                notified  = true;
                healthDue = true;     /* report health as soon as we're back online */
                clearError();
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

/* ================================================================
 *  APP TASK  (Core 1, pri 2) — parses LoRa RX into the node table,
 *  now keyed by NODE_ID instead of FId.
 * ================================================================ */
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
                Serial.printf("Invalid packet origin 0x%02X\n", pkt.origin);
                continue;
            }
            if (js > 0) payload = payload.substring(js);

            DynamicJsonDocument tmp(RX_JSON_DOC_SIZE);
            if (deserializeJson(tmp, payload) != DeserializationError::Ok) {
                diagRxBad++;
                Serial.printf("JSON parse fail origin 0x%02X\n", pkt.origin);
                continue;
            }

            /* ---- Node1: the separate NPK device ----
             * Just cache its latest reading. It gets embedded as the
             * "Node1" nested object the next time any Node2 device
             * publishes. No publish is triggered here by itself. */
            /* ---- ota_ack from the node being updated (Node1 format) ---- */
            const char* type = tmp["type"] | "";
            if (strcmp(type, "ota_ack") == 0) {
                if (pkt.isNode1 && nodeOta.active && pkt.origin == nodeOta.destNode) {
                    const char* status = tmp["status"] | "fail";
                    bool ok = (strcasecmp(status, "fail") != 0);
                    Serial.printf("OTA ack from %s — status:%s\n", nodeOta.nodeLabel, status);
                    publishOtaAck(ok, nodeOta.nodeLabel, status);
                    nodeOta.active = false;
                } else {
                    Serial.printf("[skip] stale/unexpected ota_ack from 0x%02X\n", pkt.origin);
                }
                continue;
            }

            if (pkt.isNode1) {
                if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                    n1_N  = tmp["N"]  | 0.0f;
                    n1_P  = tmp["P"]  | 0.0f;
                    n1_K  = tmp["K"]  | 0.0f;
                    n1_ST = tmp["ST"] | 0.0f;
                    n1_SM = tmp["SM"] | 0.0f;
                    n1_EC = tmp["EC"] | 0.0f;
                    n1_everSeen = true;
                    n1_lastSeen = millis();
                    n1_lastRssi = pkt.rssi;
                    xSemaphoreGive(dataMutex);
                    Serial.printf("\nNode1 (NPK) -> N=%.0f P=%.0f K=%.0f ST=%.1f SM=%.0f EC=%.1f\n",
                                  n1_N, n1_P, n1_K, n1_ST, n1_SM, n1_EC);
                } else {
                    Serial.println("dataMutex timeout (Node1)");
                }
                continue;   /* nothing more to do — no fixed slot to publish under */
            }

            /* ---- Node2 boards: identity is the NODE_ID itself now ---- */
            int idx = nodeIndex(pkt.origin);
            if (idx < 0) {
                Serial.printf("Unknown NODE_ID 0x%02X — not in cloudIds[], add it if this is a real board\n", pkt.origin);
                continue;
            }

            Serial.printf("\nLoRa NODE_ID 0x%02X  RSSI:%d  SNR:%.1f  TTL:%u Seq:%u\n",
                          pkt.origin, pkt.rssi, pkt.snr, pkt.ttl, pkt.seq);

            if (xSemaphoreTake(dataMutex, pdMS_TO_TICKS(50)) == pdTRUE) {
                NodeState &n = nodeTable[idx];

                /* Skip exact duplicates (e.g. a packet relayed by two
                 * different boards that both heard the same leaf). */
                bool isDuplicate = n.everSeen && n.lastSeq == pkt.seq;

                if (!isDuplicate) {
                    n.everSeen = true;
                    n.nodeId   = pkt.origin;
                    n.lastSeq  = pkt.seq;
                    n.lastSeen = millis();
                    n.lastRssi = pkt.rssi;   /* first copy only — a relayed duplicate's
                                                RSSI is the relay's, not this board's */

                    n.CO2   = tmp["CO2"]   | 0.0f;
                    n.CT    = tmp["CT"]    | 0.0f;
                    n.CH    = tmp["CH"]    | 0.0f;
                    n.ST    = tmp["ST"]    | 0.0f;
                    n.SM    = tmp["SM"]    | 0.0f;
                    n.SoilV = tmp["SoilV"] | 0.0f;
                    n.TDS   = tmp["TDS"]   | 0.0f;
                    n.TDSV  = tmp["TDSV"]  | 0.0f;
                    n.PAR   = tmp["PAR"]   | 0.0f;
                    if (n.SM  < 0) n.SM  = 0;
                    if (n.TDS < 0) n.TDS = 0;
                    n.F1    = tmp["F1"]    | 0.0f;
                    n.F2    = tmp["F2"]    | 0.0f;
                    n.F3    = tmp["F3"]    | 0.0f;
                    n.F4    = tmp["F4"]    | 0.0f;
                    n.F5    = tmp["F5"]    | 0.0f;
                    n.F6    = tmp["F6"]    | 0.0f;
                    n.F7    = tmp["F7"]    | 0.0f;
                    n.F8    = tmp["F8"]    | 0.0f;
                    n.NIR   = tmp["NIR"]   | 0.0f;
                    n.Clear = tmp["Clear"] | 0.0f;

                    /* Battery (INA219). Defaults mean "unknown" so a
                     * board without an INA219 never shows a fake 0 %. */
                    n.VB    = tmp["VB"]    | 0.0f;
                    n.IB    = tmp["IB"]    | 0.0f;
                    n.BP    = tmp["BP"]    | -1;
                    n.CHG   = tmp["CHG"]   | -2;
                    Serial.printf("         Batt VB=%.2fV IB=%+.0fmA BP=%d%% %s\n",
                                  n.VB, n.IB, n.BP, chgLabel(n.CHG));
                }

                xSemaphoreGive(dataMutex);

                if (!isDuplicate) {
                    char buf[PUB_PAYLOAD_SIZE];
                    buildDeviceJson(buf, sizeof(buf), idx);
                    if (mqttOnline) {
                        enqueuePublish(buf);
                    } else {
                        strlcpy(pendingBuf, buf, sizeof(pendingBuf));
                        pendingData = true;
                        Serial.println("MQTT offline — buffered for retry");
                    }
                    idleSent = false;
                    oledRefresh();
                }
            } else {
                Serial.println("dataMutex timeout");
            }
        }

        if (now - lastOledMs >= 1000UL) {
            lastOledMs = now;
            oledRefresh();
        }

        if (now - lastIdleCheck >= 10000UL) {
            lastIdleCheck = now;
            if (!anyNodeActive() && !idleSent && mqttOnline) {
                idleSent = true;
                diagIdleSent++;
                char buf[300];
                buildIdleJson(buf, sizeof(buf));
                enqueuePublish(buf);
                Serial.println("Idle enqueued");
            }
        }

        /* ---- LoRa node OTA retry ticker ---- */
        if (nodeOta.active && (int32_t)(now - nodeOta.nextRetryMs) >= 0) {
            if (nodeOta.retryCount < OTA_MAX_RETRIES) {
                nodeOta.retryCount++;
                Serial.printf("[OTA attempt %d/3] retry -> %s\n",
                              nodeOta.retryCount + 1, nodeOta.nodeLabel);
                loraSend(nodeOta.destNode, (const uint8_t*)nodeOta.payload,
                         nodeOta.payloadLen);
                nodeOta.nextRetryMs = now + OTA_RETRY_MS;
            } else {
                Serial.printf("[X] OTA — all 3 attempts exhausted for %s\n",
                              nodeOta.nodeLabel);
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
            Serial.printf("  Node1 (NPK) active: %s (N:%.0f P:%.0f K:%.0f ST:%.1f SM:%.0f EC:%.1f)\n",
                          isNode1Active() ? "YES" : "NO",
                          n1_N, n1_P, n1_K, n1_ST, n1_SM, n1_EC);
            for (int i = 0; i < NODE_COUNT; i++) {
                Serial.printf("  NODE_ID 0x%02X active: %s  Batt %.2fV %+.0fmA %d%% %s\n",
                              cloudIds[i].nodeId, isNodeActive(i) ? "YES" : "NO",
                              nodeTable[i].VB, nodeTable[i].IB,
                              nodeTable[i].BP, chgLabel(nodeTable[i].CHG));
            }
            Serial.printf("  MQTT online:   %s\n",   mqttOnline?"YES":"NO");
            if (nodeOta.active)
                Serial.printf("  OTA pending:   %s attempt %d/3\n",
                              nodeOta.nodeLabel, nodeOta.retryCount + 1);
            Serial.printf("  Free heap:     %u B\n", esp_get_free_heap_size());
            Serial.println("==========================\n");
        }

        vTaskDelay(pdMS_TO_TICKS(10));
    }
}

/* ================================================================
 *  SETUP
 * ================================================================ */
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
        Serial.println("OLED ready");
    } else {
        Serial.println("OLED not found — continuing without display");
    }

    dataMutex    = xSemaphoreCreateMutex();
    rxQueue      = xQueueCreate(RX_QUEUE_DEPTH,       sizeof(LoRaRxPacket));
    mqttPubQueue = xQueueCreate(MQTT_PUB_QUEUE_DEPTH, sizeof(MqttPubReq));
    loraTxQueue  = xQueueCreate(LORA_TX_QUEUE_DEPTH,  sizeof(LoRaTxReq));
    otaJobQueue  = xQueueCreate(1,                    sizeof(SelfOtaJob));

    if (!dataMutex || !rxQueue || !mqttPubQueue || !loraTxQueue || !otaJobQueue) {
        Serial.println("Resource init failed — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }

    LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
    if (!LoRa.begin(LORA_FREQ)) {
        Serial.println("LoRa init FAILED — halting");
        while (true) vTaskDelay(pdMS_TO_TICKS(1000));
    }
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.setTxPower(LORA_TXPOWER, PA_OUTPUT_PA_BOOST_PIN);
    LoRa.enableCrc();
    Serial.println("LoRa ready — SF7 BW125 CR5 0xAB, 4 nodes (v7.2/v5.1 format)");

    xTaskCreatePinnedToCore(loraTask, "LoRa", 4096,  NULL, 3, &loraTaskHandle, 0);
    /* 12 KB: health/active JSON + 768-byte publish buffers live on this stack */
    xTaskCreatePinnedToCore(mqttTask, "MQTT", 12288, NULL, 3, &mqttTaskHandle, 1);
    xTaskCreatePinnedToCore(appTask,  "App",  6144,  NULL, 2, &appTaskHandle,  1);
    xTaskCreatePinnedToCore(otaTask,  "OTA",  8192,  NULL, 1, &otaTaskHandle,  1);

    Serial.println("Sensor gateway started (4 nodes, battery + OTA)");
    vTaskSuspend(NULL);
}

void loop() { vTaskDelay(portMAX_DELAY); }
