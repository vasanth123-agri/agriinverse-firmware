// ================================================================
//  Agri Inverse — Fertigation Gateway (GSM-only, AWS IoT Core)  v1.2 (OTA)
//  Bridges: Node <--LoRa--> Gateway <--GSM/MQTT/TLS--> AWS IoT Core
//
//  Topics:
//    fert/outdoor  <- gateway publishes   (periodic sensor data, power_on)
//    fert/ack      <- gateway publishes   (command acks, cal/tune/roles results,
//                                           state-change/alert messages, and a
//                                           ONE-TIME initial_setpoint_request if
//                                           this gateway has never been set up)
//    fert/cmd      -> gateway subscribes  (commands from AWS)
//    OTA/mode      -> gateway subscribes  (OTA commands from AWS — NEW v1.1)
//
//  All payloads pass through close to verbatim: the node already
//  builds identity-tagged JSON (device/UId/FId/BId), the gateway's
//  job is LoRa RX/TX plus GSM/MQTT transport — not payload construction,
//  except for fert/cmd -> LoRa cmd forwarding.
//
//  v1.1 — OTA support:
//    device == "Fertigation gateway" -> self-OTA, THIS board, over WiFi
//      (separate radio from the GSM link used for MQTT).
//    device == "Fertigation"         -> relayed to the node over LoRa,
//      using this file's own envelope convention:
//      {"to":FERT_UNIT_ID,"from":GW_DEVICE_ID,"type":"ota",
//       "url":"...","wifi":"...","pass":"..."}
//      (NOT the Did/uid-style envelope used by the other fertigation
//      gateway variant — this node's protocol uses to/from throughout,
//      so OTA follows that same convention for consistency.)
//    Self-OTA download runs on a dedicated otaTask (lowest priority),
//    never on mqttTask, so it can never stall modem.mqtt_handle()
//    (which must be pumped every 10ms or the AT-command MQTT session
//    drops) — same fix applied across every other gateway in this repo.
//
//  v1.2 — fert/cmd forwarding fixes (mqttCallback only):
//    - Rejects commands whose uid/UId does not match GW_DEVICE_ID, before
//      the setpoint cache or setup state can be changed.
//    - Replaces cloud identity fields (Did/uid/fid/bid/FId/BId) with the
//      device/UId fields the node checks.
//    - Converts {"type":"cmd","cmd":"stop"} to {"type":"stop"}, because
//      the running node rejects type "cmd" as unknown.
//    - Uses the converted command name for retry tracking.
// ================================================================

// ================================================================
//  0. MODEM DEFINE — MUST be first, before TinyGsmClient.h
// ================================================================
#define TINY_GSM_MODEM_A7670
#define SerialMon      Serial
#define SerialAT       Serial1
#define TINY_GSM_DEBUG SerialMon
#define GSM_PIN        ""

// FIX: TinyGSM's OWN internal stream/URC parsing buffer is separate
// from the ESP32 UART ring buffer (which was already fixed via
// SerialAT.setRxBufferSize()). TINY_GSM_RX_BUFFER controls the size
// TinyGSM itself allocates for reading AT responses/URCs — including
// the +CMQTTRXPAYLOAD content that carries your actual JSON command.
// Left undefined, it defaults to a small value (commonly 64 bytes in
// many TinyGSM versions), silently truncating anything larger BEFORE
// it ever reaches your own buf[]/StaticJsonDocument, regardless of
// how big those are. MUST be defined before the #include below.
#define TINY_GSM_RX_BUFFER 1024

#include <Arduino.h>
#include <SPI.h>
#include <LoRa.h>
#include <ArduinoJson.h>
#include <TinyGsmClient.h>
#include <Preferences.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

// AWS IoT TLS certificates — same include pattern as the reference
// gateway (v1.3): each header defines a single const char* array.
#include "certificate.h"   // const char* certificate  (device cert, PEM)
#include "privatekey.h"    // const char* privateKey   (device private key, PEM)
#include "root_ca.h"       // const char* root_ca      (Amazon Root CA 1, PEM)

// ================================================================
//  ALL STRUCT/TYPE DEFINITIONS — placed immediately after includes.
//  Arduino IDE auto-generates function prototypes near the top of
//  the concatenated sketch; any struct used in a function signature
//  must be fully defined before that point or the auto-prototype
//  fails with "does not name a type". Keeping every struct together
//  here, before all function definitions, avoids that entirely.
// ================================================================
struct NodeIdentity { char device[16]; char UId[8]; char FId[40]; char BId[40]; bool valid; };
struct LoRaRxPacket { char buf[768]; uint16_t len; int rssi; };
struct LoraTxPacket { char payload[768]; };
struct RingMsg { char topic[16]; char payload[512]; bool used; };
struct MqttPubReq { char topic[32]; char payload[640]; };
struct PendingCmd { char payload[768]; char cmdName[32]; uint8_t retries; uint32_t nextTryMs; bool active; };

// Job handed from mqttTask -> otaTask so a slow/blocking self-OTA
// download never stalls modem.mqtt_handle() (must be pumped every
// 10ms or MQTT drops).
struct SelfOtaJob { char ssid[64]; char pass[64]; char url[220]; };

// ================================================================
//  IDENTITY
// ================================================================
#define GW_DEVICE_ID  "66"
#define FERT_UNIT_ID  "FId1"    // must match node's MY_DEVICE

// ================================================================
//  GSM MODEM PINS (A7670)
// ================================================================
#define MODEM_RX     16
#define MODEM_TX     17
#define MODEM_PWKEY   4

TinyGsm modem(SerialAT);

// ================================================================
//  APN / MQTT BROKER — fill in your actual values
// ================================================================
const char apn[]        = "iot.com";
const char gprsUser[]   = "";
const char gprsPass[]   = "";
const char broker[]     = "a2nvakoqnh614u-ats.iot.us-east-1.amazonaws.com";
const int  broker_port  = 8883;
const char client_id[]  = "gateway_01";
#define MQTT_KEEPALIVE_SEC 60

// ================================================================
//  MQTT TOPICS
// ================================================================
#define TOPIC_OUTDOOR "fert/outdoor"
#define TOPIC_ACK     "fert/ack"
#define TOPIC_CMD     "fert/cmd"   // gateway subscribes
#define TOPIC_OTA     "OTA/mode"   // gateway subscribes — NEW v1.1

// ================================================================
//  MQTT RETRY / WATCHDOG TIMING (from reference)
// ================================================================
#define MQTT_RETRY_MIN_MS     10000UL
#define MQTT_RETRY_MAX_MS     60000UL
#define LTE_WATCHDOG_MS      (10UL * 60 * 1000)
#define MAX_MQTT_FAILURES     5
#define MQTT_NOTIFY_DEBOUNCE  5000UL

static volatile bool mqttOnline = false;
bool cloudOnline = false;   // alias kept for the rest of this file
bool powerOnSent = false;
static bool     lastNotifiedOnline = false;
static uint32_t lastMqttNotifyMs   = 0;

// ================================================================
//  INITIAL SETPOINT REQUEST — sent to AWS ONCE, ever, on this
//  gateway's very first successful boot/setup (persisted in NVS
//  so it survives reboots). After that single request, the
//  gateway never asks again — all further setpoint changes come
//  purely from the user pushing a "setpoint" command via fert/cmd.
// ================================================================
Preferences setupPrefs;
bool initialSetupDone = false;

void loadSetupState() {
  setupPrefs.begin("setup", true);
  initialSetupDone = setupPrefs.getBool("done", false);
  setupPrefs.end();
}
void markSetupDone() {
  if (initialSetupDone) return;
  initialSetupDone = true;
  setupPrefs.begin("setup", false);
  setupPrefs.putBool("done", true);
  setupPrefs.end();
}

// ================================================================
//  SETPOINT CACHE — last real setpoint received from AWS via
//  fert/cmd, kept so the gateway can answer the node's LoRa
//  "setpoint_req" IMMEDIATELY and LOCALLY, without a round trip to
//  AWS. Persisted to NVS so it survives a gateway reboot too.
// ================================================================
Preferences spPrefs;
struct GwSetpointCache {
  bool  valid;
  float ph, ec;
  float n1, n2, n3, n4, n5;
  char  roles[6];       // compact 5-digit string + null terminator
  char  tankstatus[2];
  char  valvestatus[2];
};
GwSetpointCache gwSetpoint = { false, 6.0f, 1.0f, 1,0,0,0,0, "12345", "1", "1" };

void loadGwSetpoint() {
  spPrefs.begin("gwsp", true);
  gwSetpoint.valid = spPrefs.getBool("valid", false);
  gwSetpoint.ph = spPrefs.getFloat("ph", 6.0f);
  gwSetpoint.ec = spPrefs.getFloat("ec", 1.0f);
  gwSetpoint.n1 = spPrefs.getFloat("n1", 1.0f);
  gwSetpoint.n2 = spPrefs.getFloat("n2", 0.0f);
  gwSetpoint.n3 = spPrefs.getFloat("n3", 0.0f);
  gwSetpoint.n4 = spPrefs.getFloat("n4", 0.0f);
  gwSetpoint.n5 = spPrefs.getFloat("n5", 0.0f);
  spPrefs.getString("roles", gwSetpoint.roles, sizeof(gwSetpoint.roles));
  spPrefs.getString("tk",    gwSetpoint.tankstatus,  sizeof(gwSetpoint.tankstatus));
  spPrefs.getString("vs",    gwSetpoint.valvestatus, sizeof(gwSetpoint.valvestatus));
  if (!gwSetpoint.roles[0])       strcpy(gwSetpoint.roles, "12345");
  if (!gwSetpoint.tankstatus[0])  strcpy(gwSetpoint.tankstatus, "1");
  if (!gwSetpoint.valvestatus[0]) strcpy(gwSetpoint.valvestatus, "1");
  spPrefs.end();
}
void saveGwSetpoint() {
  spPrefs.begin("gwsp", false);
  spPrefs.putBool("valid", true);
  spPrefs.putFloat("ph", gwSetpoint.ph); spPrefs.putFloat("ec", gwSetpoint.ec);
  spPrefs.putFloat("n1", gwSetpoint.n1); spPrefs.putFloat("n2", gwSetpoint.n2);
  spPrefs.putFloat("n3", gwSetpoint.n3); spPrefs.putFloat("n4", gwSetpoint.n4);
  spPrefs.putFloat("n5", gwSetpoint.n5);
  spPrefs.putString("roles", gwSetpoint.roles);
  spPrefs.putString("tk",    gwSetpoint.tankstatus);
  spPrefs.putString("vs",    gwSetpoint.valvestatus);
  spPrefs.end();
}

// Extracts and caches a setpoint's fields from an incoming fert/cmd
// JSON doc, whatever fields happen to be present (matches the node's
// own "only update what's provided" rule).
void cacheSetpointFromCmd(JsonDocument& doc) {
  GwSetpointCache fresh = { false, 6.0f, 1.0f, 1,0,0,0,0, "12345", "1", "1" };

  if (doc.containsKey("ph")) fresh.ph = doc["ph"];
  if (doc.containsKey("ec")) fresh.ec = doc["ec"];
  if (doc.containsKey("n1")) fresh.n1 = doc["n1"];
  if (doc.containsKey("n2")) fresh.n2 = doc["n2"];
  if (doc.containsKey("n3")) fresh.n3 = doc["n3"];
  if (doc.containsKey("n4")) fresh.n4 = doc["n4"];
  if (doc.containsKey("n5")) fresh.n5 = doc["n5"];
  if (doc.containsKey("roles"))       strlcpy(fresh.roles, doc["roles"] | "12345", sizeof(fresh.roles));
  if (doc.containsKey("tankstatus"))  strlcpy(fresh.tankstatus,  doc["tankstatus"]  | "1", sizeof(fresh.tankstatus));
  if (doc.containsKey("valvestatus")) strlcpy(fresh.valvestatus, doc["valvestatus"] | "1", sizeof(fresh.valvestatus));

  fresh.valid = true;
  gwSetpoint = fresh;   // erase previous entirely, replace with this
  saveGwSetpoint();
  Serial.println("[SETPOINT CACHE] previous value erased, new setpoint stored");
}

// ================================================================
//  CACHED NODE IDENTITY
// ================================================================
Preferences idPrefs;
NodeIdentity nodeId = { "", "", "", "", false };

void loadNodeIdentity() {
  idPrefs.begin("nodeid", true);
  idPrefs.getString("device", nodeId.device, sizeof(nodeId.device));
  idPrefs.getString("UId",    nodeId.UId,    sizeof(nodeId.UId));
  idPrefs.getString("FId",    nodeId.FId,    sizeof(nodeId.FId));
  idPrefs.getString("BId",    nodeId.BId,    sizeof(nodeId.BId));
  nodeId.valid = idPrefs.getBool("valid", false);
  idPrefs.end();
}
void saveNodeIdentity() {
  idPrefs.begin("nodeid", false);
  idPrefs.putString("device", nodeId.device);
  idPrefs.putString("UId",    nodeId.UId);
  idPrefs.putString("FId",    nodeId.FId);
  idPrefs.putString("BId",    nodeId.BId);
  idPrefs.putBool("valid", true);
  idPrefs.end();
}
void captureNodeIdentity(JsonDocument& doc) {
  const char* dev = doc["device"] | "";
  if (!dev[0]) return;

  bool changed = strcmp(dev, nodeId.device) != 0;
  if (doc.containsKey("UId") && strcmp(doc["UId"] | "", nodeId.UId) != 0) changed = true;
  if (doc.containsKey("FId") && strcmp(doc["FId"] | "", nodeId.FId) != 0) changed = true;
  if (doc.containsKey("BId") && strcmp(doc["BId"] | "", nodeId.BId) != 0) changed = true;

  if (changed || !nodeId.valid) {
    strlcpy(nodeId.device, dev, sizeof(nodeId.device));
    if (doc.containsKey("UId")) strlcpy(nodeId.UId, doc["UId"] | "", sizeof(nodeId.UId));
    if (doc.containsKey("FId")) strlcpy(nodeId.FId, doc["FId"] | "", sizeof(nodeId.FId));
    if (doc.containsKey("BId")) strlcpy(nodeId.BId, doc["BId"] | "", sizeof(nodeId.BId));
    nodeId.valid = true;
    saveNodeIdentity();
  }
}

// ================================================================
//  LoRa
// ================================================================
#define LORA_SS    5
#define LORA_RST   14
#define LORA_DIO0  26
#define LORA_FREQ  433E6
#define LORA_SF    9
#define LORA_BW    125E3
#define LORA_CR    5
#define LORA_SW    0xF3
#define LORA_PWR   17

// ================================================================
//  RETRY / QUEUES
// ================================================================
static QueueHandle_t rxQueue, txQueue, otaJobQueue;

#define RING_SIZE 20
RingMsg ringBuf[RING_SIZE];
uint8_t ringHead = 0;

void ringPush(const char* topic, const char* payload) {
  ringBuf[ringHead] = { "", "", true };
  strlcpy(ringBuf[ringHead].topic, topic, sizeof(ringBuf[ringHead].topic));
  strlcpy(ringBuf[ringHead].payload, payload, sizeof(ringBuf[ringHead].payload));
  ringHead = (ringHead + 1) % RING_SIZE;
}

static QueueHandle_t mqttPubQueue;

void enqueueMqttPublish(const char* topic, const char* payload) {
  MqttPubReq req;
  strlcpy(req.topic, topic, sizeof(req.topic));
  strlcpy(req.payload, payload, sizeof(req.payload));
  if (xQueueSend(mqttPubQueue, &req, pdMS_TO_TICKS(100)) != pdTRUE) {
    Serial.println("[MQTT] pub queue full — dropped");
  }
}

void ringFlush() {
  for (uint8_t i = 0; i < RING_SIZE; i++) {
    if (ringBuf[i].used) {
      enqueueMqttPublish(ringBuf[i].topic, ringBuf[i].payload);
      ringBuf[i].used = false;
    }
  }
}

void publishOrBuffer(const char* topic, const char* payload) {
  if (cloudOnline) { enqueueMqttPublish(topic, payload); return; }
  ringPush(topic, payload);
}

// ================================================================
//  COMMAND RETRY
// ================================================================
#define CMD_MAX_RETRIES 3
#define CMD_RETRY_MS    3000UL
PendingCmd pendingCmd = {"", "", 0, 0, false};

void loraTxWithRetry(const char* payload, const char* cmdName) {
  strlcpy(pendingCmd.payload, payload, sizeof(pendingCmd.payload));
  strlcpy(pendingCmd.cmdName, cmdName, sizeof(pendingCmd.cmdName));
  pendingCmd.retries   = CMD_MAX_RETRIES;
  pendingCmd.nextTryMs = millis() + CMD_RETRY_MS;
  pendingCmd.active    = true;

  LoraTxPacket tx; strlcpy(tx.payload, payload, sizeof(tx.payload));
  xQueueSend(txQueue, &tx, pdMS_TO_TICKS(50));
  Serial.printf("[RETRY] armed cmd='%s' retries=%d\n", cmdName, CMD_MAX_RETRIES);
}

void checkCmdRetry() {
  if (!pendingCmd.active) return;
  uint32_t now = millis();
  if ((int32_t)(now - pendingCmd.nextTryMs) < 0) return;

  if (pendingCmd.retries > 0) {
    pendingCmd.retries--;
    pendingCmd.nextTryMs = now + CMD_RETRY_MS;
    Serial.printf("[RETRY] attempt %d/%d cmd='%s'\n",
                  CMD_MAX_RETRIES - pendingCmd.retries, CMD_MAX_RETRIES, pendingCmd.cmdName);
    LoraTxPacket tx; strlcpy(tx.payload, pendingCmd.payload, sizeof(tx.payload));
    xQueueSend(txQueue, &tx, pdMS_TO_TICKS(50));
  } else {
    Serial.printf("[RETRY] FAILED — giving up on cmd='%s' after %d attempts\n",
                  pendingCmd.cmdName, CMD_MAX_RETRIES);
    pendingCmd.active = false;
  }
}

void clearPendingIfAcked(const char* type, JsonDocument& doc) {
  if (!pendingCmd.active) return;
  bool isMatch = false;

  if (strcmp(type, "ack") == 0) {
    const char* ackCmd = doc["cmd"] | "";
    if (strcasecmp(ackCmd, pendingCmd.cmdName) == 0) isMatch = true;
  } else if (strcmp(type, "cal_resp") == 0 && strcmp(pendingCmd.cmdName, "calibrate") == 0) {
    isMatch = true;
  } else if (strcmp(type, "tune_resp") == 0 && strcmp(pendingCmd.cmdName, "TunePI") == 0) {
    isMatch = true;
  } else if (strcmp(type, "status") == 0 && strcmp(pendingCmd.cmdName, "status") == 0) {
    isMatch = true;
  } else if (strcmp(type, "ota_ack") == 0 && strcmp(pendingCmd.cmdName, "ota") == 0) {
    isMatch = true;
  }

  if (isMatch) {
    Serial.printf("[RETRY] ACK matched — clearing pending cmd='%s'\n", pendingCmd.cmdName);
    pendingCmd.active = false;
  }
}

// ================================================================
//  OTA — publish ack to cloud
// ================================================================
static void publishOtaAck(bool success, const char* target, const char* detail) {
  StaticJsonDocument<220> doc;
  doc["type"]   = success ? "ota_ack" : "ota_no_ack";
  doc["UId"]    = GW_DEVICE_ID;
  doc["target"] = target;
  if (detail && strlen(detail)) doc["detail"] = detail;
  char buf[220];
  serializeJson(doc, buf, sizeof(buf));
  publishOrBuffer(TOPIC_ACK, buf);
  Serial.printf("[OTA] %s -> AWS: %s\n", success ? "ota_ack" : "ota_no_ack", buf);
}

// ================================================================
//  OTA — self-flash over WiFi, runs on otaTask (NOT mqttTask)
// ================================================================
static void selfOtaUpdate(const char* ssid, const char* pass, const char* url) {
  if (!ssid || !strlen(ssid) || !pass || !strlen(pass)) {
    Serial.println("[OTA] missing wifi_ssid/wifi_pass");
    publishOtaAck(false, "Fertigation gateway", "missing wifi credentials");
    return;
  }

  Serial.printf("[OTA] joining WiFi '%s'...\n", ssid);
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, pass);

  uint32_t deadline = millis() + 20000UL;
  while (WiFi.status() != WL_CONNECTED && millis() < deadline) vTaskDelay(pdMS_TO_TICKS(500));

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[OTA] WiFi join failed/timed out");
    publishOtaAck(false, "Fertigation gateway", "wifi join failed");
    WiFi.mode(WIFI_OFF);
    return;
  }
  Serial.printf("[OTA] WiFi joined, IP: %s — downloading %s\n",
                WiFi.localIP().toString().c_str(), url);

  const int MAX_ATTEMPTS = 3;
  t_httpUpdate_return result = HTTP_UPDATE_FAILED;

  for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    Serial.printf("[OTA] attempt %d/%d...\n", attempt, MAX_ATTEMPTS);
    WiFiClientSecure client;
    client.setInsecure();
    client.setTimeout(15000);
    httpUpdate.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
    httpUpdate.rebootOnUpdate(true);
    result = httpUpdate.update(client, url);
    if (result == HTTP_UPDATE_OK) break;
    Serial.printf("[OTA]  failed: %d %s\n", httpUpdate.getLastError(),
                  httpUpdate.getLastErrorString().c_str());
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
//  otaTask (lowest priority — never blocks LoRa/MQTT)
// ================================================================
void otaTask(void* param) {
  for (;;) {
    SelfOtaJob job;
    if (xQueueReceive(otaJobQueue, &job, portMAX_DELAY) == pdTRUE) {
      Serial.println("[OTA] otaTask: starting self-OTA job");
      selfOtaUpdate(job.ssid, job.pass, job.url);
    }
  }
}

// ================================================================
//  OTA — handle OTA/mode message (called from mqttCallback)
//
//  Device-type routing (matches the OTA config page's dropdown):
//    "Fertigation gateway" -> self-OTA, THIS board, over WiFi
//    "Fertigation"         -> relayed over LoRa to the node, using
//                              this file's own to/from envelope
//                              convention (not Did/uid).
// ================================================================
void handleOtaCommand(const char* payload, uint32_t len) {
  StaticJsonDocument<512> doc;
  char buf[512];
  uint32_t n = (len < sizeof(buf) - 1) ? len : sizeof(buf) - 1;
  memcpy(buf, payload, n); buf[n] = '\0';

  if (deserializeJson(doc, buf) != DeserializationError::Ok) {
    Serial.println("[OTA] JSON parse error");
    return;
  }

  const char* uid = doc["UId"] | "";
  if (strcmp(uid, GW_DEVICE_ID) != 0) {
    Serial.println("[OTA] UId mismatch — ignoring");
    return;
  }

  const char* deviceType = doc["device"] | "";
  const char* url        = doc["url"] | "";
  const char* ssid       = doc["wifi_ssid"] | "";
  const char* pass       = doc["wifi_pass"] | "";

  if (!strlen(url)) {
    Serial.println("[OTA] missing url");
    return;
  }

  if (strcasecmp(deviceType, "fertigation gateway") == 0) {
    SelfOtaJob job;
    strlcpy(job.ssid, ssid, sizeof(job.ssid));
    strlcpy(job.pass, pass, sizeof(job.pass));
    strlcpy(job.url,  url,  sizeof(job.url));
    if (xQueueSend(otaJobQueue, &job, 0) != pdTRUE) {
      Serial.println("[OTA] otaJobQueue full — OTA already in progress");
      publishOtaAck(false, "Fertigation gateway", "OTA already in progress");
    } else {
      Serial.println("[OTA] Self-OTA job queued -> otaTask");
    }
    return;
  }

  if (strcasecmp(deviceType, "fertigation") == 0 ||
      strcasecmp(deviceType, "fertigation node") == 0) {
    // Uses this file's own to/from LoRa envelope, not Did/uid.
    StaticJsonDocument<300> pkt;
    pkt["to"]   = FERT_UNIT_ID;
    pkt["from"] = GW_DEVICE_ID;
    pkt["type"] = "ota";
    pkt["url"]  = url;
    if (strlen(ssid)) pkt["wifi"] = ssid;
    if (strlen(pass)) pkt["pass"] = pass;

    String out;
    serializeJson(pkt, out);
    if (out.length() > 250)
      Serial.printf("[WARN] OTA forward payload %u bytes — near LoRa 255-byte cap\n", out.length());
    loraTxWithRetry(out.c_str(), "ota");
    Serial.printf("[OTA->node] %s\n", out.c_str());
    return;
  }

  Serial.printf("[OTA] unrecognised device type '%s'\n", deviceType);
}

// ================================================================
//  POWER-ON ANNOUNCEMENT
// ================================================================
void sendPowerOnAnnouncement() {
  StaticJsonDocument<256> d;
  d["device"] = nodeId.valid ? nodeId.device : "unknown";
  d["UId"]    = nodeId.valid ? nodeId.UId    : "unknown";
  d["FId"]    = nodeId.valid ? nodeId.FId    : "unknown";
  d["BId"]    = nodeId.valid ? nodeId.BId    : "unknown";
  d["type"]   = "power_on";
  d["ts"]     = (uint32_t)(millis() / 1000UL);
  String out; serializeJson(d, out);
  enqueueMqttPublish(TOPIC_OUTDOOR, out.c_str());
  Serial.printf("[BOOT] power_on sent: %s\n", out.c_str());
}

// ================================================================
//  INITIAL SETPOINT REQUEST
// ================================================================
void requestInitialSetpoint() {
  if (initialSetupDone) return;

  StaticJsonDocument<256> d;
  d["device"] = nodeId.valid ? nodeId.device : "unknown";
  d["UId"]    = nodeId.valid ? nodeId.UId    : "unknown";
  d["FId"]    = nodeId.valid ? nodeId.FId    : "unknown";
  d["BId"]    = nodeId.valid ? nodeId.BId    : "unknown";
  d["type"]   = "alert";
  d["e_stop"] = false; d["session_locked"] = false; d["failsafe"] = false;
  d["alert_code"] = 0; d["ph_partial"] = false; d["cloud_online"] = true;
  d["reason"] = "initial_setpoint_request";
  String out; serializeJson(d, out);
  enqueueMqttPublish(TOPIC_ACK, out.c_str());
  Serial.printf("[BOOT] initial setpoint request sent: %s\n", out.c_str());
}

// ================================================================
//  MQTT-STATE NOTIFICATION TO NODE
// ================================================================
void notifyNodeCloudState(bool online) {
  StaticJsonDocument<128> d;
  d["to"]    = FERT_UNIT_ID;
  d["from"]  = GW_DEVICE_ID;
  d["type"]  = "mqtt";
  d["state"] = online ? "connected" : "failed";
  String out; serializeJson(d, out);
  LoraTxPacket tx; strlcpy(tx.payload, out.c_str(), sizeof(tx.payload));
  xQueueSend(txQueue, &tx, pdMS_TO_TICKS(50));
}

// ================================================================
//  MQTT CALLBACK
// ================================================================
void mqttCallback(const char* topic, const uint8_t* payload, uint32_t len) {
  if (strcmp(topic, TOPIC_OTA) == 0) {
    handleOtaCommand((const char*)payload, len);
    return;
  }
  if (strcmp(topic, TOPIC_CMD) != 0) return;

  StaticJsonDocument<768> doc;
  char buf[768];
  uint32_t n = (len < sizeof(buf)-1) ? len : sizeof(buf)-1;
  memcpy(buf, payload, n); buf[n] = '\0';

  Serial.printf("[MQTT RX %s] %s\n", topic, buf);

  if (deserializeJson(doc, buf)) { Serial.println("[MQTT] bad JSON on fert/cmd"); return; }

  // v1.2: check this first, so another gateway's setpoint can't change
  // this gateway's setup state or setpoint cache.
  const char* cloudUid = doc["uid"] | "";
  if (!cloudUid[0]) cloudUid = doc["UId"] | "";
  if (strcmp(cloudUid, GW_DEVICE_ID) != 0) {
    Serial.printf("[MQTT] UId mismatch ('%s') — ignored\n", cloudUid);
    return;
  }

  const char* incomingType = doc["type"] | "";
  if (strcmp(incomingType, "setpoint") == 0) {
    markSetupDone();
    cacheSetpointFromCmd(doc);
  }

  // v1.2: the running node reads the command name from "type", so
  // {"type":"cmd","cmd":"stop"} is forwarded as {"type":"stop"}.
  const char* commandName = incomingType;
  if (strcmp(incomingType, "cmd") == 0) {
    const char* nestedCmd = doc["cmd"] | "";
    if (!nestedCmd[0]) {
      Serial.println("[MQTT] type=cmd without cmd field — ignored");
      return;
    }
    commandName = nestedCmd;
    doc["type"] = commandName;
    doc.remove("cmd");
  }

  // v1.2: remove cloud IDs and add the IDs checked by the node.
  doc.remove("Did");
  doc.remove("uid");
  doc.remove("fid");
  doc.remove("bid");
  doc.remove("FId");
  doc.remove("BId");

  doc["device"] = FERT_UNIT_ID;
  doc["UId"]    = GW_DEVICE_ID;
  doc["to"]     = FERT_UNIT_ID;
  doc["from"]   = GW_DEVICE_ID;

  String out; serializeJson(doc, out);
  if (out.length() > 250) Serial.printf("[WARN] cmd forward payload %u bytes — near LoRa 255-byte cap\n", out.length());
  loraTxWithRetry(out.c_str(), commandName);
}

// ================================================================
//  MQTT-STATE NOTIFICATION TO NODE — debounced
// ================================================================
void notifyNodeCloudStateDebounced(bool online) {
  uint32_t now = millis();
  if (online == lastNotifiedOnline && (now - lastMqttNotifyMs) < MQTT_NOTIFY_DEBOUNCE) return;
  lastNotifiedOnline = online;
  lastMqttNotifyMs   = now;
  cloudOnline = online;
  notifyNodeCloudState(online);
}

// ================================================================
//  MQTT CONNECT
// ================================================================
bool mqttConnectOnce() {
  modem.mqtt_set_certificate(root_ca, certificate, privateKey);
  if (!modem.mqtt_connect(0, broker, broker_port, client_id, NULL, NULL, MQTT_KEEPALIVE_SEC))
    return false;
  modem.mqtt_subscribe(0, TOPIC_CMD, 0);
  modem.mqtt_subscribe(0, TOPIC_OTA, 1);
  Serial.printf("[MQTT] subscribed to %s and %s\n", TOPIC_CMD, TOPIC_OTA);
  return true;
}

// ================================================================
//  PROCESS LoRa RX
// ================================================================
void processRx(const LoRaRxPacket& pkt) {
  String body(pkt.buf, pkt.len); body.trim();
  int js = body.indexOf('{');
  if (js < 0) return;
  if (js > 0) body = body.substring(js);

  StaticJsonDocument<768> doc;
  DeserializationError err = deserializeJson(doc, body);
  if (err != DeserializationError::Ok) {
    Serial.printf("[RX] JSON parse error: %s  rssi=%d len=%u\n",
                  err.c_str(), pkt.rssi, pkt.len);
    return;
  }

  const char* dev  = doc["device"] | "";
  const char* type = doc["type"]   | "";
  if (strcmp(dev, FERT_UNIT_ID) != 0) { Serial.printf("[RX] ignored, device=%s\n", dev); return; }

  Serial.printf("[RX] type=%s RSSI=%d\n", type, pkt.rssi);

  captureNodeIdentity(doc);
  clearPendingIfAcked(type, doc);

  if (strcmp(type, "status") == 0 || strcmp(type, "power_on") == 0) {
    publishOrBuffer(TOPIC_OUTDOOR, body.c_str());
  }
  else if (strcmp(type, "setpoint_req") == 0) {
    StaticJsonDocument<256> r;
    r["to"]   = FERT_UNIT_ID;
    r["from"] = GW_DEVICE_ID;
    r["type"] = "setpoint_resp";
    r["device"] = FERT_UNIT_ID;
    r["UId"]    = nodeId.valid ? nodeId.UId : "";
    if (!gwSetpoint.valid) {
      r["error"] = "not_set";
    } else {
      r["ph"] = gwSetpoint.ph; r["ec"] = gwSetpoint.ec;
      r["n1"] = gwSetpoint.n1; r["n2"] = gwSetpoint.n2; r["n3"] = gwSetpoint.n3;
      r["n4"] = gwSetpoint.n4; r["n5"] = gwSetpoint.n5;
      r["roles"] = gwSetpoint.roles;
      r["tankstatus"]  = gwSetpoint.tankstatus;
      r["valvestatus"] = gwSetpoint.valvestatus;
    }
    String out; serializeJson(r, out);
    LoraTxPacket tx; strlcpy(tx.payload, out.c_str(), sizeof(tx.payload));
    xQueueSend(txQueue, &tx, pdMS_TO_TICKS(50));
    Serial.printf("[SETPOINT_REQ] replied: %s\n", out.c_str());
  }
  else if (strcmp(type, "ota_ack") == 0) {
    bool ok = doc["ok"] | false;
    const char* err2 = doc["err"] | "";
    publishOtaAck(ok, FERT_UNIT_ID, err2);
  }
  else if (strcmp(type, "ack") == 0 ||
           strcmp(type, "alert") == 0 ||
           strcmp(type, "tune_resp") == 0 ||
           strcmp(type, "cal_resp") == 0 ||
           strcmp(type, "roles_resp") == 0) {
    publishOrBuffer(TOPIC_ACK, body.c_str());
  }
  else {
    Serial.printf("[RX] unhandled type=%s — not routed\n", type);
  }
}

// ================================================================
//  LoRa TASK
// ================================================================
void loraTask(void* p) {
  LoRa.receive();
  Serial.println("[LORA] task ready");
  for (;;) {
    int sz = LoRa.parsePacket();
    if (sz > 0) {
      LoRaRxPacket rx;
      int bl = sz; if (bl > (int)(sizeof(rx.buf)-1)) bl = sizeof(rx.buf)-1;
      rx.len  = (uint16_t)LoRa.readBytes((uint8_t*)rx.buf, bl);
      rx.buf[rx.len] = '\0';
      rx.rssi = LoRa.packetRssi();
      xQueueSend(rxQueue, &rx, 0);
      LoRa.receive();
    }

    LoraTxPacket tx;
    if (xQueueReceive(txQueue, &tx, 0) == pdTRUE) {
      LoRa.beginPacket();
      LoRa.print(tx.payload);
      LoRa.endPacket();
      LoRa.receive();
      Serial.printf("[LORA TX] %s\n", tx.payload);
    }

    vTaskDelay(pdMS_TO_TICKS(5));
  }
}

// ================================================================
//  MAIN TASK
// ================================================================
void mainTask(void* p) {
  for (;;) {
    LoRaRxPacket pkt;
    while (xQueueReceive(rxQueue, &pkt, 0) == pdTRUE) processRx(pkt);
    checkCmdRetry();
    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ================================================================
//  MQTT TASK
// ================================================================
void mqttTask(void* param) {
  pinMode(MODEM_PWKEY, OUTPUT);
  digitalWrite(MODEM_PWKEY, LOW);
  vTaskDelay(pdMS_TO_TICKS(1500));
  digitalWrite(MODEM_PWKEY, HIGH);
  vTaskDelay(pdMS_TO_TICKS(5000));

  SerialAT.setRxBufferSize(2048);
  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX, MODEM_TX);
  Serial.println("[GSM] SerialAT RX buffer set to 2048 bytes");
  vTaskDelay(pdMS_TO_TICKS(3000));

  Serial.println("[GSM] Modem booting...");
  Serial.printf("[GSM] TINY_GSM_RX_BUFFER=%d\n", TINY_GSM_RX_BUFFER);
  modem.init();
  vTaskDelay(pdMS_TO_TICKS(500));
  Serial.println("[GSM] " + modem.getModemInfo());

  uint32_t netDeadline = millis() + 90000UL;
  while (!modem.waitForNetwork(5000)) {
    Serial.println("[GSM] registering...");
    if (millis() > netDeadline) {
      modem.restart();
      vTaskDelay(pdMS_TO_TICKS(5000));
      netDeadline = millis() + 90000UL;
    }
  }
  Serial.println("[GSM] network registered");

  while (!modem.gprsConnect(apn, gprsUser, gprsPass)) {
    Serial.println("[GSM] GPRS retry...");
    vTaskDelay(pdMS_TO_TICKS(5000));
  }
  Serial.println("[GSM] GPRS up | IP: " + modem.localIP().toString());

  modem.mqtt_begin(false);
  modem.mqtt_set_callback(mqttCallback);
  Serial.println("[MQTT] client initialised");

  enum State { CONNECTING, CONNECTED } state = CONNECTING;
  uint32_t retryDelay    = MQTT_RETRY_MIN_MS;
  uint32_t nextRetry     = 0;
  uint32_t lastHandleMs  = 0;
  uint32_t lastSuccessMs = millis();
  bool     notified      = false;
  uint8_t  failCount     = 0;

  for (;;) {
    uint32_t now = millis();

    if (now - lastHandleMs >= 10) { lastHandleMs = now; modem.mqtt_handle(); }

    if (state == CONNECTED) {
      MqttPubReq req;
      if (xQueueReceive(mqttPubQueue, &req, 0) == pdTRUE) {
        if (modem.mqtt_publish(0, req.topic, req.payload, 0)) {
          failCount = 0;
        } else {
          Serial.printf("[MQTT] publish FAILED [%s] — buffering\n", req.topic);
          ringPush(req.topic, req.payload);
          if (++failCount >= MAX_MQTT_FAILURES) {
            failCount = 0; notified = false; state = CONNECTING;
            notifyNodeCloudStateDebounced(false);
            nextRetry = now + retryDelay;
          }
        }
      }
    }

    switch (state) {
      case CONNECTING:
        if (now < nextRetry) break;

        if (lastSuccessMs > 0 && (now - lastSuccessMs) > LTE_WATCHDOG_MS) {
          Serial.println("[GSM] LTE watchdog — restarting modem");
          modem.restart();
          vTaskDelay(pdMS_TO_TICKS(5000));
          modem.waitForNetwork(15000);
          modem.gprsConnect(apn, gprsUser, gprsPass);
          lastSuccessMs = millis();
          retryDelay = MQTT_RETRY_MIN_MS;
          nextRetry = 0;
          break;
        }

        Serial.println("[MQTT] connecting...");
        if (mqttConnectOnce()) {
          state = CONNECTED;
          retryDelay = MQTT_RETRY_MIN_MS;
          lastSuccessMs = millis();
          notified = false;
          failCount = 0;
          Serial.println("[MQTT] connected");
        } else {
          Serial.printf("[MQTT] failed — retry in %lu s\n", retryDelay/1000);
          nextRetry = now + retryDelay;
          retryDelay = min(retryDelay * 2, (uint32_t)MQTT_RETRY_MAX_MS);
        }
        break;

      case CONNECTED:
        if (!modem.mqtt_connected()) {
          Serial.println("[MQTT] dropped — reconnecting");
          notified = false; state = CONNECTING;
          notifyNodeCloudStateDebounced(false);
          nextRetry = now + retryDelay;
          break;
        }
        if (!notified) {
          notified = true;
          Serial.println("[MQTT] online");
          notifyNodeCloudStateDebounced(true);
          if (!powerOnSent) { sendPowerOnAnnouncement(); requestInitialSetpoint(); powerOnSent = true; }
          ringFlush();
        }
        lastSuccessMs = now;
        break;
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ================================================================
//  SETUP
// ================================================================
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n=== Agri Inverse Fertigation Gateway — GSM/AWS build (OTA v1.2) ===");

  rxQueue      = xQueueCreate(8, sizeof(LoRaRxPacket));
  txQueue      = xQueueCreate(8, sizeof(LoraTxPacket));
  mqttPubQueue = xQueueCreate(8, sizeof(MqttPubReq));
  otaJobQueue  = xQueueCreate(1, sizeof(SelfOtaJob));

  LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
  if (!LoRa.begin(LORA_FREQ)) { Serial.println("[LORA] FAILED"); while(1) delay(1000); }
  LoRa.setSpreadingFactor(LORA_SF);
  LoRa.setSignalBandwidth(LORA_BW);
  LoRa.setCodingRate4(LORA_CR);
  LoRa.setSyncWord(LORA_SW);
  LoRa.setTxPower(LORA_PWR, PA_OUTPUT_PA_BOOST_PIN);
  LoRa.enableCrc();
  Serial.println("[LORA] Ready");

  for (uint8_t i = 0; i < RING_SIZE; i++) ringBuf[i].used = false;

  loadNodeIdentity();
  loadSetupState();
  loadGwSetpoint();
  Serial.printf("[BOOT] cached setpoint valid: %s\n", gwSetpoint.valid ? "YES" : "NO");
  Serial.printf("[BOOT] initial setup already done: %s\n", initialSetupDone ? "YES" : "NO");
  if (nodeId.valid) Serial.printf("[BOOT] cached node identity: device=%s UId=%s\n", nodeId.device, nodeId.UId);
  else Serial.println("[BOOT] no cached node identity yet");

  xTaskCreatePinnedToCore(loraTask, "LoRa", 4096,  NULL, 3, NULL, 0);
  xTaskCreatePinnedToCore(mqttTask, "MQTT", 10240, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(mainTask, "Main", 4096,  NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(otaTask,  "OTA",  8192,  NULL, 1, NULL, 1);

  Serial.println("[BOOT] Ready — modem/MQTT connecting in background");
}

void loop() { vTaskDelay(portMAX_DELAY); }
