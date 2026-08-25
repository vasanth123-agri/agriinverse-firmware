// =============================================================================
//  AgriInverse — Solenoid Valve Node  (v3 — TB6612FNG dual H-bridge driver)
//
//  CHANGE FROM v2
//  ----------------
//  v2 drove the valve H-bridge with 4 raw GPIO pins (IN1-IN4), assuming a
//  driver (e.g. L298N) that outputs whenever a direction pin goes HIGH.
//
//  TB6612FNG requires explicit PWM gating — AIN1/AIN2 (or BIN1/BIN2) alone
//  do NOT produce output; PWMA/PWMB must be HIGH for the corresponding
//  channel to actually switch, and STBY must be HIGH to leave standby.
//  This board only has 6 usable GPIOs available (16,17,21,22,27,32 —
//  34/35 are input-only), not enough for both direction pins AND PWM
//  pins on both channels. Since this application never needs variable
//  speed — only full on/off — PWMA/PWMB are tied DIRECTLY TO 3.3V in
//  hardware instead of being GPIOs, freeing enough pins for STBY +
//  both channels' direction control.
//
//  TB6612FNG is a DUAL H-bridge — one chip drives BOTH valves:
//    Valve A → AIN1 / AIN2   (PWMA hardwired HIGH)
//    Valve B → BIN1 / BIN2   (PWMB hardwired HIGH)
//    STBY is SHARED across both channels.
//
//  Every actuation follows this safe sequence:
//    1. STBY HIGH          (leave standby)
//    2. Set direction       (AIN1/AIN2 or BIN1/BIN2)
//    3. PWMx HIGH            (channel actually outputs now)
//    4. Hold for exactly PULSE_MS
//    5. PWMx LOW             (stop immediately)
//    6. Direction pins LOW   (clear, unambiguous state)
//    7. STBY LOW            (back to true standby — near-zero current,
//                             unlike a raw L298N which can't fully sleep)
//
//  Everything else — mesh routing, dedupe, hop/relay, OTA, ack format —
//  is UNCHANGED from v2.
// =============================================================================

#include <SPI.h>
#include <LoRa.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>

// =============================================================================
//  SECTION 1 — LoRa PINS & FREQUENCY
// =============================================================================

#define LORA_SS   5
#define LORA_RST  14
#define LORA_DIO0 26
#define LORA_FREQ 433E6

// =============================================================================
//  SECTION 2 — NODE IDENTITY  (EDIT THESE PER PHYSICAL NODE)
// =============================================================================

#define MY_VID   2                                  // 1..15, unique per node
#define MY_DID   "DId2"                              // human-readable label
#define MY_FID   "cmswv2gnx001342b0b6bj1bwd"         // farm id
#define MY_BID   "cmswv2i62001742b00omppsyk"         // block id
#define MY_UID   "66"                                // same across the whole farm

#define GATEWAY_ID  0xBB
#define BROADCAST   0xD1

// =============================================================================
//  SECTION 3 — TB6612FNG DUAL H-BRIDGE PINS
//  Only 6 usable GPIOs available on this board: 16,17,21,22,27,32
//  (34 and 35 are INPUT-ONLY on ESP32 — cannot drive outputs, left unused)
//
//  PWMA/PWMB are NOT wired to any GPIO — tie them DIRECTLY TO 3.3V in
//  hardware. Since this application only ever needs full on/off drive
//  (never variable speed), permanently-high PWM plus STBY + direction
//  pins alone gives complete control with no GPIO cost.
// =============================================================================

// Valve A
#define AIN1  16
#define AIN2  17

// Valve B
#define BIN1  21
#define BIN2  22

// Shared standby — LOW = both channels off, true low-power state
#define STBY  27

// GPIO32 — spare, unused for now
// GPIO34, GPIO35 — input-only, cannot drive outputs, left unconnected

#define PULSE_MS 300
#define SAFE_MS  150

enum ValveState { VS_UNKNOWN = 0, VS_OPEN = 1, VS_CLOSED = 2 };
ValveState valveStateA = VS_UNKNOWN;
ValveState valveStateB = VS_UNKNOWN;

// =============================================================================
//  SECTION 4 — MESH / HOP / DUTY-CYCLE SETTINGS
// =============================================================================

#define DEFAULT_HOP_COUNT     6
#define MIN_TX_GAP_MS         800
#define RSSI_STRONG           -60
#define RSSI_WEAK              -120
#define RSSI_DELAY_MIN_MS      40
#define RSSI_DELAY_MAX_MS      300

#define ACK_PRE_DELAY_MS      700
#define ACK_REPEATS           1

// =============================================================================
//  SECTION 5 — DEDUPE CACHE  (by msgId, not by content)
// =============================================================================

#define SEEN_CACHE_SIZE   24
#define SEEN_EXPIRE_MS    30000UL

struct SeenEntry { uint32_t msgId; unsigned long seenAt; bool used; };
SeenEntry seenCache[SEEN_CACHE_SIZE];
int seenIdx = 0;

bool alreadySeen(uint32_t msgId) {
    unsigned long now = millis();
    for (int i = 0; i < SEEN_CACHE_SIZE; i++) {
        if (seenCache[i].used &&
            seenCache[i].msgId == msgId &&
            (now - seenCache[i].seenAt) < SEEN_EXPIRE_MS)
            return true;
    }
    return false;
}
void markSeen(uint32_t msgId) {
    seenCache[seenIdx] = { msgId, millis(), true };
    seenIdx = (seenIdx + 1) % SEEN_CACHE_SIZE;
}

uint32_t lastExecutedCmdMsgId = 0;
uint32_t lastExecutedOtaMsgId = 0;

String last4(const char* s) {
    String str(s);
    int len = str.length();
    return (len <= 4) ? str : str.substring(len - 4);
}
String myFid4 = last4(MY_FID);
String myBid4 = last4(MY_BID);

// =============================================================================
//  SECTION 6 — DUTY CYCLE GUARD
// =============================================================================

unsigned long lastTxMs = 0;
bool canTransmit() { return (millis() - lastTxMs) >= MIN_TX_GAP_MS; }
void recordTx()     { lastTxMs = millis(); }

// =============================================================================
//  SECTION 7 — VALVE STATE PERSISTENCE (NVS)
// =============================================================================

void loadValveState() {
    Preferences p;
    p.begin("valve-state", true);
    valveStateA = (ValveState)p.getInt("A", VS_UNKNOWN);
    valveStateB = (ValveState)p.getInt("B", VS_UNKNOWN);
    p.end();
    Serial.printf("💾 Loaded valve state — A:%d B:%d\n", valveStateA, valveStateB);
}
void saveValveState(const String& valve, ValveState state) {
    Preferences p;
    p.begin("valve-state", false);
    p.putInt(valve.c_str(), (int)state);
    p.end();
}
bool isRedundant(const String& valve, const String& cmd) {
    ValveState cur = (valve == "A") ? valveStateA : valveStateB;
    if (cur == VS_UNKNOWN) return false;
    if (cmd == "open"  && cur == VS_OPEN)   return true;
    if (cmd == "close" && cur == VS_CLOSED) return true;
    return false;
}
void updateState(const String& valve, const String& cmd) {
    ValveState next = (cmd == "open") ? VS_OPEN : VS_CLOSED;
    if (valve == "A") valveStateA = next; else valveStateB = next;
    saveValveState(valve, next);
}

// =============================================================================
//  SECTION 8 — RSSI-WEIGHTED FORWARD DELAY
// =============================================================================

int rssiForwardDelay(int rssi) {
    int clamped = constrain(rssi, RSSI_WEAK, RSSI_STRONG);
    int base = map(clamped, RSSI_WEAK, RSSI_STRONG, RSSI_DELAY_MAX_MS, RSSI_DELAY_MIN_MS);
    return base + random(10, 60);
}

// =============================================================================
//  SECTION 9 — INTERRUPT-DRIVEN RX QUEUE
// =============================================================================

#define RX_QUEUE_SIZE 4

struct RawPacket {
    uint8_t  dest;
    uint8_t  src;
    char     buf[256];
    uint16_t len;
    int      rssi;
    bool     used;
};

volatile RawPacket rxQueue[RX_QUEUE_SIZE];
volatile uint8_t   rxHead = 0;
volatile uint8_t   rxTail = 0;

void onReceivePacket(int packetSize) {
    if (packetSize < 2) return;

    uint8_t next = (rxHead + 1) % RX_QUEUE_SIZE;
    if (next == rxTail) return;

    volatile RawPacket& slot = rxQueue[rxHead];
    slot.dest = LoRa.read();
    slot.src  = LoRa.read();

    int bodyLen = packetSize - 2;
    if (bodyLen > (int)(sizeof(slot.buf) - 1)) bodyLen = sizeof(slot.buf) - 1;
    int i = 0;
    while (i < bodyLen && LoRa.available()) {
        slot.buf[i] = (char)LoRa.read();
        i++;
    }
    slot.buf[i] = '\0';
    slot.len  = i;
    slot.rssi = LoRa.packetRssi();
    slot.used = true;

    rxHead = next;
}

// =============================================================================
//  SECTION 10 — LOW-LEVEL SEND
// =============================================================================

void sendRaw(const String& json) {
    if (!canTransmit()) {
        Serial.println("⚠️ TX skipped — duty cycle guard");
        return;
    }
    LoRa.beginPacket();
    LoRa.write(BROADCAST);
    LoRa.write(MY_VID);
    LoRa.print(json);
    LoRa.endPacket();
    recordTx();
    LoRa.receive();
}

void relayEnvelope(StaticJsonDocument<600>& doc, int hop, int rssi, const char* why) {
    if (hop <= 0) {
        Serial.printf("⏭ %s — hop exhausted, not relaying\n", why);
        return;
    }
    doc["hop"] = hop - 1;
    doc["via"] = MY_DID;
    String out;
    serializeJson(doc, out);

    int d = rssiForwardDelay(rssi);
    delay(d);
    sendRaw(out);
    Serial.printf("🔁 Relayed [%s] hop %d→%d | delay %dms\n", why, hop, hop - 1, d);
}

// =============================================================================
//  SECTION 11 — ACK / OTA_ACK SENDERS
// =============================================================================

void sendAck(const String& status, const char* fid, const char* bid,
            uint32_t replyToMsgId) {
    StaticJsonDocument<300> ack;
    ack["type"]    = "ack";
    ack["UId"]     = MY_UID;
    ack["VId"]     = MY_VID;
    ack["msgId"]   = (uint32_t)esp_random();
    ack["replyTo"] = replyToMsgId;
    ack["hop"]     = DEFAULT_HOP_COUNT;
    ack["FId"]     = fid;
    ack["BId"]     = bid;
    ack["from"]    = MY_DID;
    ack["node"]    = "Solenoid valve";
    ack["status"]  = status;

    String out;
    serializeJson(ack, out);

    Serial.printf("⏳ ACK pre-delay %dms...\n", ACK_PRE_DELAY_MS);
    delay(ACK_PRE_DELAY_MS);
    for (int i = 0; i < ACK_REPEATS; i++) sendRaw(out);
    Serial.printf("📡 ACK sent: %s\n", out.c_str());
}

void sendOtaAck(bool ok, const char* fid, const char* bid,
               uint32_t replyToMsgId, const char* detail) {
    StaticJsonDocument<300> ack;
    ack["type"]    = "ota_ack";
    ack["UId"]     = MY_UID;
    ack["VId"]     = MY_VID;
    ack["msgId"]   = (uint32_t)esp_random();
    ack["replyTo"] = replyToMsgId;
    ack["hop"]     = DEFAULT_HOP_COUNT;
    ack["FId"]     = fid;
    ack["BId"]     = bid;
    ack["from"]    = MY_DID;
    ack["node"]    = "Solenoid valve";
    ack["status"]  = ok ? "ok" : "fail";
    if (detail && strlen(detail)) ack["detail"] = detail;

    String out;
    serializeJson(ack, out);
    delay(ACK_PRE_DELAY_MS);
    for (int i = 0; i < ACK_REPEATS; i++) sendRaw(out);
    Serial.printf("📡 OTA-ACK sent: %s\n", out.c_str());
}

// =============================================================================
//  SECTION 12 — VALVE DRIVERS  (TB6612FNG — proper PWM gating + STBY)
// =============================================================================

void actuateValve(uint8_t ainPos, uint8_t ainNeg,
                  bool energizePositive, const char* label, const char* action,
                  const char* fid, const char* bid, uint32_t msgId) {
    digitalWrite(ainPos, LOW);
    digitalWrite(ainNeg, LOW);
    delay(SAFE_MS);

    digitalWrite(STBY, HIGH);
    digitalWrite(ainPos, energizePositive ? HIGH : LOW);
    digitalWrite(ainNeg, energizePositive ? LOW  : HIGH);
    // PWMA/PWMB are hardwired to 3.3V — direction pins alone now control
    // output, since PWM is permanently "on" at the hardware level.

    Serial.printf("%s Valve %s\n", energizePositive ? "🔓 Opening" : "🔒 Closing", label);
    delay(PULSE_MS);

    digitalWrite(ainPos, LOW);
    digitalWrite(ainNeg, LOW);
    digitalWrite(STBY, LOW);

    updateState(String(label), action);
    String status = "Valve " + String(label) + (energizePositive ? " Opened" : " Closed");
    sendAck(status, fid, bid, msgId);
}

void openValveA(const char* fid, const char* bid, uint32_t msgId) {
    actuateValve(AIN1, AIN2, true, "A", "open", fid, bid, msgId);
}
void closeValveA(const char* fid, const char* bid, uint32_t msgId) {
    actuateValve(AIN1, AIN2, false, "A", "close", fid, bid, msgId);
}
void openValveB(const char* fid, const char* bid, uint32_t msgId) {
    actuateValve(BIN1, BIN2, true, "B", "open", fid, bid, msgId);
}
void closeValveB(const char* fid, const char* bid, uint32_t msgId) {
    actuateValve(BIN1, BIN2, false, "B", "close", fid, bid, msgId);
}

// =============================================================================
//  SECTION 13 — OTA  (self-flash, blocking — acceptable, no other duty here)
// =============================================================================

void runSelfOta(const char* url, const char* ssid, const char* pass,
                const char* fid, const char* bid, uint32_t msgId) {
    if (!strlen(url)) {
        sendOtaAck(false, fid, bid, msgId, "missing url");
        return;
    }

    bool useWifi = strlen(ssid) > 0;
    if (useWifi) {
        Serial.printf("📶 OTA: joining WiFi '%s'...\n", ssid);
        WiFi.mode(WIFI_STA);
        WiFi.begin(ssid, pass);
        uint32_t deadline = millis() + 20000UL;
        while (WiFi.status() != WL_CONNECTED && millis() < deadline) delay(500);
        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("❌ OTA: WiFi join failed");
            sendOtaAck(false, fid, bid, msgId, "wifi join failed");
            WiFi.mode(WIFI_OFF);
            return;
        }
        Serial.printf("✅ WiFi joined, IP: %s\n", WiFi.localIP().toString().c_str());
    } else {
        Serial.println("❌ OTA: no wifi_ssid supplied — this node has no other network path");
        sendOtaAck(false, fid, bid, msgId, "no network path (node has no LTE)");
        return;
    }

    WiFiClientSecure client;
    client.setInsecure();
    client.setTimeout(15000);
    httpUpdate.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
    httpUpdate.rebootOnUpdate(true);

    const int MAX_ATTEMPTS = 3;
    t_httpUpdate_return result = HTTP_UPDATE_FAILED;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        Serial.printf("OTA attempt %d/%d...\n", attempt, MAX_ATTEMPTS);
        result = httpUpdate.update(client, url);
        if (result == HTTP_UPDATE_OK) break;
        Serial.printf("  failed: %d %s\n", httpUpdate.getLastError(),
                      httpUpdate.getLastErrorString().c_str());
        if (attempt < MAX_ATTEMPTS) delay(3000);
    }

    switch (result) {
        case HTTP_UPDATE_FAILED:
            sendOtaAck(false, fid, bid, msgId, httpUpdate.getLastErrorString().c_str());
            break;
        case HTTP_UPDATE_NO_UPDATES:
            sendOtaAck(false, fid, bid, msgId, "no update at url");
            break;
        case HTTP_UPDATE_OK:
            break;
    }
    WiFi.mode(WIFI_OFF);
}

// =============================================================================
//  SECTION 14 — PACKET PROCESSING  (called from loop(), not the ISR)
// =============================================================================

void processPacket(uint8_t dest, uint8_t src, const char* buf, uint16_t len, int rssi) {
    if (dest != BROADCAST) return;

    StaticJsonDocument<600> doc;
    if (deserializeJson(doc, buf, len) != DeserializationError::Ok) {
        Serial.println("⚠️ JSON parse error");
        return;
    }

    const char* type = doc["type"] | "";
    const char* uid  = doc["UId"]  | "";
    uint32_t msgId    = doc["msgId"] | 0;
    int      hop      = doc["hop"]   | 0;

    if (strcmp(uid, MY_UID) != 0) return;
    if (msgId == 0) return;

    Serial.printf("\n📥 [%s] from 0x%02X RSSI:%d msgId:%lu hop:%d\n",
                  type, src, rssi, (unsigned long)msgId, hop);

    if (strcmp(type, "ack") == 0 || strcmp(type, "ota_ack") == 0) {
        const char* from = doc["from"] | "";
        if (strcmp(from, MY_DID) == 0) return;
        if (alreadySeen(msgId)) return;
        markSeen(msgId);
        relayEnvelope(doc, hop, rssi, type);
        return;
    }

    if (strcmp(type, "command_broadcast") != 0 && strcmp(type, "ota") != 0) {
        Serial.printf("⏭ Unknown type: %s\n", type);
        return;
    }

    if (alreadySeen(msgId)) { Serial.println("⏭ Duplicate msgId"); return; }
    markSeen(msgId);

    if (!doc.containsKey("VId")) { Serial.println("⏭ Missing VId — rejecting"); return; }
    int vId = doc["VId"];

    if (vId != MY_VID) {
        relayEnvelope(doc, hop, rssi, type);
        return;
    }

    bool isOta = (strcmp(type, "ota") == 0);

    if (isOta) {
        const char* fid4 = doc["FId4"] | "";
        const char* bid4 = doc["BId4"] | "";
        if (strlen(fid4) && myFid4 != fid4) {
            Serial.println("⏭ FId mismatch (OTA, last-4) — rejecting");
            sendOtaAck(false, MY_FID, MY_BID, msgId, "FId mismatch — wrong farm");
            return;
        }
        if (strlen(bid4) && myBid4 != bid4) {
            Serial.println("⏭ BId mismatch (OTA, last-4) — rejecting");
            sendOtaAck(false, MY_FID, MY_BID, msgId, "BId mismatch — wrong block");
            return;
        }
    } else {
        const char* fid = doc["FId"] | "";
        const char* bid = doc["BId"] | "";
        if (strlen(fid) && strcmp(fid, MY_FID) != 0) {
            Serial.println("⏭ FId mismatch — rejecting");
            sendAck("rejected — FId mismatch", MY_FID, MY_BID, msgId);
            return;
        }
        if (strlen(bid) && strcmp(bid, MY_BID) != 0) {
            Serial.println("⏭ BId mismatch — rejecting");
            sendAck("rejected — BId mismatch", MY_FID, MY_BID, msgId);
            return;
        }
    }
    const char* fid = MY_FID;
    const char* bid = MY_BID;

    if (isOta) {
        if (msgId == lastExecutedOtaMsgId) {
            Serial.println("⏭ OTA already actioned for this msgId — ack only");
            sendOtaAck(true, fid, bid, msgId, "duplicate — already handled");
            return;
        }
        lastExecutedOtaMsgId = msgId;

        const char* url  = doc["url"]  | "";
        const char* ssid = doc["wifi"] | "";
        const char* pass = doc["pass"] | "";
        Serial.printf("🎯 OTA for me — url:%s\n", url);
        runSelfOta(url, ssid, pass, fid, bid, msgId);
        return;
    }

    if (msgId == lastExecutedCmdMsgId) {
        Serial.println("⏭ Command already executed for this msgId — ack only, no re-actuation");
        sendAck("duplicate — already actioned", fid, bid, msgId);
        return;
    }

    if (!doc.containsKey("valve")) { Serial.println("⏭ No valve key"); return; }
    String valve = doc["valve"]   | "";
    String cmd   = doc["command"] | "";
    valve.toUpperCase();
    cmd.toLowerCase();

    Serial.printf("🎯 Valve:%s Command:%s\n", valve.c_str(), cmd.c_str());

    if (isRedundant(valve, cmd)) {
        lastExecutedCmdMsgId = msgId;
        String status = "Valve " + valve + (cmd == "open" ? " already open" : " already closed");
        sendAck(status, fid, bid, msgId);
        return;
    }

    lastExecutedCmdMsgId = msgId;
    if (valve == "A") {
        if      (cmd == "open")  openValveA(fid, bid, msgId);
        else if (cmd == "close") closeValveA(fid, bid, msgId);
        else Serial.printf("⚠️ Unknown command: %s\n", cmd.c_str());
    } else if (valve == "B") {
        if      (cmd == "open")  openValveB(fid, bid, msgId);
        else if (cmd == "close") closeValveB(fid, bid, msgId);
        else Serial.printf("⚠️ Unknown command: %s\n", cmd.c_str());
    } else {
        Serial.printf("⚠️ Unknown valve: %s\n", valve.c_str());
    }
}

// =============================================================================
//  SECTION 15 — SAFE STARTUP  (called first — TB6612 STBY LOW, all
//  direction/PWM pins LOW before anything else can possibly run)
// =============================================================================

void safeStartupPins() {
    pinMode(AIN1, OUTPUT); pinMode(AIN2, OUTPUT);
    pinMode(BIN1, OUTPUT); pinMode(BIN2, OUTPUT);
    pinMode(STBY, OUTPUT);

    digitalWrite(STBY, LOW);
    digitalWrite(AIN1, LOW); digitalWrite(AIN2, LOW);
    digitalWrite(BIN1, LOW); digitalWrite(BIN2, LOW);

    Serial.println("🔒 Safe startup — TB6612 STBY LOW, all direction pins LOW (PWMA/PWMB hardwired HIGH)");
}

// =============================================================================
//  SECTION 16 — SETUP
// =============================================================================

void setup() {
    Serial.begin(115200);

    safeStartupPins();

    for (int i = 0; i < SEEN_CACHE_SIZE; i++) seenCache[i] = { 0, 0, false };
    for (int i = 0; i < RX_QUEUE_SIZE; i++)   rxQueue[i].used = false;
    loadValveState();
    randomSeed(esp_random());

    Serial.println("\n🚀 Valve Node (TB6612FNG) starting...");
    Serial.printf("📛 VId:%d  DId:%s\n", MY_VID, MY_DID);

    LoRa.setPins(LORA_SS, LORA_RST, LORA_DIO0);
    if (!LoRa.begin(LORA_FREQ)) { Serial.println("❌ LoRa init failed!"); while (true); }
    LoRa.setSpreadingFactor(7);
    LoRa.setSignalBandwidth(125E3);
    LoRa.setCodingRate4(5);
    LoRa.setSyncWord(0xAB);
    LoRa.setPreambleLength(8);
    LoRa.enableCrc();

    LoRa.onReceive(onReceivePacket);
    LoRa.receive();

    Serial.println("✅ LoRa ready — SF7/BW125/CR5/0xAB — interrupt-driven RX active");
}

// =============================================================================
//  SECTION 17 — MAIN LOOP
// =============================================================================

void loop() {
    while (rxTail != rxHead) {
        uint8_t dest, src;
        char buf[256];
        uint16_t len;
        int rssi;

        noInterrupts();
        volatile RawPacket& slot = rxQueue[rxTail];
        dest = slot.dest;
        src  = slot.src;
        len  = slot.len;
        memcpy(buf, (const void*)slot.buf, len + 1);
        rssi = slot.rssi;
        slot.used = false;
        rxTail = (rxTail + 1) % RX_QUEUE_SIZE;
        interrupts();

        processPacket(dest, src, buf, len, rssi);
    }

    delay(5);
}
