// =============================================================================
//  AgriInverse — Solenoid Valve Node  (v4 — TB6612FNG + INA219 battery SoC)
//
//  CHANGES FROM v3pollachi
//  ----------------
//  1. INA219 added on I2C (SDA=21, SCL=22). Battery state-of-charge is
//     tracked by Coulomb counting (same method as the LiFePO4 logger:
//     OCV seed at boot, trapezoidal integration, full/empty snap-to-ref,
//     NVS persistence with a resting-OCV sanity check at boot).
//  2. Every "ack" now carries:
//        "bat" : battery SoC in whole percent (0..100, or -1 if INA219 missing)
//        "bv"  : battery bus voltage, 2 decimals (dropped automatically if
//                the packet would get too close to the LoRa 255-byte limit)
//  3. PIN REMAP — I2C needs 21/22, so Valve B moved:
//        BIN1 21 -> 27,  BIN2 22 -> 32
//     STBY is no longer a GPIO: tie TB6612 STBY directly to 3.3V in
//     hardware (same idea as PWMA/PWMB). With AIN/BIN both LOW the chip is
//     in "stop" (outputs high-impedance), so valves are still de-energised
//     between pulses. Cost: ~1-2 mA TB6612 logic current instead of ~1 uA.
//  4. The valve pulse is now metered: INA219 is sampled every 10 ms during
//     the 300 ms pulse so the solenoid's charge is actually counted.
//  5. Empty-voltage snap only applies at rest, so the voltage sag during a
//     valve pulse can't falsely zero the SoC.
//  6. SoC is saved to NVS right before OTA (OTA reboots the node).
//  7. sendRaw() refuses to transmit a body that won't fit in one LoRa packet
//     instead of letting it be silently truncated into broken JSON.
//
//  Pins used:
//    LoRa  : SS 5, RST 14, DIO0 26 (+ VSPI 18/19/23)
//    Valve : AIN1 16, AIN2 17, BIN1 27, BIN2 32
//    INA219: SDA 21, SCL 22
//    TB6612: PWMA, PWMB, STBY -> 3.3V (hardwired)
//    GPIO34/35: input-only, unused (cannot drive outputs or I2C)
//
//  Everything else — mesh routing, dedupe, hop/relay, OTA, ack format —
//  is unchanged from v3 apart from the two added ack fields.
// =============================================================================

#include <SPI.h>
#include <LoRa.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include <Wire.h>
#include <Adafruit_INA219.h>

// =============================================================================
//  SECTION 1 — LoRa PINS & FREQUENCY
// =============================================================================

#define LORA_SS   5
#define LORA_RST  14
#define LORA_DIO0 26
#define LORA_FREQ 433E6

// One LoRa packet = 255 bytes FIFO. We use 2 bytes for dest+src header.
#define LORA_MAX_BODY   253
// A relaying node appends "via":"DIdNN" (~14 bytes). Keep room for it so
// our ack still fits after being relayed.
#define RELAY_HEADROOM  16

// =============================================================================
//  SECTION 2 — NODE IDENTITY  (EDIT THESE PER PHYSICAL NODE)
// =============================================================================

#define MY_VID   12                                  // 1..15, unique per node
#define MY_DID   "DId12"                              // human-readable label
#define MY_FID   "cmswukkav000d420ine0pfxmw"         // farm id
#define MY_BID   "cmswuklzh000h420ih0axdrgj"         // block id
#define MY_UID   "66"                                // same across the whole farm

#define GATEWAY_ID  0xBB
#define BROADCAST   0xD1

// =============================================================================
//  SECTION 3 — TB6612FNG DUAL H-BRIDGE PINS
//
//  PWMA, PWMB and STBY are all tied DIRECTLY TO 3.3V in hardware.
//  IN1=IN2=LOW with PWM HIGH = "stop" mode (outputs OFF / high-Z), so the
//  valves are de-energised whenever both direction pins are LOW.
//
//  Recommended: 10k pull-downs from AIN1/AIN2/BIN1/BIN2 to GND so the
//  inputs are firmly LOW while the ESP32 is booting (the TB6612 has weak
//  internal pull-downs, but external ones remove any doubt now that STBY
//  is no longer holding the chip in standby).
// =============================================================================

// Valve A
#define AIN1  16
#define AIN2  17

// Valve B  (moved off 21/22 — those are now the INA219 I2C bus)
#define BIN1  27
#define BIN2  32

#define PULSE_MS 300
#define SAFE_MS  150

enum ValveState { VS_UNKNOWN = 0, VS_OPEN = 1, VS_CLOSED = 2 };
ValveState valveStateA = VS_UNKNOWN;
ValveState valveStateB = VS_UNKNOWN;

// =============================================================================
//  SECTION 3B — BATTERY MONITOR (INA219, Coulomb counting)
//
//  Wiring (high-side): battery+ -> INA219 VIN+ ; INA219 VIN- -> node supply
//  (TB6612 VM + ESP32 regulator). Any charger/solar output must connect on
//  the VIN- side so charge current flows back through the shunt.
//
//  Defaults assume a single-cell LiFePO4, 6000 mAh (same as the logger).
//  If this node's pack is different, change capacity + the OCV table.
// =============================================================================

#define I2C_SDA 21
#define I2C_SCL 22

Adafruit_INA219 ina219;
bool inaOk = false;

const float NOMINAL_CAPACITY_MAH = 6000.0f;

const float V_FULL_CV             = 3.60f;  // charger CV target
const float V_EMPTY_CUTOFF        = 2.50f;  // ~0% SoC (checked at rest only)
const float FULL_TAPER_CURRENT_MA = 150.0f;
const float FULL_TAPER_HOLD_S     = 60.0f;

// "At rest" for this node means "no valve pulse". The ESP32 + LoRa in RX
// already draw ~50-90 mA continuously, so the logger's 30 mA threshold
// would never be met here and the boot OCV sanity check would never run.
const float RESTING_CURRENT_MA = 150.0f;

const float OCV_MISMATCH_THRESHOLD_PERCENT = 25.0f;

// Sign convention used below: + = discharging, - = charging.
// -1.0 matches the fix in the battery logger (shunt reads reversed).
// If SoC climbs while the node is just idling on battery, set this to +1.0.
const float CURRENT_SIGN = -1.0f;

const char* BAT_NVS_NS = "socdata";
const float BAT_SAVE_THRESHOLD_PERCENT = 1.0f;
const unsigned long BAT_SAVE_INTERVAL_MS  = 5UL * 60UL * 1000UL;
const unsigned long BAT_SAMPLE_MS         = 1000;   // idle sampling
const unsigned long PULSE_SAMPLE_MS       = 10;     // sampling during a valve pulse
const unsigned long BAT_PRINT_MS          = 30000;

float remainingCapacityMAh = NAN;
float lastCurrent_mA       = 0.0f;
float lastBatteryV         = 0.0f;
unsigned long lastBatSampleMs = 0;
unsigned long lastBatSaveMs   = 0;
unsigned long lastBatPrintMs  = 0;
float lastSavedSoC         = -1.0f;
float fullTaperTimerS      = 0.0f;

struct OcvPoint { float v; float soc; };
const OcvPoint ocvTable[] = {
    {3.65, 100.0}, {3.45, 99.0}, {3.35, 95.0}, {3.30, 90.0}, {3.28, 80.0},
    {3.26,  70.0}, {3.25, 60.0}, {3.23, 50.0}, {3.21, 40.0}, {3.18, 30.0},
    {3.15,  20.0}, {3.10, 15.0}, {3.00, 10.0}, {2.80,  5.0}, {2.50,  0.0}
};
const int OCV_TABLE_SIZE = sizeof(ocvTable) / sizeof(ocvTable[0]);

float ocvToSoC(float v) {
    if (v >= ocvTable[0].v) return 100.0f;
    if (v <= ocvTable[OCV_TABLE_SIZE - 1].v) return 0.0f;
    for (int i = 0; i < OCV_TABLE_SIZE - 1; i++) {
        float vHigh = ocvTable[i].v, vLow = ocvTable[i + 1].v;
        if (v <= vHigh && v >= vLow) {
            float frac = (v - vLow) / (vHigh - vLow);
            return ocvTable[i + 1].soc + frac * (ocvTable[i].soc - ocvTable[i + 1].soc);
        }
    }
    return 50.0f;
}

float batterySoC() {
    if (!inaOk || isnan(remainingCapacityMAh)) return -1.0f;
    return (remainingCapacityMAh / NOMINAL_CAPACITY_MAH) * 100.0f;
}

void saveBattery() {
    if (!inaOk || isnan(remainingCapacityMAh)) return;
    Preferences p;
    p.begin(BAT_NVS_NS, false);
    p.putFloat("remMAh", remainingCapacityMAh);
    p.end();
    lastSavedSoC  = batterySoC();
    lastBatSaveMs = millis();
}

// One Coulomb-counting step. Safe to call at any rate; integrates over the
// real elapsed time since the previous call (trapezoidal).
void updateBattery() {
    if (!inaOk) return;

    unsigned long now = millis();
    float dtH = (now - lastBatSampleMs) / 3600000.0f;
    lastBatSampleMs = now;

    float v = ina219.getBusVoltage_V();
    float i = CURRENT_SIGN * ina219.getCurrent_mA();   // + discharge, - charge

    float avg = (i + lastCurrent_mA) * 0.5f;
    remainingCapacityMAh -= avg * dtH;
    lastCurrent_mA = i;
    lastBatteryV   = v;

    if (remainingCapacityMAh > NOMINAL_CAPACITY_MAH) remainingCapacityMAh = NOMINAL_CAPACITY_MAH;
    if (remainingCapacityMAh < 0) remainingCapacityMAh = 0;

    // Empty snap — only when not pulsing a valve, otherwise the pulse sag
    // could read below cutoff on a perfectly healthy battery.
    bool atRest = fabsf(i) < RESTING_CURRENT_MA;
    if (atRest && v <= V_EMPTY_CUTOFF) remainingCapacityMAh = 0.0f;

    // Full snap — CV voltage reached and charge current tapered off.
    bool nearFull = v >= V_FULL_CV;
    bool tapered  = (i < 0) && (fabsf(i) < FULL_TAPER_CURRENT_MA);
    if (nearFull && tapered) {
        fullTaperTimerS += dtH * 3600.0f;
        if (fullTaperTimerS >= FULL_TAPER_HOLD_S) remainingCapacityMAh = NOMINAL_CAPACITY_MAH;
    } else {
        fullTaperTimerS = 0.0f;
    }
}

void maybeSaveBattery() {
    if (!inaOk) return;
    float soc = batterySoC();
    bool changedEnough = fabsf(soc - lastSavedSoC) >= BAT_SAVE_THRESHOLD_PERCENT;
    bool timeElapsed   = (millis() - lastBatSaveMs) >= BAT_SAVE_INTERVAL_MS;
    if (changedEnough || timeElapsed) saveBattery();
}

void initBattery() {
    Wire.begin(I2C_SDA, I2C_SCL);
    if (!ina219.begin(&Wire)) {
        // Don't halt — a valve node must keep working without its sensor.
        Serial.println("⚠️ INA219 not found — battery reporting disabled (bat:-1)");
        inaOk = false;
        return;
    }
    inaOk = true;
    // Default calibration = 32V / 2A range (±3.2A readable with 0.1Ω shunt).
    // Keep it unless the valve pulse can exceed ~3A.

    Preferences p;
    p.begin(BAT_NVS_NS, true);
    bool hasSaved  = p.isKey("remMAh");
    float savedMAh = hasSaved ? p.getFloat("remMAh", NAN) : NAN;
    p.end();

    delay(200);
    float bootV  = ina219.getBusVoltage_V();
    float bootI  = CURRENT_SIGN * ina219.getCurrent_mA();
    float ocvSoC = ocvToSoC(bootV);

    if (hasSaved && !isnan(savedMAh)) {
        float savedSoC = (savedMAh / NOMINAL_CAPACITY_MAH) * 100.0f;
        bool atRest   = fabsf(bootI) < RESTING_CURRENT_MA;
        bool mismatch = fabsf(ocvSoC - savedSoC) > OCV_MISMATCH_THRESHOLD_PERCENT;
        if (atRest && mismatch) {
            remainingCapacityMAh = (ocvSoC / 100.0f) * NOMINAL_CAPACITY_MAH;
            Serial.printf("🔋 Saved SoC %.0f%% conflicts with OCV %.0f%% — reseeding from OCV\n",
                          savedSoC, ocvSoC);
        } else {
            remainingCapacityMAh = savedMAh;
            Serial.printf("🔋 Restored %.0f mAh (%.0f%%) from flash\n", savedMAh, savedSoC);
        }
    } else {
        remainingCapacityMAh = (ocvSoC / 100.0f) * NOMINAL_CAPACITY_MAH;
        Serial.printf("🔋 No saved state — OCV %.2fV -> %.0f%%\n", bootV, ocvSoC);
    }

    lastCurrent_mA  = bootI;
    lastBatteryV    = bootV;
    lastBatSampleMs = millis();
    lastBatSaveMs   = millis();
    lastSavedSoC    = batterySoC();
}

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
    if (json.length() > LORA_MAX_BODY) {
        Serial.printf("❌ TX refused — body %u bytes > %d (would be truncated)\n",
                      json.length(), LORA_MAX_BODY);
        return;
    }
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
    StaticJsonDocument<384> ack;
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

    // Battery — take a fresh sample so the value reflects post-pulse state.
    updateBattery();
    float soc = batterySoC();
    ack["bat"] = (soc < 0) ? -1 : (int)lroundf(soc);
    if (inaOk) ack["bv"] = serialized(String(lastBatteryV, 2));

    String out;
    serializeJson(ack, out);

    // Keep room for a relay's "via" field. "bv" is optional — drop it first.
    if (out.length() > LORA_MAX_BODY - RELAY_HEADROOM && ack.containsKey("bv")) {
        ack.remove("bv");
        out = "";
        serializeJson(ack, out);
    }
    if (out.length() > LORA_MAX_BODY - RELAY_HEADROOM) {
        Serial.printf("⚠️ ACK is %u bytes — may not survive relaying\n", out.length());
    }

    Serial.printf("⏳ ACK pre-delay %dms...\n", ACK_PRE_DELAY_MS);
    delay(ACK_PRE_DELAY_MS);
    for (int i = 0; i < ACK_REPEATS; i++) sendRaw(out);
    Serial.printf("📡 ACK sent (%u B): %s\n", out.length(), out.c_str());
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
//  SECTION 12 — VALVE DRIVERS  (TB6612FNG, PWM + STBY hardwired HIGH)
//  The pulse is metered: INA219 sampled every PULSE_SAMPLE_MS so the
//  solenoid current is counted instead of falling between 1 s samples.
// =============================================================================

void actuateValve(uint8_t inPos, uint8_t inNeg,
                  bool energizePositive, const char* label, const char* action,
                  const char* fid, const char* bid, uint32_t msgId) {
    digitalWrite(inPos, LOW);
    digitalWrite(inNeg, LOW);
    delay(SAFE_MS);

    Serial.printf("%s Valve %s\n", energizePositive ? "🔓 Opening" : "🔒 Closing", label);

    updateBattery();   // close the idle interval right before the pulse

    digitalWrite(inPos, energizePositive ? HIGH : LOW);
    digitalWrite(inNeg, energizePositive ? LOW  : HIGH);

    float peak_mA = 0.0f;
    unsigned long t0 = millis();
    while (millis() - t0 < PULSE_MS) {
        if (inaOk && (millis() - lastBatSampleMs) >= PULSE_SAMPLE_MS) {
            updateBattery();
            if (lastCurrent_mA > peak_mA) peak_mA = lastCurrent_mA;
        }
        delay(1);
    }

    digitalWrite(inPos, LOW);
    digitalWrite(inNeg, LOW);   // both LOW = TB6612 stop (outputs OFF)

    updateBattery();   // close the pulse interval
    if (inaOk) Serial.printf("⚡ Pulse peak %.0f mA | SoC %.1f%%\n", peak_mA, batterySoC());

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

    // OTA reboots on success — persist SoC so the count isn't lost.
    updateBattery();
    saveBattery();

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
//  SECTION 15 — SAFE STARTUP  (all direction pins LOW before anything else)
// =============================================================================

void safeStartupPins() {
    pinMode(AIN1, OUTPUT); pinMode(AIN2, OUTPUT);
    pinMode(BIN1, OUTPUT); pinMode(BIN2, OUTPUT);

    digitalWrite(AIN1, LOW); digitalWrite(AIN2, LOW);
    digitalWrite(BIN1, LOW); digitalWrite(BIN2, LOW);

    Serial.println("🔒 Safe startup — all TB6612 direction pins LOW (PWMA/PWMB/STBY hardwired HIGH)");
}

// =============================================================================
//  SECTION 16 — SETUP
// =============================================================================

void setup() {
    safeStartupPins();          // first — before Serial, before anything
    Serial.begin(115200);

    for (int i = 0; i < SEEN_CACHE_SIZE; i++) seenCache[i] = { 0, 0, false };
    for (int i = 0; i < RX_QUEUE_SIZE; i++)   rxQueue[i].used = false;
    loadValveState();
    randomSeed(esp_random());

    Serial.println("\n🚀 Valve Node (TB6612FNG + INA219) starting...");
    Serial.printf("📛 VId:%d  DId:%s\n", MY_VID, MY_DID);

    initBattery();

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

    // Battery Coulomb counting — non-blocking, once per second
    if (inaOk && (millis() - lastBatSampleMs) >= BAT_SAMPLE_MS) {
        updateBattery();
        maybeSaveBattery();
    }
    if (inaOk && (millis() - lastBatPrintMs) >= BAT_PRINT_MS) {
        lastBatPrintMs = millis();
        Serial.printf("🔋 V:%.3fV I:%.1fmA Rem:%.0f/%.0fmAh SoC:%.1f%%\n",
                      lastBatteryV, lastCurrent_mA, remainingCapacityMAh,
                      NOMINAL_CAPACITY_MAH, batterySoC());
    }

    delay(5);
}
