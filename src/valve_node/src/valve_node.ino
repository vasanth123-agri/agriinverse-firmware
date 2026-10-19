// =============================================================================
//  AgriInverse — Solenoid Valve Node  (v4.4 — accurate INA219 battery gauge)
//
//  CHANGES FROM v4.3  (battery measurement only — radio/mesh/valve logic
//  and the ack format are unchanged, so the gateway needs no update)
//  -----------------------------------------------------------------
//  1. Own INA219 register driver (Adafruit_INA219 removed).
//     - 128-sample averaging on shunt AND bus ADC: rejects the 12 V boost
//       converter's switching ripple, which aliased into the old 12-bit
//       single-sample readings as tens of mA of random error.
//     - Current is computed from the SHUNT VOLTAGE register (10 uV/LSB)
//       / R_SHUNT, not the CURRENT register. It does not depend on the
//       volatile calibration register, so a sensor brown-out can never
//       silently zero the current.
//     - Cell voltage is corrected for the shunt drop (bus + shunt), so
//       "bv" is the real cell voltage, not cell minus I*R.
//     - One setting (CELL_ON_VIN_PLUS) picks the wiring; the sign follows.
//     - Config register is checked every 10 s and rewritten if it was lost;
//       5 failed reads -> INA marked missing, retried every 30 s.
//  2. Valve pulse metered in a FAST mode (8-sample avg, ~4.3 ms) sampled
//     every 5 ms plus a 60 ms tail, then back to 128-sample slow mode.
//     Shunt clipping (pulse current above the INA219 range) is detected
//     and reported on serial.
//  3. Idle sampling every 250 ms instead of 1 s.
//  4. Self-calibrating gauge (same logic as the AgriNode TX gauge):
//     - 100 % anchored when V >= FULL_V and charge current tapered, 120 s
//     - 0 %   anchored when loaded V <= EMPTY_V at rest, 30 s
//     - real capacity LEARNED from one full -> empty run
//     - charge efficiency applied to charge current
//     - displayed % never rises while discharging / falls while charging,
//       moves max 1 %/min, holds 99 % until really full, 1 % until empty
//     - "~" on serial = not yet confirmed by a full or empty anchor
//  5. Boot OCV sanity check no longer runs while CHARGING (charging lifts
//     LiFePO4 voltage and used to reseed SoC to ~99 % on a daytime reboot).
//  6. NVS: v4.3's saved "remMAh" is migrated automatically.
//
//  Ack fields unchanged:
//     "bat" displayed SoC % (-1 if INA219 missing)
//     "ib"  battery current mA, smoothed (+ charging, - discharging)
//     "cs"  0 discharging, 1 charging, 2 full, 3 idle
//     "bv"  cell voltage (dropped automatically if the packet is too long)
//
//  Pins used:
//    LoRa  : SS 5, RST 14, DIO0 26 (+ VSPI 18/19/23)
//    Valve : AIN1 16, AIN2 17, BIN1 27, BIN2 32
//    INA219: SDA 21, SCL 22
//    TB6612: PWMA, PWMB, STBY -> 3.3V (hardwired), VM -> 12V boost
// =============================================================================

#include <SPI.h>
#include <LoRa.h>
#include <ArduinoJson.h>
#include <Preferences.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>
#include <Wire.h>

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

#define MY_VID   11                                  // 1..15, unique per node
#define MY_DID   "DId11"                             // human-readable label
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
//  inputs are firmly LOW while the ESP32 is booting.
// =============================================================================

// Valve A
#define AIN1  16
#define AIN2  17

// Valve B
#define BIN1  27
#define BIN2  32

#define PULSE_MS 300
#define SAFE_MS  150

enum ValveState { VS_UNKNOWN = 0, VS_OPEN = 1, VS_CLOSED = 2 };
ValveState valveStateA = VS_UNKNOWN;
ValveState valveStateB = VS_UNKNOWN;

// =============================================================================
//  SECTION 3B — BATTERY MONITOR (INA219 direct driver + self-calibrating gauge)
//
//  Wiring (high-side, CELL_ON_VIN_PLUS = 1):
//    battery+ -> INA219 VIN+ ; INA219 VIN- -> node supply bus
//    (12V boost IN + 3.3V regulator IN + solar charger OUT).
//    Battery- -> common GND.  INA219 VCC -> 3.3V, SDA 21, SCL 22.
//  Everything except the cell itself must be on the VIN- side, otherwise
//  charge current bypasses the shunt.
//
//  If your board is wired the other way (load/charger on VIN+, cell+ on
//  VIN-, like the AgriNode TX), set CELL_ON_VIN_PLUS to 0. Nothing else
//  changes — the current sign and the voltage correction follow.
//
//  Check: at night with no valve activity the serial line must show
//  DISCHARGING and I around -50..-90 mA. If it shows CHARGING, the
//  CELL_ON_VIN_PLUS setting doesn't match the wiring.
// =============================================================================

#define I2C_SDA 21
#define I2C_SCL 22

#define CELL_ON_VIN_PLUS   1        // 1 = cell on VIN+ (v4.3 wiring), 0 = cell on VIN-
#define INA_DEBUG          0        // 1 = print raw INA219 registers every 30 s

const float R_SHUNT_OHM       = 0.100f;  // R100 on the usual breakout
const float CURRENT_GAIN_TRIM = 1.000f;  // optional: (meter mA / shown mA)
const float SHUNT_OFFSET_MV   = 0.000f;  // optional: shunt mV shown with 0 A flowing

#define INA_ADDR        0x40       // A0 = A1 = GND
#define INA_REG_CONFIG  0x00
#define INA_REG_SHUNTV  0x01
#define INA_REG_BUSV    0x02

// CONFIG = BRNG | PG | BADC | SADC | MODE
//  SLOW: 16 V bus, PGA /8 (±320 mV = ±3.2 A on 0.1R), 128-sample average on
//        shunt and bus (68.1 ms each, ~136 ms per pair), shunt+bus continuous.
//  FAST: same range, shunt 8-sample average (4.26 ms), bus 9-bit (84 us).
#define INA_CFG_SLOW         0x1FFF
#define INA_CFG_FAST         0x185F
#define INA_SLOW_SETTLE_MS   150
#define INA_FAST_SETTLE_MS   6
#define SHUNT_CLIP_RAW       31900      // full scale is ±32000 (±320 mV)

bool     inaOk          = false;
bool     gaugeReady     = false;
bool     wireStarted    = false;
uint16_t inaCfgNow      = INA_CFG_SLOW;
uint8_t  inaFailCount   = 0;
bool     shuntClipped   = false;
int16_t  lastShuntRaw   = 0;
uint16_t lastBusRaw     = 0;

// --- Pack (single-cell LiFePO4). Capacity is only the starting value;
//     the real capacity is learned after one full -> empty run. ---
const float NOMINAL_CAPACITY_MAH = 6000.0f;
const float FULL_V               = 3.45f;  // full above this at low current
const float EMPTY_V              = 2.90f;  // 0 % under (resting) load
const float FULL_TAPER_MA        = 150.0f; // |I| below this at FULL_V = tapered
const float FULL_HOLD_S          = 120.0f;
const float EMPTY_HOLD_S         = 30.0f;
const float CHARGE_EFFICIENCY    = 0.99f;

// "At rest" = no valve pulse. ESP32 + LoRa RX draw ~50-90 mA continuously.
const float RESTING_CURRENT_MA   = 150.0f;
const float OCV_MISMATCH_THRESHOLD_PERCENT = 25.0f;

const char*    BAT_NVS_NS = "socdata";
const uint32_t GAUGE_VER  = 0xBA770044UL;  // change -> gauge restarts clean

const unsigned long BAT_SAVE_INTERVAL_MS = 5UL * 60UL * 1000UL;
const unsigned long BAT_SAMPLE_MS        = 250;     // idle sampling
const unsigned long PULSE_SAMPLE_MS      = 5;       // during a valve pulse
const unsigned long POST_PULSE_MS        = 60;      // boost recovery tail
const unsigned long DISP_UPDATE_MS       = 60000;   // displayed % step period
const unsigned long INA_HEALTH_MS        = 10000;
const unsigned long INA_RETRY_MS         = 30000;
const unsigned long BAT_PRINT_MS         = 30000;

#define FL_SEEN_FULL    0x01UL   // full seen since last empty
#define FL_ANCHORED     0x02UL   // ever confirmed full/empty
#define FL_CAP_LEARNED  0x04UL   // capacity measured at least once

float    g_remain   = NAN;                    // mAh left
float    g_cap      = NOMINAL_CAPACITY_MAH;   // mAh (learned)
float    g_dis      = 0.0f;                   // net mAh out since last full
uint32_t g_flags    = 0;
int      g_disp     = -1;                     // displayed %
bool     g_fullNow  = false;
bool     g_emptyNow = false;
float    fullTimerS  = 0.0f;
float    emptyTimerS = 0.0f;

float lastCurrent_mA = 0.0f;   // internal sign: + discharge, - charge
float lastBatteryV   = 0.0f;   // cell voltage (shunt drop corrected)
unsigned long lastBatSampleMs = 0;
unsigned long lastBatSaveMs   = 0;
unsigned long lastBatPrintMs  = 0;
unsigned long lastDispMs      = 0;
unsigned long lastHealthMs    = 0;
unsigned long lastRetryMs     = 0;
int lastSavedDisp = -1;

// --- Charge status + reported current ---
const float CHG_THRESHOLD_MA = 20.0f;   // |I| below this = idle
const float IB_FILTER_ALPHA  = 0.05f;   // EMA on 250 ms samples (~5 s)
float ibFiltered_mA = 0.0f;             // internal sign: + discharge, - charge

enum ChargeState { CS_DISCHARGING = 0, CS_CHARGING = 1, CS_FULL = 2, CS_IDLE = 3 };

// LiFePO4 resting voltage -> % (first guess / sanity check only)
const float OCV_V[]   = { 2.50f, 2.90f, 3.00f, 3.10f, 3.20f, 3.22f,
                          3.25f, 3.26f, 3.27f, 3.28f, 3.30f, 3.32f,
                          3.33f, 3.35f, 3.40f };
const float OCV_PCT[] = {   0.f,   5.f,   9.f,  14.f,  17.f,  20.f,
                           30.f,  40.f,  50.f,  60.f,  70.f,  80.f,
                           90.f,  99.f, 100.f };
const int OCV_N = sizeof(OCV_V) / sizeof(OCV_V[0]);

float ocvToSoC(float v) {
    if (v <= OCV_V[0])         return 0.0f;
    if (v >= OCV_V[OCV_N - 1]) return 100.0f;
    for (int i = 1; i < OCV_N; i++) {
        if (v <= OCV_V[i]) {
            float t = (v - OCV_V[i - 1]) / (OCV_V[i] - OCV_V[i - 1]);
            return OCV_PCT[i - 1] + t * (OCV_PCT[i] - OCV_PCT[i - 1]);
        }
    }
    return 100.0f;
}

// ---------------- INA219 low level ----------------

bool inaWrite(uint8_t reg, uint16_t val) {
    Wire.beginTransmission(INA_ADDR);
    Wire.write(reg);
    Wire.write((uint8_t)(val >> 8));
    Wire.write((uint8_t)(val & 0xFF));
    return Wire.endTransmission() == 0;
}

bool inaRead(uint8_t reg, uint16_t& out) {
    Wire.beginTransmission(INA_ADDR);
    Wire.write(reg);
    if (Wire.endTransmission() != 0) return false;
    if (Wire.requestFrom((uint8_t)INA_ADDR, (uint8_t)2) != 2) return false;
    uint8_t hi = Wire.read();
    uint8_t lo = Wire.read();
    out = ((uint16_t)hi << 8) | lo;
    return true;
}

// Write CONFIG and verify by read-back (also proves the chip is there).
bool inaSetConfig(uint16_t cfg) {
    if (!inaWrite(INA_REG_CONFIG, cfg)) return false;
    uint16_t rb = 0;
    if (!inaRead(INA_REG_CONFIG, rb) || rb != cfg) return false;
    inaCfgNow = cfg;
    return true;
}

void inaUseFast() {
    if (!inaOk) return;
    if (inaSetConfig(INA_CFG_FAST)) delay(INA_FAST_SETTLE_MS);
}

void inaUseSlow() {
    if (!inaOk) return;
    inaSetConfig(INA_CFG_SLOW);
}

// One reading. vCell = real cell voltage, iMa: + discharging, - charging.
bool inaSample(float& vCell, float& iMa) {
    uint16_t sh = 0, bus = 0;
    if (!inaRead(INA_REG_SHUNTV, sh) || !inaRead(INA_REG_BUSV, bus)) {
        if (++inaFailCount >= 5) {
            inaOk = false;
            Serial.println("⚠️ INA219 stopped responding — battery reporting disabled (bat:-1)");
        }
        return false;
    }
    inaFailCount = 0;

    int16_t s = (int16_t)sh;
    lastShuntRaw = s;
    lastBusRaw   = bus;
    if (s >= SHUNT_CLIP_RAW || s <= -SHUNT_CLIP_RAW) shuntClipped = true;

    float vsh_mV = (float)s * 0.01f - SHUNT_OFFSET_MV;          // 10 uV / LSB
    float vbus   = (float)(bus >> 3) * 0.004f;                  // 4 mV / LSB
    float i      = (vsh_mV / R_SHUNT_OHM) * CURRENT_GAIN_TRIM;  // mV / ohm = mA

#if CELL_ON_VIN_PLUS
    // Positive shunt = VIN+ -> VIN- = cell -> load = discharging.
    // Bus is measured at VIN-, so add the shunt drop back for the cell.
    iMa   = i;
    vCell = vbus + vsh_mV / 1000.0f;
#else
    // Positive shunt = load/charger node -> cell = charging.
    // Bus is measured at VIN- = cell, already the true cell voltage.
    iMa   = -i;
    vCell = vbus;
#endif
    return true;
}

// ---------------- Gauge ----------------

int batteryPct() {
    return (inaOk && gaugeReady && g_disp >= 0) ? g_disp : -1;
}

float batterySoCRaw() {
    if (!gaugeReady || isnan(g_remain)) return -1.0f;
    return g_remain / g_cap * 100.0f;
}

ChargeState chargeState() {
    float i = ibFiltered_mA;   // + discharge, - charge
    if (g_disp >= 99 && lastBatteryV >= FULL_V &&
        fabsf(i) < FULL_TAPER_MA && i < CHG_THRESHOLD_MA) return CS_FULL;
    if (i < -CHG_THRESHOLD_MA) return CS_CHARGING;
    if (i >  CHG_THRESHOLD_MA) return CS_DISCHARGING;
    return CS_IDLE;
}

void saveBattery() {
    if (!gaugeReady || isnan(g_remain)) return;
    Preferences p;
    p.begin(BAT_NVS_NS, false);
    p.putUInt ("gv",   GAUGE_VER);
    p.putFloat("rem",  g_remain);
    p.putFloat("cap",  g_cap);
    p.putFloat("dis",  g_dis);
    p.putUInt ("flg",  g_flags);
    p.putInt  ("disp", g_disp);
    p.end();
    lastSavedDisp = g_disp;
    lastBatSaveMs = millis();
}

void maybeSaveBattery() {
    if (!gaugeReady) return;
    bool changed     = (g_disp != lastSavedDisp);
    bool timeElapsed = (millis() - lastBatSaveMs) >= BAT_SAVE_INTERVAL_MS;
    if (changed || timeElapsed) saveBattery();
}

// Phone-style displayed percentage. force = jump straight to target.
void updateDisplayPct(bool force) {
    if (!gaugeReady || isnan(g_remain)) return;

    int target = (int)lroundf(g_remain / g_cap * 100.0f);
    if (target >= 100 && !(g_fullNow || (g_disp == 100 && g_remain >= 0.99f * g_cap)))
        target = 99;                      // 100 % only when really full
    if (target <= 0 && !g_emptyNow)
        target = 1;                       // 0 % only at real cutoff

    if (force || g_disp < 0) {
        g_disp = target;
    } else {
        int step = target - g_disp;
        if (ibFiltered_mA >  CHG_THRESHOLD_MA && step > 0) step = 0;  // discharging: never up
        if (ibFiltered_mA < -CHG_THRESHOLD_MA && step < 0) step = 0;  // charging: never down
        if (step >  1) step =  1;
        if (step < -1) step = -1;
        g_disp += step;
    }
    g_disp = constrain(g_disp, 0, 100);
}

// One Coulomb-counting step. Integrates over the real elapsed time since
// the previous call (trapezoidal). Safe to call at any rate.
void updateBattery() {
    if (!inaOk) return;

    float v, i;
    if (!inaSample(v, i)) return;

    unsigned long now = millis();
    float dtS = (now - lastBatSampleMs) / 1000.0f;
    lastBatSampleMs = now;

    float avg = 0.5f * (i + lastCurrent_mA);
    lastCurrent_mA = i;
    lastBatteryV   = v;

    if (!gaugeReady) return;

    float dq = avg * dtS / 3600.0f;            // mAh, + = out of the cell
    if (dq >= 0.0f) {
        g_remain -= dq;
        g_dis    += dq;
    } else {
        g_remain -= dq * CHARGE_EFFICIENCY;    // adds charge
        g_dis    += dq * CHARGE_EFFICIENCY;
        if (g_dis < 0.0f) g_dis = 0.0f;
    }
    if (g_remain > g_cap) g_remain = g_cap;
    if (g_remain < 0.0f)  g_remain = 0.0f;

    // Anchors only from slow (averaged) samples, never during a pulse.
    if (inaCfgNow != INA_CFG_SLOW) return;

    // ---- 100 %: CV voltage reached and current tapered off ----
    bool fullCond = (v >= FULL_V) && (fabsf(i) < FULL_TAPER_MA) && (i < CHG_THRESHOLD_MA);
    if (fullCond) {
        fullTimerS += dtS;
        if (fullTimerS >= FULL_HOLD_S) {
            if (!g_fullNow) {
                g_fullNow = true;
                g_remain  = g_cap;
                g_dis     = 0.0f;
                g_flags  |= FL_SEEN_FULL | FL_ANCHORED;
                Serial.println("🔋 Full anchor — gauge set to 100 %");
                saveBattery();
            }
            g_remain = g_cap;
            g_dis    = 0.0f;
        }
    } else {
        fullTimerS = 0.0f;
        g_fullNow  = false;
    }

    // ---- 0 %: cutoff voltage under resting load; learn capacity ----
    bool emptyCond = (v <= EMPTY_V) && (i > 0.0f) && (i < RESTING_CURRENT_MA);
    if (emptyCond) {
        emptyTimerS += dtS;
        if (emptyTimerS >= EMPTY_HOLD_S) {
            if (!g_emptyNow) {
                g_emptyNow = true;
                if ((g_flags & FL_SEEN_FULL) && g_dis > 0.25f * g_cap) {
                    float learned = g_dis;
                    if (g_flags & FL_CAP_LEARNED) g_cap = 0.7f * g_cap + 0.3f * learned;
                    else                          g_cap = learned;
                    g_cap = constrain(g_cap, 0.25f * NOMINAL_CAPACITY_MAH,
                                             4.00f * NOMINAL_CAPACITY_MAH);
                    g_flags |= FL_CAP_LEARNED;
                    Serial.printf("🔋 Capacity learned: %.0f mAh\n", g_cap);
                }
                g_flags &= ~FL_SEEN_FULL;
                g_flags |= FL_ANCHORED;
                Serial.println("🔋 Empty anchor — gauge set to 0 %");
                g_remain = 0.0f;
                saveBattery();
            }
            g_remain = 0.0f;
        }
    } else {
        emptyTimerS = 0.0f;
        g_emptyNow  = false;
    }
}

// Load saved state (or seed from OCV). Called once, on the first
// successful INA219 start.
void gaugeRestore(float vBoot, float iBoot) {
    Preferences p;
    p.begin(BAT_NVS_NS, true);
    bool hasNew = (p.getUInt("gv", 0) == GAUGE_VER);
    if (hasNew) {
        g_remain = p.getFloat("rem", NAN);
        g_cap    = p.getFloat("cap", NOMINAL_CAPACITY_MAH);
        g_dis    = p.getFloat("dis", 0.0f);
        g_flags  = p.getUInt ("flg", 0);
        g_disp   = p.getInt  ("disp", -1);
    } else if (p.isKey("remMAh")) {                // migrate v4.3 state
        g_remain = p.getFloat("remMAh", NAN);
        g_cap    = NOMINAL_CAPACITY_MAH;
        g_dis    = 0.0f;
        g_flags  = 0;
        g_disp   = -1;
        Serial.println("🔋 Migrated v4.3 battery state");
    }
    p.end();

    if (!(g_cap > 0.25f * NOMINAL_CAPACITY_MAH && g_cap < 4.0f * NOMINAL_CAPACITY_MAH))
        g_cap = NOMINAL_CAPACITY_MAH;
    if (!(g_dis >= 0.0f && g_dis < 1.0e6f)) g_dis = 0.0f;
    if (!(g_remain >= 0.0f && g_remain <= g_cap)) g_remain = NAN;
    if (g_disp < -1 || g_disp > 100) g_disp = -1;

    float ocvSoC = ocvToSoC(vBoot);

    if (isnan(g_remain)) {
        g_remain = ocvSoC / 100.0f * g_cap;
        g_flags &= ~FL_ANCHORED;
        g_disp   = -1;
        Serial.printf("🔋 No saved state — OCV %.3fV -> %.0f%%\n", vBoot, ocvSoC);
    } else {
        float savedSoC = g_remain / g_cap * 100.0f;
        // Only trust OCV while lightly discharging — charging lifts a
        // LiFePO4 cell's voltage and would read ~99 %.
        bool restingOk = (iBoot > -CHG_THRESHOLD_MA) && (iBoot < RESTING_CURRENT_MA);
        bool mismatch  = fabsf(ocvSoC - savedSoC) > OCV_MISMATCH_THRESHOLD_PERCENT;
        if (restingOk && mismatch) {
            g_remain = ocvSoC / 100.0f * g_cap;
            g_flags &= ~FL_ANCHORED;
            g_disp   = -1;
            Serial.printf("🔋 Saved SoC %.0f%% conflicts with OCV %.0f%% — reseeding from OCV\n",
                          savedSoC, ocvSoC);
        } else {
            Serial.printf("🔋 Restored %.0f/%.0f mAh (%.0f%%) from flash\n",
                          g_remain, g_cap, savedSoC);
        }
    }

    gaugeReady = true;
    updateDisplayPct(g_disp < 0);
    lastSavedDisp = g_disp;
    lastBatSaveMs = millis();
    lastDispMs    = millis();
}

// Start (or restart) the INA219. Safe to call again from loop().
void initBattery() {
    if (!wireStarted) {
        Wire.begin(I2C_SDA, I2C_SCL);
        Wire.setClock(400000);
        wireStarted = true;
    }

    if (!inaSetConfig(INA_CFG_SLOW)) {
        // Don't halt — a valve node must keep working without its sensor.
        if (!gaugeReady) Serial.println("⚠️ INA219 not found — battery reporting disabled (bat:-1)");
        inaOk = false;
        return;
    }
    inaOk        = true;
    inaFailCount = 0;
    delay(INA_SLOW_SETTLE_MS);   // first averaged shunt+bus pair

    float v, i;
    if (!inaSample(v, i)) { inaOk = false; return; }

    lastCurrent_mA  = i;
    lastBatteryV    = v;
    ibFiltered_mA   = i;
    lastBatSampleMs = millis();
    lastHealthMs    = millis();

    if (!gaugeReady) gaugeRestore(v, i);
    Serial.printf("🔋 INA219 OK — V:%.3fV I:%+.1fmA (shunt %.3f ohm, cell on VIN%s)\n",
                  v, -i, R_SHUNT_OHM, CELL_ON_VIN_PLUS ? "+" : "-");
}

// Called every loop() pass — non-blocking except the rare INA219 retry.
void batteryService() {
    unsigned long now = millis();

    if (!inaOk) {
        if (now - lastRetryMs >= INA_RETRY_MS) {
            lastRetryMs = now;
            initBattery();
        }
        return;
    }

    if (now - lastBatSampleMs >= BAT_SAMPLE_MS) {
        updateBattery();
        ibFiltered_mA += IB_FILTER_ALPHA * (lastCurrent_mA - ibFiltered_mA);
    }

    if (now - lastDispMs >= DISP_UPDATE_MS) {
        lastDispMs = now;
        updateDisplayPct(false);
    }

    // Config register lost (brown-out / glitch)? Rewrite it.
    if (now - lastHealthMs >= INA_HEALTH_MS) {
        lastHealthMs = now;
        uint16_t c = 0;
        if (inaRead(INA_REG_CONFIG, c) && c != inaCfgNow) {
            Serial.printf("⚠️ INA219 config was 0x%04X — rewriting 0x%04X\n", c, inaCfgNow);
            inaSetConfig(inaCfgNow);
        }
    }

    maybeSaveBattery();

    if (now - lastBatPrintMs >= BAT_PRINT_MS) {
        lastBatPrintMs = now;
        static const char* csName[] = { "DISCHARGING", "CHARGING", "FULL", "IDLE" };
        Serial.printf("🔋 V:%.3fV I:%+.1fmA Rem:%.0f/%.0fmAh SoC:%d%%%s (raw %.1f%%) %s\n",
                      lastBatteryV, -ibFiltered_mA, g_remain, g_cap, g_disp,
                      (g_flags & FL_ANCHORED) ? "" : "~", batterySoCRaw(),
                      csName[chargeState()]);
#if INA_DEBUG
        uint16_t c = 0;
        inaRead(INA_REG_CONFIG, c);
        Serial.printf("   INA raw: shunt %d (%+.2f mV) bus 0x%04X (%.3f V) cfg 0x%04X\n",
                      lastShuntRaw, lastShuntRaw * 0.01f, lastBusRaw,
                      (lastBusRaw >> 3) * 0.004f, c);
#endif
    }
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

// 16-bit, never 0 (0 means "no msgId" and is dropped by every node).
uint16_t newShortId() {
    uint16_t v;
    do { v = (uint16_t)(esp_random() & 0xFFFF); } while (v == 0);
    return v;
}

void sendAck(const String& status, const char* fid, const char* bid,
            uint32_t replyToMsgId) {
    StaticJsonDocument<384> ack;
    ack["type"]    = "ack";
    ack["UId"]     = MY_UID;
    ack["VId"]     = MY_VID;
    ack["msgId"]   = newShortId();
    ack["replyTo"] = replyToMsgId;
    ack["hop"]     = DEFAULT_HOP_COUNT;
    ack["FId"]     = fid;
    ack["BId"]     = bid;
    ack["from"]    = MY_DID;
    ack["status"]  = status;

    // Battery — fresh sample so V reflects post-pulse state.
    updateBattery();
    ack["bat"] = batteryPct();
    if (inaOk) {
        ack["ib"] = (int)lroundf(-ibFiltered_mA);   // reported: + charging, - discharging
        ack["cs"] = (int)chargeState();             // 0 dis, 1 chg, 2 full, 3 idle
        ack["bv"] = serialized(String(lastBatteryV, 2));
    }

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
    ack["msgId"]   = newShortId();
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
//  The pulse is metered: INA219 switched to fast mode and sampled every
//  PULSE_SAMPLE_MS, including a short tail while the boost recovers.
// =============================================================================

void actuateValve(uint8_t inPos, uint8_t inNeg,
                  bool energizePositive, const char* label, const char* action,
                  const char* fid, const char* bid, uint32_t msgId) {
    digitalWrite(inPos, LOW);
    digitalWrite(inNeg, LOW);
    delay(SAFE_MS);

    Serial.printf("%s Valve %s\n", energizePositive ? "🔓 Opening" : "🔒 Closing", label);

    updateBattery();          // close the idle interval
    inaUseFast();
    updateBattery();          // fresh fast-mode baseline
    shuntClipped = false;

    digitalWrite(inPos, energizePositive ? HIGH : LOW);
    digitalWrite(inNeg, energizePositive ? LOW  : HIGH);

    float peak_mA = 0.0f;
    unsigned long t0 = millis();
    unsigned long lastS = t0;
    while (millis() - t0 < PULSE_MS) {
        if (inaOk && (millis() - lastS) >= PULSE_SAMPLE_MS) {
            lastS = millis();
            updateBattery();
            if (lastCurrent_mA > peak_mA) peak_mA = lastCurrent_mA;
        }
        delay(1);
    }

    digitalWrite(inPos, LOW);
    digitalWrite(inNeg, LOW);   // both LOW = TB6612 stop (outputs OFF)

    // Tail: boost output caps recharge after the coil switches off.
    t0 = millis();
    while (millis() - t0 < POST_PULSE_MS) {
        if (inaOk && (millis() - lastS) >= PULSE_SAMPLE_MS) {
            lastS = millis();
            updateBattery();
        }
        delay(1);
    }
    updateBattery();
    inaUseSlow();

    if (inaOk) {
        Serial.printf("⚡ Pulse peak %.0f mA | SoC %d%%\n", peak_mA, g_disp);
        if (shuntClipped)
            Serial.println("⚠️ Pulse current exceeded the INA219 range (±320 mV) — "
                           "use a smaller shunt or this pulse is under-counted");
    }

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

    // OTA reboots on success — persist the gauge so the count isn't lost.
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

    Serial.println("\n🚀 Valve Node v4.4 (TB6612FNG + INA219 gauge) starting...");
    Serial.printf("📛 VId:%d  DId:%s\n", MY_VID, MY_DID);

    initBattery();
    lastRetryMs = millis();

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

    batteryService();   // 250 ms Coulomb counting, display %, health, save, print

    delay(5);
}
