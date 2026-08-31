// ============================================================
//  Agri Inverse — Fertigation Sump Tank Controller v2.3-LoRa (OTA)
//  MCU      : ESP32 DevKit (38-pin)
//  LoRa     : SX1278 Ra-02  433 MHz
//
//  ARCHITECTURE: Tank-based (batch) fertigation
//  ─────────────────────────────────────────────────────────
//  Sump tank dosed from:
//    - 3 nutrient stock tanks   (N1, N2, N3)
//    - 2 pH correction stocks   (pH Up, pH Down)
//  All 5 dosing actuators = peristaltic pumps on H-bridge.
//  One pH sensor + one EC sensor submerged in sump.
//  One DS18B20 temperature sensor in sump for EC compensation.
//  One HC-SR04 ultrasonic sensor measuring sump water level.
//
//  LORA ADDRESSING
//    This node  : ADDR_SELF = 0x0A
//    Gateway    : ADDR_GW   = 0xBB
//    TX: prepend [ADDR_GW, ADDR_SELF] before JSON body.
//    RX: accept only packets where byte[0] == ADDR_SELF.
//
//  TELEMETRY PACKET (type field absent — routed to handleNodeTelemetry on GW)
//    {"Did":"Fert1","uid":"...","bid":"...","fid":"...",
//     "Ph":6.12,"EC":770,"T":28.4,"wtlvl":42.3,
//     "alert":0,"safe":true,"ready":false,"estop":false}
//    EC in PPM (integer).  wtlvl = cm from sensor to water surface.
//
//  INBOUND COMMAND SCHEMA
//    {"type":"cmd","cmd":"stop"}
//    {"type":"cmd","cmd":"resume"}
//    {"type":"cmd","cmd":"status"}
//    {"type":"cmd","cmd":"set_setpoints","ph":6.2,"ec":2.0}
//    {"type":"cmd","cmd":"set_ratios","n1":1.0,"n2":0.5,"n3":0.5}
//    {"type":"cmd","cmd":"set_failsafe","ph":6.0,"ec":1.0}
//    {"type":"cmd","cmd":"update_profile","ec":2.0,"ph":6.2,
//           "n1":1.0,"n2":0.5,"n3":0.5,"name":"Tomato","ts":1700000000}
//    {"type":"cmd","cmd":"cal_capture","sensor":"ph","point":1}
//    {"type":"cmd","cmd":"cal_capture","sensor":"ec","point":2}
//    {"type":"cmd","cmd":"cal_confirm","sensor":"ph"}
//    {"type":"cmd","cmd":"cal_confirm","sensor":"ec"}
//    {"type":"cmd","cmd":"set_pi","ec_kp":1.2,"ec_ki":0.06,
//           "ph_kp":0.8,"ph_ki":0.03,"ph_db":0.15,"ic":8.0,
//           "mn_p":200,"mx_p":5000,"ph_off":45000}
//  MQTT connectivity notification (from gateway, not a cmd):
//    {"type":"mqtt","Did":"Fert1","state":"connected"|"failed"}
//
//  OTA COMMAND (NEW in v2.3) — relayed by the gateway from the
//  cloud's OTA/mode topic when device type == "fertigation" or
//  "fertigation node":
//    {"type":"ota","Did":"Fert1","uid":"32",
//     "url":"https://.../fertigation_node.bin",
//     "wifi":"farm-ap","pass":"secret"}
//  Node joins the given WiFi (separate from its own LoRa link),
//  downloads and flashes itself, and replies over LoRa with an
//  ack. Successful OTA reboots the board directly (no ack sent
//  for success, matching HTTPUpdate's rebootOnUpdate(true)
//  behaviour); failures send {"type":"ack","cmd":"ota","ok":false,...}.
//
//  ACK reply (type="ack" routed to handleNodeAck on GW):
//    {"Did":"Fert1","uid":"...","type":"ack","cmd":"...","ok":true}
//    {"Did":"Fert1","uid":"...","type":"ack","cmd":"...","ok":false,"err":"..."}
//
//  STATUS reply (type="status" routed to handleNodeStatus on GW):
//    Full state document — see status handler below.
//
//  GPIO SUMMARY
//    Peristaltic pumps (H-bridge IN1, IN2 tied GND):
//      PUMP_N1   = 21   Nutrient 1
//      PUMP_N2   = 22   Nutrient 2
//      PUMP_N3   = 13   Nutrient 3
//      PUMP_PHUP = 27   pH Up
//      PUMP_PHDN = 25   pH Down
//    LoRa SX1278 : SCK=18 MISO=19 MOSI=23 CS=5 RST=14 DIO0=26
//    Ultrasonic  : TRIG=32  ECHO=33
//    pH ADC      : GPIO 35  (ADC1_CH7, input-only)
//    EC ADC      : GPIO 34  (ADC1_CH6, input-only)
//    DS18B20     : GPIO 15  (add 4.7 kΩ pull-up to 3.3 V)
//
//  CHANGELOG v2.3
//    - NEW: OTA support. Node can be updated remotely via a LoRa
//      "type":"ota" packet forwarded by the gateway, joining a
//      separate WiFi network to download the new firmware. Same
//      redirect-following / retry pattern proven on the gateway
//      and valve node firmware.
//  CHANGELOG v2.2 (inherited)
//    - FIX CRITICAL (pH): ADC conversion now uses /4096.0 * 3300.0
//      matching DFRobot's own ESP32 reference code exactly.
//    - FIX CRITICAL (pH): Simple averaging replaced with median filter
//      (getMedianNum) for pH ADC sampling.
//    - FIX CRITICAL (pH): readRawAvg() pH path now uses the same
//      median filter for calibration capture.
//    - FIX MEDIUM (pH): Uncalibrated pH fallback now uses DFRobot's
//      default slope=3.5, intercept=0.0.
//    - FIX MEDIUM (setup): analogReadResolution(12) and
//      analogSetAttenuation(ADC_11db) added explicitly in setup().
// ============================================================

#include <Arduino.h>
#include <SPI.h>
#include <Preferences.h>
#include <LoRa.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <math.h>   // isnan()
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPUpdate.h>

// ============================================================
//  SECTION 1 — IDENTITY & PIN DEFINES
// ============================================================

#define DEVICE_ID  "Fert1"
#define UNIT_ID    "32"
#define BOARD_ID   "dufggcgirfighvcehvceh"
#define FARM_ID    "cveghvjhvejhvjehvjehv"

// LoRa addressing — must match gateway definitions exactly
#define ADDR_SELF  0x0A
#define ADDR_GW    0xBB

// Peristaltic pump GPIOs (H-bridge, single direction)
#define PUMP_N1    21
#define PUMP_N2    22
#define PUMP_N3    13
#define PUMP_PHUP  27
#define PUMP_PHDN  25

// LoRa SX1278
#define LORA_SCK   18
#define LORA_MISO  19
#define LORA_MOSI  23
#define LORA_CS     5
#define LORA_RST   14
#define LORA_DIO0  26
#define LORA_FREQ  433E6

// RF parameters — all must match gateway v1.4 exactly
#define LORA_SF        9
#define LORA_BW        125E3
#define LORA_CR        5
#define LORA_SYNCWORD  0xF3
#define LORA_TXPOWER   17
#define LORA_PREAMBLE  8

// Ultrasonic (sump level only)
#define US_TRIG_SUMP  32
#define US_ECHO_SUMP  33

// Analog sensors — ADC1 input-only pins (safe with LoRa SPI active)
#define ADC_PH_PIN  35
#define ADC_EC_PIN  34

// DS18B20
#define ONE_WIRE_PIN  15

#define PH_ADC_VREF    3300.0f
#define PH_ADC_RES     4096.0f
#define PH_SCOUNT      30
#define PH_CAL_ROUNDS  5

#define TDS_ADC_SAMPLES  20
#define VREF             3.3f
#define ADC_RESOLUTION   4095.0f
#define VREF_EC_FACTOR   2.0f
#define EC_TO_PPM_FACTOR 0.5f

#define TEMP_DEFAULT  25.0f

#define PH_MIN_SAFE  4.5f
#define PH_MAX_SAFE  8.5f
#define EC_MAX_SAFE  5.5f

#define EC_INPUT_MIN  0.5f
#define EC_INPUT_MAX  4.0f
#define PH_INPUT_MIN  5.0f
#define PH_INPUT_MAX  8.0f

#define EC_TOL  0.10f
#define PH_TOL  0.10f

#define ALERT_NONE           0
#define ALERT_JSON_ERROR     1
#define ALERT_FETCH_TIMEOUT  2
#define ALERT_SAFETY         3
#define ALERT_TANK_LOW       4

#define PI_DEF_EC_KP          1.2f
#define PI_DEF_EC_KI          0.0f
#define PI_DEF_PH_KP          0.8f
#define PI_DEF_PH_KI          0.0f
#define PI_DEF_PH_DEADBAND    0.05f
#define PI_DEF_INTEGRAL_CLAMP 1.0f

#define PI_DEF_MIN_PULSE      5000UL
#define PI_DEF_MAX_PULSE     30000UL

#define PI_DEF_MIN_PULSE_PH   6000UL
#define PI_DEF_MAX_PULSE_PH  30000UL
#define PI_DEF_PH_MIN_OFF_MS 180000UL

#define EC_CTRL_MS   30000UL
#define TELE_INT     60000UL
#define MAX_PENDING  2

// ============================================================
//  SECTION 2 — STRUCTS
// ============================================================

struct SensorCal {
  float   slope;
  float   offset;
  float   raw1;
  float   raw2;
  bool    valid;
};

struct CropProfile {
  float    target_ec;
  float    target_ph;
  float    n1_ratio;
  float    n2_ratio;
  float    n3_ratio;
  uint32_t timestamp;
  char     name[24];
  bool     valid;
};

// ============================================================
//  SECTION 3 — GLOBAL VARIABLES
// ============================================================

Preferences prefs;

#define CAL_PH       0
#define CAL_EC       1
#define NUM_SENSORS  2
const float CAL_REF_PH[2] = { 4.00f, 9.10f   };
const float CAL_REF_EC[2] = { 0.0f,  1413.0f };

SensorCal sensorCal[NUM_SENSORS];

float calRawCapture[NUM_SENSORS][2];
bool  calPtDone[NUM_SENSORS][2];

CropProfile cropProfile     = { 1.5f, 6.0f, 1.0f, 1.0f, 1.0f, 0, "DEFAULT",  true  };
CropProfile failsafeProfile = { 1.0f, 6.0f, 1.0f, 1.0f, 1.0f, 0, "FAILSAFE", true  };
CropProfile pendingUpdates[MAX_PENDING];
uint8_t     pendingUpdateCount = 0;

float    target_ec   = 1.5f;
float    target_ph   = 6.0f;
float    n1_ratio    = 1.0f;
float    n2_ratio    = 1.0f;
float    n3_ratio    = 1.0f;

float    ec_kp           = PI_DEF_EC_KP;
float    ec_ki           = PI_DEF_EC_KI;
float    ph_kp           = PI_DEF_PH_KP;
float    ph_ki           = PI_DEF_PH_KI;
float    ph_deadband     = PI_DEF_PH_DEADBAND;
float    integral_clamp  = PI_DEF_INTEGRAL_CLAMP;
uint32_t min_pulse_ms    = PI_DEF_MIN_PULSE;
uint32_t max_pulse_ms    = PI_DEF_MAX_PULSE;
uint32_t min_pulse_ph_ms = PI_DEF_MIN_PULSE_PH;
uint32_t max_pulse_ph_ms = PI_DEF_MAX_PULSE_PH;
uint32_t ph_min_off_ms   = PI_DEF_PH_MIN_OFF_MS;

float    ec_integral  = 0.0f;
float    ph_integral  = 0.0f;
uint32_t lastPHDoseAt = 0;

float    live_ph    = 0.0f;
float    live_ec    = 0.0f;
uint16_t live_ppm   = 0;
float    waterTemp  = TEMP_DEFAULT;
float    sumpDistCm = NAN;

bool emergencyStop  = false;
bool failsafeActive = false;
bool dosing         = true;
bool sumpReady      = false;

bool cloudOnline = false;

uint8_t currentAlert = ALERT_NONE;
bool    alarmActive  = false;

bool loraInitOk = false;

OneWire           oneWire(ONE_WIRE_PIN);
DallasTemperature tempSensors(&oneWire);
DeviceAddress     tempAddr_Sump;

// ============================================================
//  SECTION 4 — NVS LOAD / SAVE
// ============================================================

void loadCalibration() {
  prefs.begin("cal", true);
  for (uint8_t i = 0; i < NUM_SENSORS; i++) {
    char k[10];
    snprintf(k, sizeof(k), "sl%d", i); sensorCal[i].slope  = prefs.getFloat(k, 1.0f);
    snprintf(k, sizeof(k), "of%d", i); sensorCal[i].offset = prefs.getFloat(k, 0.0f);
    snprintf(k, sizeof(k), "v%d",  i); sensorCal[i].valid  = prefs.getBool(k,  false);
  }
  prefs.end();
}

void saveCalibration(uint8_t idx) {
  prefs.begin("cal", false);
  char k[10];
  snprintf(k, sizeof(k), "sl%d", idx); prefs.putFloat(k, sensorCal[idx].slope);
  snprintf(k, sizeof(k), "of%d", idx); prefs.putFloat(k, sensorCal[idx].offset);
  snprintf(k, sizeof(k), "v%d",  idx); prefs.putBool(k,  sensorCal[idx].valid);
  prefs.end();
}

void loadCropProfile() {
  prefs.begin("crop", true);
  cropProfile.target_ec = prefs.getFloat("ec",    1.5f);
  cropProfile.target_ph = prefs.getFloat("ph",    6.0f);
  cropProfile.n1_ratio  = prefs.getFloat("n1r",   1.0f);
  cropProfile.n2_ratio  = prefs.getFloat("n2r",   1.0f);
  cropProfile.n3_ratio  = prefs.getFloat("n3r",   1.0f);
  cropProfile.timestamp = prefs.getUInt("ts",     0);
  cropProfile.valid     = prefs.getBool("valid",  false);
  prefs.getString("name", cropProfile.name, 24);
  prefs.end();
}

void saveCropProfile() {
  prefs.begin("crop", false);
  prefs.putFloat("ec",    cropProfile.target_ec);
  prefs.putFloat("ph",    cropProfile.target_ph);
  prefs.putFloat("n1r",   cropProfile.n1_ratio);
  prefs.putFloat("n2r",   cropProfile.n2_ratio);
  prefs.putFloat("n3r",   cropProfile.n3_ratio);
  prefs.putUInt("ts",     cropProfile.timestamp);
  prefs.putBool("valid",  cropProfile.valid);
  prefs.putString("name", cropProfile.name);
  prefs.end();
}

void loadFailsafe() {
  prefs.begin("failsafe", true);
  failsafeProfile.target_ec = prefs.getFloat("fs_ec", 1.0f);
  failsafeProfile.target_ph = prefs.getFloat("fs_ph", 6.0f);
  prefs.end();
}

void saveFailsafe() {
  prefs.begin("failsafe", false);
  prefs.putFloat("fs_ec", failsafeProfile.target_ec);
  prefs.putFloat("fs_ph", failsafeProfile.target_ph);
  prefs.end();
}

void loadPIParams() {
  prefs.begin("pi", true);
  ec_kp          = prefs.getFloat("ec_kp",  PI_DEF_EC_KP);
  ec_ki          = prefs.getFloat("ec_ki",  PI_DEF_EC_KI);
  ph_kp          = prefs.getFloat("ph_kp",  PI_DEF_PH_KP);
  ph_ki          = prefs.getFloat("ph_ki",  PI_DEF_PH_KI);
  ph_deadband    = prefs.getFloat("ph_db",  PI_DEF_PH_DEADBAND);
  integral_clamp = prefs.getFloat("ic",     PI_DEF_INTEGRAL_CLAMP);
  min_pulse_ms   = prefs.getUInt("mn_p",    PI_DEF_MIN_PULSE);
  max_pulse_ms   = prefs.getUInt("mx_p",    PI_DEF_MAX_PULSE);
  ph_min_off_ms  = prefs.getUInt("ph_off",  PI_DEF_PH_MIN_OFF_MS);
  prefs.end();
}

void savePIParams() {
  prefs.begin("pi", false);
  prefs.putFloat("ec_kp", ec_kp);
  prefs.putFloat("ec_ki", ec_ki);
  prefs.putFloat("ph_kp", ph_kp);
  prefs.putFloat("ph_ki", ph_ki);
  prefs.putFloat("ph_db", ph_deadband);
  prefs.putFloat("ic",    integral_clamp);
  prefs.putUInt("mn_p",   min_pulse_ms);
  prefs.putUInt("mx_p",   max_pulse_ms);
  prefs.putUInt("ph_off", ph_min_off_ms);
  prefs.end();
}

// ============================================================
//  SECTION 5 — LORA SEND / RECEIVE
// ============================================================

void loraSend(const String& json) {
  LoRa.beginPacket();
  LoRa.write(ADDR_GW);
  LoRa.write(ADDR_SELF);
  LoRa.print(json);
  LoRa.endPacket();
  Serial.printf("[LORA TX->0x%02X] %s\n", ADDR_GW, json.c_str());
}

void loraSendAck(const char* cmdName, bool ok, const char* errMsg = "") {
  StaticJsonDocument<256> adoc;
  adoc["Did"]  = DEVICE_ID;
  adoc["uid"]  = UNIT_ID;
  adoc["type"] = "ack";
  adoc["cmd"]  = cmdName;
  adoc["ok"]   = ok;
  if (!ok && errMsg[0] != '\0') adoc["err"] = errMsg;
  String out;
  serializeJson(adoc, out);
  loraSend(out);
}

String loraReceiveIfAvailable() {
  int sz = LoRa.parsePacket();
  if (!sz) return "";
  if (sz < 2) {
    while (LoRa.available()) LoRa.read();
    return "";
  }
  uint8_t dest = LoRa.read();
  uint8_t src  = LoRa.read();
  String body  = "";
  while (LoRa.available()) body += (char)LoRa.read();
  Serial.printf("[LORA RX dest=0x%02X src=0x%02X] %s\n", dest, src, body.c_str());
  if (dest != ADDR_SELF) return "";
  return body;
}

// ============================================================
//  SECTION 6 — ALERT SYSTEM
// ============================================================

void raiseAlert(uint8_t code, const char* msg) {
  currentAlert = code;
  alarmActive  = true;
  Serial.printf("[ALERT] Code=%d  %s\n", code, msg);
  if (loraInitOk) {
    StaticJsonDocument<192> adoc;
    adoc["Did"]  = DEVICE_ID;
    adoc["uid"]  = UNIT_ID;
    adoc["type"] = "alert";
    adoc["code"] = code;
    adoc["msg"]  = msg;
    String out;
    serializeJson(adoc, out);
    loraSend(out);
  }
}

void clearAlert() {
  currentAlert = ALERT_NONE;
  alarmActive  = false;
}

// ============================================================
//  SECTION 7 — CALIBRATION MATH
// ============================================================

float applyCalibration(uint8_t idx, float raw) {
  if (!sensorCal[idx].valid) return raw;
  return sensorCal[idx].slope * raw + sensorCal[idx].offset;
}

void computeCalibration(uint8_t idx, float r1, float r2) {
    const float* refs = (idx == CAL_PH) ? CAL_REF_PH : CAL_REF_EC;
    if (fabsf(r2 - r1) < 0.1f) return;

    float slope, offset;
    if (idx == CAL_PH) {
        float v1 = r1 / 1000.0f;
        float v2 = r2 / 1000.0f;
        slope  = (refs[0] - refs[1]) / (v1 - v2);
        offset = refs[0] - slope * v1;
    } else {
        slope  = (refs[1] - refs[0]) / (r2 - r1);
        offset = refs[0] - slope * r1;
    }
    sensorCal[idx] = { slope, offset, r1, r2, true };
}

// ============================================================
//  SECTION 8 — PROFILE HELPERS
// ============================================================

bool validateProfileJSON(JsonObject& obj, CropProfile& out) {
  if (!obj.containsKey("ec") || !obj.containsKey("ph")) return false;
  float ec = obj["ec"].as<float>();
  float ph = obj["ph"].as<float>();
  if (ec < EC_INPUT_MIN || ec > EC_INPUT_MAX) return false;
  if (ph < PH_INPUT_MIN || ph > PH_INPUT_MAX) return false;
  out.target_ec = ec;
  out.target_ph = ph;
  out.n1_ratio  = obj.containsKey("n1") ? constrain(obj["n1"].as<float>(), 0.0f, 10.0f) : 1.0f;
  out.n2_ratio  = obj.containsKey("n2") ? constrain(obj["n2"].as<float>(), 0.0f, 10.0f) : 1.0f;
  out.n3_ratio  = obj.containsKey("n3") ? constrain(obj["n3"].as<float>(), 0.0f, 10.0f) : 1.0f;
  out.timestamp = obj.containsKey("ts") ? obj["ts"].as<uint32_t>() : 0;
  out.valid     = true;
  strlcpy(out.name,
          obj.containsKey("name") ? obj["name"].as<const char*>() : "REMOTE",
          24);
  return true;
}

void activateProfile(CropProfile& p, bool isFailsafe) {
  target_ec      = p.target_ec;
  target_ph      = p.target_ph;
  n1_ratio       = p.n1_ratio;
  n2_ratio       = p.n2_ratio;
  n3_ratio       = p.n3_ratio;
  failsafeActive = isFailsafe;
  Serial.printf("[PROFILE] %s  EC=%.2f pH=%.2f  ratios=%.1f:%.1f:%.1f\n",
                p.name, target_ec, target_ph, n1_ratio, n2_ratio, n3_ratio);
}

void queuePendingUpdate(CropProfile& fp) {
  if (pendingUpdateCount < MAX_PENDING) {
    pendingUpdates[pendingUpdateCount++] = fp;
  } else {
    pendingUpdates[0] = pendingUpdates[1];
    pendingUpdates[1] = fp;
  }
}

void applyPendingUpdates() {
  if (!pendingUpdateCount) return;
  cropProfile = pendingUpdates[pendingUpdateCount - 1];
  saveCropProfile();
  pendingUpdateCount = 0;
  Serial.println("[PROFILE] Pending update applied");
}

void resetPIIntegrals() {
  ec_integral = 0.0f;
  ph_integral = 0.0f;
}

// ============================================================
//  SECTION 9 — SENSOR READS
// ============================================================

void tempInit() {
  tempSensors.begin();
  if (tempSensors.getDeviceCount() >= 1)
    tempSensors.getAddress(tempAddr_Sump, 0);
  tempSensors.setResolution(12);
}

void readTemperature() {
  tempSensors.requestTemperatures();
  float t = tempSensors.getTempC(tempAddr_Sump);
  waterTemp = (t > -100.0f) ? t : TEMP_DEFAULT;
}

float getMedianNum_pH(float* bArray, int iFilterLen) {
  float bTab[iFilterLen];
  for (int i = 0; i < iFilterLen; i++) bTab[i] = bArray[i];

  for (int j = 0; j < iFilterLen - 1; j++)
    for (int i = 0; i < iFilterLen - j - 1; i++)
      if (bTab[i] > bTab[i + 1]) {
        float tmp   = bTab[i];
        bTab[i]     = bTab[i + 1];
        bTab[i + 1] = tmp;
      }

  if (iFilterLen & 1)
    return bTab[(iFilterLen - 1) / 2];
  else
    return (bTab[iFilterLen / 2] + bTab[iFilterLen / 2 - 1]) / 2.0f;
}

float readVoltage_pH() {
  float buf[PH_SCOUNT];
  for (int i = 0; i < PH_SCOUNT; i++) {
    buf[i] = (float)analogRead(ADC_PH_PIN) / PH_ADC_RES * PH_ADC_VREF;
    delay(40);
  }
  float mV = getMedianNum_pH(buf, PH_SCOUNT);
  Serial.printf("[pH RAW] median=%.2f mV\n", mV);
  return mV;
}

float readRaw_TDS_uS() {
  float arr[TDS_ADC_SAMPLES];
  for (uint8_t i = 0; i < TDS_ADC_SAMPLES; i++) {
    arr[i] = analogRead(ADC_EC_PIN) * (VREF / ADC_RESOLUTION);
    delay(30);
  }
  for (uint8_t i = 0; i < TDS_ADC_SAMPLES - 1; i++)
    for (uint8_t j = i + 1; j < TDS_ADC_SAMPLES; j++)
      if (arr[j] < arr[i]) { float t = arr[i]; arr[i] = arr[j]; arr[j] = t; }
  float sum = 0.0f; int cnt = 0;
  for (uint8_t i = 5; i < 15; i++) {
    if (arr[i] > 0.01f) { sum += arr[i]; cnt++; }
  }
  if (!cnt) return 0.0f;
  float v = sum / cnt;
  return (133.42f * v * v * v - 255.86f * v * v + 857.39f * v) * 0.5f * VREF_EC_FACTOR;
}

float compensateEC(float rawUS, float tempC) {
  if (tempC <= 0.0f || tempC > 100.0f) return rawUS;
  return rawUS / (1.0f + 0.02f * (tempC - 25.0f));
}

float readRawAvg(uint8_t idx, uint8_t n = 20) {
    if (idx == CAL_PH) {
        float s = 0.0f;
        for (uint8_t i = 0; i < PH_CAL_ROUNDS; i++) {
            s += readVoltage_pH();
            delay(10);
        }
        return s / PH_CAL_ROUNDS;
    }
    return readRaw_TDS_uS();
}

float readSump_pH() {
    float mV = readVoltage_pH();

    if (!sensorCal[CAL_PH].valid) {
        return (mV / 1000.0f) * 3.5f + 0.0f;
    }

    return (mV / 1000.0f) * sensorCal[CAL_PH].slope + sensorCal[CAL_PH].offset;
}

float readSump_EC_mS() {
  return compensateEC(
    applyCalibration(CAL_EC, readRaw_TDS_uS()),
    waterTemp) / 1000.0f;
}

void readSumpLevel() {
  float r[3];
  for (uint8_t i = 0; i < 3; i++) {
    digitalWrite(US_TRIG_SUMP, LOW);  delayMicroseconds(2);
    digitalWrite(US_TRIG_SUMP, HIGH); delayMicroseconds(10);
    digitalWrite(US_TRIG_SUMP, LOW);
    long d = pulseIn(US_ECHO_SUMP, HIGH, 30000UL);
    r[i] = d ? (d * 0.0343f) / 2.0f : NAN;
    delay(20);
  }
  float valid[3]; uint8_t vc = 0;
  for (uint8_t i = 0; i < 3; i++) if (!isnan(r[i])) valid[vc++] = r[i];
  if (vc == 0) { sumpDistCm = NAN; return; }
  for (uint8_t i = 0; i < vc - 1; i++)
    for (uint8_t j = i + 1; j < vc; j++)
      if (valid[j] < valid[i]) { float t = valid[i]; valid[i] = valid[j]; valid[j] = t; }
  sumpDistCm = valid[vc / 2];
}

// ============================================================
//  SECTION 10 — PUMP CONTROL
// ============================================================

void allPumpsOff() {
  digitalWrite(PUMP_N1,   LOW);
  digitalWrite(PUMP_N2,   LOW);
  digitalWrite(PUMP_N3,   LOW);
  digitalWrite(PUMP_PHUP, LOW);
  digitalWrite(PUMP_PHDN, LOW);
}

void firePulse(uint8_t pin, uint32_t ms) {
  if (!ms || emergencyStop) return;
  digitalWrite(PUMP_N1,   LOW);
  digitalWrite(PUMP_N2,   LOW);
  digitalWrite(PUMP_N3,   LOW);
  digitalWrite(PUMP_PHUP, LOW);
  digitalWrite(PUMP_PHDN, LOW);
  delay(20);
  digitalWrite(pin, HIGH);
  delay(ms);
  digitalWrite(pin, LOW);
  delay(20);
}
void splitNutrientDose(uint32_t total_ms,
                       uint32_t& out_n1,
                       uint32_t& out_n2,
                       uint32_t& out_n3) {
  float sum = n1_ratio + n2_ratio + n3_ratio;
  if (sum < 0.001f) {
    out_n1 = out_n2 = out_n3 = total_ms / 3;
    return;
  }
  out_n1 = (uint32_t)(total_ms * (n1_ratio / sum));
  out_n2 = (uint32_t)(total_ms * (n2_ratio / sum));
  out_n3 = (uint32_t)(total_ms * (n3_ratio / sum));
}

uint32_t toPulseMs(float u) {
  float a = fabsf(u);
  if (a < 1.0f) return 0;
  uint32_t ms = (uint32_t)(a / 100.0f * max_pulse_ms);
  return (ms < min_pulse_ms) ? 0 : min(ms, max_pulse_ms);
}
uint32_t toPulseMs_pH(float u) {
  float a = fabsf(u);
  if (a < 1.0f) return 0;
  uint32_t ms = (uint32_t)(a / 100.0f * max_pulse_ph_ms);
  return (ms < min_pulse_ph_ms) ? 0 : min(ms, max_pulse_ph_ms);
}

// ============================================================
//  SECTION 11 — SAFETY & SUMP READY
// ============================================================

bool safetyCheck(float ph, float ec) {
  (void)ph; (void)ec;
  return true;
}

static void setWtlvl(JsonDocument& doc, const char* key) {
  if (isnan(sumpDistCm))
    doc[key] = nullptr;
  else
    doc[key] = serialized(String(sumpDistCm, 1));
}

void checkSumpReady() {
  bool ecOk     = fabsf(live_ec - target_ec) <= EC_TOL;
  bool phOk     = fabsf(live_ph - target_ph) <= PH_TOL;
  bool nowReady = ecOk && phOk;

  if (nowReady && !sumpReady) {
    sumpReady = true;
    dosing    = false;
    Serial.println("[SUMP] READY — EC and pH in tolerance");
    if (loraInitOk) {
      StaticJsonDocument<320> sdoc;
      sdoc["Did"]        = DEVICE_ID;
      sdoc["uid"]        = UNIT_ID;
      sdoc["bid"]        = BOARD_ID;
      sdoc["fid"]        = FARM_ID;
      sdoc["Ph"]         = serialized(String(live_ph,   2));
      sdoc["EC"]         = live_ppm;
      sdoc["T"]          = serialized(String(waterTemp, 1));
      setWtlvl(sdoc, "wtlvl");
      sdoc["sump_ready"] = true;
      String out; serializeJson(sdoc, out);
      loraSend(out);
    }
  }
  if (!nowReady && sumpReady) {
    sumpReady = false;
    dosing    = true;
    Serial.println("[SUMP] Out of tolerance — dosing resumed");
  }
  if (!sumpReady) dosing = true;
}

// ============================================================
//  SECTION 12 — PI CONTROL CYCLE
// ============================================================

void runControlCycle() {
  if (emergencyStop) return;

  live_ph  = readSump_pH();
  live_ec  = readSump_EC_mS();
  live_ppm = (uint16_t)(live_ec * 1000.0f * EC_TO_PPM_FACTOR);

  checkSumpReady();
  if (!dosing) return;

  float dt = EC_CTRL_MS / 1000.0f;

  float ec_error = target_ec - live_ec;
  float pi_ec    = ec_kp * ec_error + ec_ki * ec_integral;
  float total_ec = constrain(pi_ec, 0.0f, 100.0f);
  uint32_t ec_pulse = toPulseMs(total_ec);

  Serial.printf("[EC] error=%.3f pi=%.2f%% pulse=%lums\n",
                ec_error, total_ec, ec_pulse);

  if (ec_pulse != 0 && ec_pulse != max_pulse_ms) {
    ec_integral += ec_error * dt;
    ec_integral  = constrain(ec_integral, -integral_clamp, integral_clamp);
  }

  if (ec_pulse > 0 && ec_error > EC_TOL) {
    uint32_t n1_ms, n2_ms, n3_ms;
    splitNutrientDose(ec_pulse, n1_ms, n2_ms, n3_ms);
    Serial.printf("[EC] N1=%lums N2=%lums N3=%lums min=%lums\n",
                  n1_ms, n2_ms, n3_ms, min_pulse_ms);
    if (n1_ms >= min_pulse_ms) {
      Serial.println("[PUMP] N1 firing");
      firePulse(PUMP_N1, n1_ms);
    }
    if (n2_ms >= min_pulse_ms) {
      Serial.println("[PUMP] N2 firing");
      firePulse(PUMP_N2, n2_ms);
    }
    if (n3_ms >= min_pulse_ms) {
      Serial.println("[PUMP] N3 firing");
      firePulse(PUMP_N3, n3_ms);
    }
  }

  float ph_error = target_ph - live_ph;

  Serial.printf("[pH] error=%.3f deadband=%.3f\n",
                ph_error, ph_deadband);

  if (fabsf(ph_error) <= ph_deadband) {
    ph_integral = 0.0f;
    Serial.println("[pH] in deadband — pumps off");
    return;
  }

  uint32_t now = millis();
  bool phOffOk = (lastPHDoseAt == 0) ||
                 ((now - lastPHDoseAt) >= ph_min_off_ms);

  Serial.printf("[pH] offOk=%d lastDose=%lums ago\n",
                phOffOk, (now - lastPHDoseAt));

  if (!phOffOk) return;

  float pi_ph    = ph_kp * ph_error + ph_ki * ph_integral;
  float total_ph = constrain(fabsf(pi_ph), 0.0f, 100.0f);
  uint32_t ph_pulse = toPulseMs(total_ph);

  Serial.printf("[pH] pi=%.2f%% pulse=%lums\n", total_ph, ph_pulse);

  if (ph_pulse != 0 && ph_pulse != max_pulse_ms) {
    ph_integral += ph_error * dt;
    ph_integral  = constrain(ph_integral, -integral_clamp, integral_clamp);
  }

  if (ph_pulse > 0 && fabsf(ph_error) > PH_TOL) {
    if (ph_error > 0.0f) {
      Serial.println("[PUMP] PHUP firing");
      firePulse(PUMP_PHUP, ph_pulse);
    } else {
      Serial.println("[PUMP] PHDN firing");
      firePulse(PUMP_PHDN, ph_pulse);
    }
    lastPHDoseAt = millis();
  }
}

// ============================================================
//  SECTION 13 — TELEMETRY
// ============================================================

void loraSendTelemetry() {
  Serial.printf("[TELE DEBUG] loraInitOk=%d\n", loraInitOk);
  if (!loraInitOk) {
    Serial.println("[TELE] SKIPPED — LoRa not initialised");
    return;
  }

  readTemperature();
  readSumpLevel();
  live_ph  = readSump_pH();
  live_ec  = readSump_EC_mS();
  live_ppm = (uint16_t)(live_ec * 1000.0f * EC_TO_PPM_FACTOR);

  bool isSafe = (live_ph >= PH_MIN_SAFE && live_ph <= PH_MAX_SAFE &&
                 live_ec <= EC_MAX_SAFE);

  StaticJsonDocument<384> doc;
  doc["Did"]   = DEVICE_ID;
  doc["uid"]   = UNIT_ID;
  doc["bid"]   = BOARD_ID;
  doc["fid"]   = FARM_ID;
  doc["Ph"]    = serialized(String(live_ph,   2));
  doc["EC"]    = live_ppm;
  doc["T"]     = serialized(String(waterTemp, 1));
  setWtlvl(doc, "wtlvl");
  doc["alert"] = currentAlert;
  doc["safe"]  = isSafe;
  doc["ready"] = sumpReady;
  doc["estop"] = emergencyStop;

  String out;
  serializeJson(doc, out);
  loraSend(out);

  Serial.printf("[TELE SENT] pH=%.2f  EC=%.3f mS/cm (%u ppm)  T=%.1f C"
                "  wtlvl=%s cm  alert=%d  safe=%d  ready=%d\n",
                live_ph, live_ec, live_ppm, waterTemp,
                isnan(sumpDistCm) ? "err" : String(sumpDistCm, 1).c_str(),
                currentAlert, isSafe, sumpReady);
}

// ============================================================
//  SECTION 13b — OTA  (self-flash over WiFi, blocking — acceptable,
//  this node has no other time-critical duty during an update)
// ============================================================

void runSelfOta(const char* url, const char* ssid, const char* pass) {
  if (!url || !strlen(url)) {
    loraSendAck("ota", false, "missing url");
    return;
  }
  if (!ssid || !strlen(ssid) || !pass || !strlen(pass)) {
    loraSendAck("ota", false, "missing wifi_ssid/wifi_pass");
    return;
  }

  Serial.printf("[OTA] joining WiFi '%s'...\n", ssid);
  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, pass);

  uint32_t deadline = millis() + 20000UL;
  while (WiFi.status() != WL_CONNECTED && millis() < deadline) delay(500);

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[OTA] WiFi join failed/timed out");
    loraSendAck("ota", false, "wifi join failed");
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
    if (attempt < MAX_ATTEMPTS) delay(3000);
  }

  switch (result) {
    case HTTP_UPDATE_FAILED:
      loraSendAck("ota", false, httpUpdate.getLastErrorString().c_str());
      break;
    case HTTP_UPDATE_NO_UPDATES:
      loraSendAck("ota", false, "no update at url");
      break;
    case HTTP_UPDATE_OK:
      // rebootOnUpdate(true) means we never reach here on success
      break;
  }
  WiFi.mode(WIFI_OFF);
}

// ============================================================
//  SECTION 14 — COMMAND HANDLER
// ============================================================

void handleLoraCommand(const String& body) {
  StaticJsonDocument<512> doc;
  if (deserializeJson(doc, body)) {
    Serial.println("[CMD] JSON parse failed");
    return;
  }

  const char* type = doc["type"] | "";

  // ── MQTT CONNECTIVITY NOTIFICATION ──────────────────────────
  // No identity check needed — sent by gateway itself
  if (strcmp(type, "mqtt") == 0) {
    const char* state = doc["state"] | "";
    cloudOnline = (strcmp(state, "connected") == 0);
    Serial.printf("[MQTT STATUS] Cloud %s\n", cloudOnline ? "ONLINE" : "OFFLINE");
    return;
  }

  // ── OTA ──────────────────────────────────────────────────────
  // Checked BEFORE the generic "cmd" gate below, since OTA uses its
  // own type. Identity check here uses just Did+uid (OTA envelopes
  // from the gateway may omit fid/bid to save LoRa payload space).
  if (strcmp(type, "ota") == 0) {
    const char* did = doc["Did"] | "";
    const char* uid = doc["uid"] | "";
    if (strcmp(did, DEVICE_ID) != 0 || strcmp(uid, UNIT_ID) != 0) {
      Serial.printf("[OTA] REJECTED — identity mismatch Did=%s uid=%s\n", did, uid);
      return;
    }
    const char* url  = doc["url"]  | "";
    const char* ssid = doc["wifi"] | "";
    const char* pass = doc["pass"] | "";
    Serial.printf("[OTA] request received — url:%s\n", url);
    runSelfOta(url, ssid, pass);
    return;
  }

  if (strcmp(type, "cmd") != 0) return;

  // ── IDENTITY CHECK — all 4 fields must match this device ────
  const char* did = doc["Did"] | "";
  const char* uid = doc["uid"] | "";
  const char* fid = doc["fid"] | "";
  const char* bid = doc["bid"] | "";

  if (strcmp(did, DEVICE_ID) != 0 ||
      strcmp(uid, UNIT_ID)   != 0 ||
      strcmp(fid, FARM_ID)   != 0 ||
      strcmp(bid, BOARD_ID)  != 0) {
    Serial.printf("[CMD] REJECTED — identity mismatch "
                  "Did=%s uid=%s fid=%s bid=%s\n",
                  did, uid, fid, bid);
    return;
  }

  const char* cmd = doc["cmd"] | "";

  // ── STOP ────────────────────────────────────────────────────
  if (strcmp(cmd, "stop") == 0) {
    emergencyStop = true;
    allPumpsOff();
    Serial.println("[CMD] STOP");
    loraSendAck(cmd, true);
    return;
  }

  // ── RESUME ──────────────────────────────────────────────────
  if (strcmp(cmd, "resume") == 0) {
    emergencyStop = false;
    Serial.println("[CMD] RESUME");
    loraSendAck(cmd, true);
    return;
  }

  // ── STATUS ──────────────────────────────────────────────────
  if (strcmp(cmd, "status") == 0) {
    StaticJsonDocument<768> sdoc;
    sdoc["Did"]        = DEVICE_ID;
    sdoc["uid"]        = UNIT_ID;
    sdoc["bid"]        = BOARD_ID;
    sdoc["fid"]        = FARM_ID;
    sdoc["type"]       = "status";
    sdoc["Ph"]         = serialized(String(live_ph,   2));
    sdoc["EC"]         = live_ppm;
    sdoc["T"]          = serialized(String(waterTemp, 1));
    setWtlvl(sdoc, "wtlvl");
    sdoc["ec_sp"]      = serialized(String(target_ec, 2));
    sdoc["ph_sp"]      = serialized(String(target_ph, 2));
    sdoc["n1r"]        = serialized(String(n1_ratio,  2));
    sdoc["n2r"]        = serialized(String(n2_ratio,  2));
    sdoc["n3r"]        = serialized(String(n3_ratio,  2));
    sdoc["fs_ec"]      = serialized(String(failsafeProfile.target_ec, 2));
    sdoc["fs_ph"]      = serialized(String(failsafeProfile.target_ph, 2));
    sdoc["sump_ready"] = sumpReady;
    sdoc["dosing"]     = dosing;
    sdoc["alert"]      = currentAlert;
    sdoc["failsafe"]   = failsafeActive;
    sdoc["e_stop"]     = emergencyStop;
    sdoc["cal_ph"]     = sensorCal[CAL_PH].valid;
    sdoc["cal_ec"]     = sensorCal[CAL_EC].valid;
    sdoc["ec_kp"]      = serialized(String(ec_kp,          3));
    sdoc["ec_ki"]      = serialized(String(ec_ki,          4));
    sdoc["ph_kp"]      = serialized(String(ph_kp,          3));
    sdoc["ph_ki"]      = serialized(String(ph_ki,          4));
    sdoc["ph_db"]      = serialized(String(ph_deadband,    3));
    sdoc["ic"]         = serialized(String(integral_clamp, 2));
    sdoc["mn_p"]       = min_pulse_ms;
    sdoc["mx_p"]       = max_pulse_ms;
    sdoc["ph_off"]     = ph_min_off_ms;
    String out; serializeJson(sdoc, out);
    loraSend(out);
    return;
  }

  // ── SET SETPOINTS ────────────────────────────────────────────
  if (strcmp(cmd, "set_setpoints") == 0) {
    if (!doc.containsKey("ph") || !doc.containsKey("ec")) {
      loraSendAck(cmd, false, "missing ph or ec"); return;
    }
    float new_ph = doc["ph"].as<float>();
    float new_ec = doc["ec"].as<float>();
    if (new_ph < PH_INPUT_MIN || new_ph > PH_INPUT_MAX) { loraSendAck(cmd, false, "ph out of range"); return; }
    if (new_ec < EC_INPUT_MIN || new_ec > EC_INPUT_MAX) { loraSendAck(cmd, false, "ec out of range"); return; }
    target_ph = new_ph;
    target_ec = new_ec;
    cropProfile.target_ph = target_ph;
    cropProfile.target_ec = target_ec;
    saveCropProfile();
    sumpReady = false; dosing = true;
    resetPIIntegrals();
    loraSendAck(cmd, true);
    return;
  }

  // ── SET RATIOS ───────────────────────────────────────────────
  if (strcmp(cmd, "set_ratios") == 0) {
    if (!doc.containsKey("n1") || !doc.containsKey("n2") || !doc.containsKey("n3")) {
      loraSendAck(cmd, false, "missing n1/n2/n3"); return;
    }
    float r1 = doc["n1"].as<float>();
    float r2 = doc["n2"].as<float>();
    float r3 = doc["n3"].as<float>();
    if (r1 < 0.0f || r1 > 10.0f || r2 < 0.0f || r2 > 10.0f || r3 < 0.0f || r3 > 10.0f) {
      loraSendAck(cmd, false, "ratio out of range 0-10"); return;
    }
    n1_ratio = r1; n2_ratio = r2; n3_ratio = r3;
    cropProfile.n1_ratio = n1_ratio;
    cropProfile.n2_ratio = n2_ratio;
    cropProfile.n3_ratio = n3_ratio;
    saveCropProfile();
    loraSendAck(cmd, true);
    return;
  }

  // ── SET FAILSAFE ─────────────────────────────────────────────
  if (strcmp(cmd, "set_failsafe") == 0) {
    if (!doc.containsKey("ph") || !doc.containsKey("ec")) {
      loraSendAck(cmd, false, "missing ph or ec"); return;
    }
    float fs_ph = doc["ph"].as<float>();
    float fs_ec = doc["ec"].as<float>();
    if (fs_ph < PH_INPUT_MIN || fs_ph > PH_INPUT_MAX) { loraSendAck(cmd, false, "ph out of range"); return; }
    if (fs_ec < EC_INPUT_MIN || fs_ec > EC_INPUT_MAX) { loraSendAck(cmd, false, "ec out of range"); return; }
    failsafeProfile.target_ph = fs_ph;
    failsafeProfile.target_ec = fs_ec;
    saveFailsafe();
    loraSendAck(cmd, true);
    return;
  }

  // ── UPDATE PROFILE ───────────────────────────────────────────
  if (strcmp(cmd, "update_profile") == 0) {
    JsonObject obj = doc.as<JsonObject>();
    CropProfile fp;
    if (!validateProfileJSON(obj, fp)) {
      loraSendAck(cmd, false, "profile validation failed");
      raiseAlert(ALERT_JSON_ERROR, "LoRa: profile validation failed");
      return;
    }
    if (dosing && !sumpReady) {
      queuePendingUpdate(fp);
    } else {
      cropProfile = fp;
      saveCropProfile();
      activateProfile(cropProfile, false);
      sumpReady = false; dosing = true;
      resetPIIntegrals();
    }
    loraSendAck(cmd, true);
    return;
  }

  // ── CAL CAPTURE ──────────────────────────────────────────────
  if (strcmp(cmd, "cal_capture") == 0) {
    const char* sensor = doc["sensor"] | "";
    int point = doc["point"] | 0;
    if (point != 1 && point != 2) { loraSendAck(cmd, false, "point must be 1 or 2"); return; }
    uint8_t sIdx;
    if      (strcmp(sensor, "ph") == 0) sIdx = CAL_PH;
    else if (strcmp(sensor, "ec") == 0) sIdx = CAL_EC;
    else { loraSendAck(cmd, false, "sensor must be ph or ec"); return; }

    int pIdx = point - 1;
    float raw = readRawAvg(sIdx, 30);
    calRawCapture[sIdx][pIdx] = raw;
    calPtDone[sIdx][pIdx]     = true;

    Serial.printf("[CAL] capture sensor=%s pt=%d raw=%.4f\n", sensor, point, raw);

    StaticJsonDocument<192> rdoc;
    rdoc["Did"]    = DEVICE_ID;
    rdoc["uid"]    = UNIT_ID;
    rdoc["type"]   = "ack";
    rdoc["cmd"]    = "cal_capture";
    rdoc["ok"]     = true;
    rdoc["sensor"] = sensor;
    rdoc["point"]  = point;
    rdoc["raw"]    = serialized(String(raw, 4));
    String rout; serializeJson(rdoc, rout);
    loraSend(rout);
    return;
  }

  // ── CAL CONFIRM ──────────────────────────────────────────────
  if (strcmp(cmd, "cal_confirm") == 0) {
    const char* sensor = doc["sensor"] | "";
    uint8_t sIdx;
    if      (strcmp(sensor, "ph") == 0) sIdx = CAL_PH;
    else if (strcmp(sensor, "ec") == 0) sIdx = CAL_EC;
    else { loraSendAck(cmd, false, "sensor must be ph or ec"); return; }

    if (!calPtDone[sIdx][0] || !calPtDone[sIdx][1]) {
      loraSendAck(cmd, false, "both points not yet captured for this sensor"); return;
    }

    computeCalibration(sIdx, calRawCapture[sIdx][0], calRawCapture[sIdx][1]);

    if (!sensorCal[sIdx].valid) {
      loraSendAck(cmd, false, "compute failed - points too close"); return;
    }
    saveCalibration(sIdx);
    calPtDone[sIdx][0] = calPtDone[sIdx][1] = false;

    Serial.printf("[CAL] confirm sensor=%s slope=%.4f offset=%.4f\n",
                  sensor, sensorCal[sIdx].slope, sensorCal[sIdx].offset);

    StaticJsonDocument<224> rdoc;
    rdoc["Did"]    = DEVICE_ID;
    rdoc["uid"]    = UNIT_ID;
    rdoc["type"]   = "ack";
    rdoc["cmd"]    = "cal_confirm";
    rdoc["ok"]     = true;
    rdoc["sensor"] = sensor;
    rdoc["slope"]  = serialized(String(sensorCal[sIdx].slope,  4));
    rdoc["offset"] = serialized(String(sensorCal[sIdx].offset, 4));
    String rout; serializeJson(rdoc, rout);
    loraSend(rout);
    return;
  }

  // ── SET PI PARAMS ────────────────────────────────────────────
  #define APPLY_F(key, var, lo, hi) \
    if (doc.containsKey(key)) { \
      float _v = doc[key].as<float>(); \
      if (_v < (float)(lo) || _v > (float)(hi)) { \
        snprintf(errBuf, sizeof(errBuf), "%s out of range", key); \
        loraSendAck(cmd, false, errBuf); return; \
      } \
      var = _v; changed = true; \
    }

  #define APPLY_U(key, var, lo, hi) \
    if (doc.containsKey(key)) { \
      uint32_t _v = doc[key].as<uint32_t>(); \
      if (_v < (uint32_t)(lo) || _v > (uint32_t)(hi)) { \
        snprintf(errBuf, sizeof(errBuf), "%s out of range", key); \
        loraSendAck(cmd, false, errBuf); return; \
      } \
      var = _v; changed = true; \
    }

  // ── SET EC PI ────────────────────────────────────────────────
  if (strcmp(cmd, "set_ec_pi") == 0) {
    bool changed = false;
    char errBuf[64] = "";

    APPLY_F("ec_kp", ec_kp,          0.0f,  20.0f)
    APPLY_F("ec_ki", ec_ki,          0.0f,   5.0f)
    APPLY_F("ic",    integral_clamp, 1.0f,  50.0f)
    APPLY_U("mn_p",  min_pulse_ms,   50UL,  10000UL)
    APPLY_U("mx_p",  max_pulse_ms,   50UL,  10000UL)

    if (min_pulse_ms >= max_pulse_ms) {
      min_pulse_ms = PI_DEF_MIN_PULSE;
      max_pulse_ms = PI_DEF_MAX_PULSE;
      loraSendAck(cmd, false, "mn_p must be < mx_p - reverted to defaults");
      return;
    }
    if (!changed) { loraSendAck(cmd, false, "no recognised EC PI fields"); return; }

    savePIParams();

    StaticJsonDocument<256> rdoc;
    rdoc["Did"]   = DEVICE_ID;
    rdoc["uid"]   = UNIT_ID;
    rdoc["type"]  = "ack";
    rdoc["cmd"]   = "set_ec_pi";
    rdoc["ok"]    = true;
    rdoc["ec_kp"] = serialized(String(ec_kp,          3));
    rdoc["ec_ki"] = serialized(String(ec_ki,          4));
    rdoc["ic"]    = serialized(String(integral_clamp, 2));
    rdoc["mn_p"]  = min_pulse_ms;
    rdoc["mx_p"]  = max_pulse_ms;
    String rout; serializeJson(rdoc, rout);
    loraSend(rout);
    return;
  }

  // ── SET PH PI ────────────────────────────────────────────────
  if (strcmp(cmd, "set_ph_pi") == 0) {
    bool changed = false;
    char errBuf[64] = "";

    APPLY_F("ph_kp", ph_kp,          0.0f, 20.0f)
    APPLY_F("ph_ki", ph_ki,          0.0f,  5.0f)
    APPLY_F("ph_db", ph_deadband,    0.0f,  2.0f)
    APPLY_U("ph_off", ph_min_off_ms, 5000UL, 300000UL)

    if (!changed) { loraSendAck(cmd, false, "no recognised pH PI fields"); return; }

    savePIParams();

    StaticJsonDocument<256> rdoc;
    rdoc["Did"]    = DEVICE_ID;
    rdoc["uid"]    = UNIT_ID;
    rdoc["type"]   = "ack";
    rdoc["cmd"]    = "set_ph_pi";
    rdoc["ok"]     = true;
    rdoc["ph_kp"]  = serialized(String(ph_kp,       3));
    rdoc["ph_ki"]  = serialized(String(ph_ki,       4));
    rdoc["ph_db"]  = serialized(String(ph_deadband, 3));
    rdoc["ph_off"] = ph_min_off_ms;
    String rout; serializeJson(rdoc, rout);
    loraSend(rout);
    return;
  }

  #undef APPLY_F
  #undef APPLY_U

  // ── UNKNOWN ──────────────────────────────────────────────────
  Serial.printf("[CMD] Unknown command: %s\n", cmd);
  loraSendAck(cmd, false, "unknown command");
}

// ============================================================
//  SECTION 15 — SETUP
// ============================================================

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n[BOOT] Fert1 SumpController v2.3-LoRa (OTA)");

  const uint8_t pumpPins[] = { PUMP_N1, PUMP_N2, PUMP_N3, PUMP_PHUP, PUMP_PHDN };
  for (uint8_t i = 0; i < 5; i++) {
    pinMode(pumpPins[i], OUTPUT);
    digitalWrite(pumpPins[i], LOW);
  }

  pinMode(US_TRIG_SUMP, OUTPUT); digitalWrite(US_TRIG_SUMP, LOW);
  pinMode(US_ECHO_SUMP, INPUT);

  analogReadResolution(12);
  analogSetPinAttenuation(ADC_PH_PIN, ADC_11db);
  analogSetPinAttenuation(ADC_EC_PIN, ADC_11db);

  pinMode(ADC_PH_PIN, INPUT);
  pinMode(ADC_EC_PIN, INPUT);

  tempInit();

  loadCalibration();
  loadCropProfile();
  loadFailsafe();
  loadPIParams();

  for (uint8_t s = 0; s < NUM_SENSORS; s++) {
    calRawCapture[s][0] = calRawCapture[s][1] = 0.0f;
    calPtDone[s][0]     = calPtDone[s][1]     = false;
  }

  SPI.begin(LORA_SCK, LORA_MISO, LORA_MOSI, LORA_CS);
  LoRa.setPins(LORA_CS, LORA_RST, LORA_DIO0);
  if (!LoRa.begin(LORA_FREQ)) {
    Serial.println("[LORA] FAILED — running offline");
    loraInitOk = false;
  } else {
    LoRa.setSpreadingFactor(LORA_SF);
    LoRa.setSignalBandwidth(LORA_BW);
    LoRa.setCodingRate4(LORA_CR);
    LoRa.setTxPower(LORA_TXPOWER);
    LoRa.setSyncWord(LORA_SYNCWORD);
    LoRa.setPreambleLength(LORA_PREAMBLE);
    LoRa.enableCrc();
    loraInitOk = true;
    Serial.printf("[LORA] Ready 433 MHz SF%d BW125 CR%d 0x%02X  node=0x%02X  gw=0x%02X\n",
                  LORA_SF, LORA_CR, LORA_SYNCWORD, ADDR_SELF, ADDR_GW);
  }

  emergencyStop      = false;
  failsafeActive     = false;
  sumpReady          = false;
  dosing             = true;
  cloudOnline        = false;
  pendingUpdateCount = 0;
  currentAlert       = ALERT_NONE;
  alarmActive        = false;
  resetPIIntegrals();
  lastPHDoseAt = 0;

  if (cropProfile.valid) {
    activateProfile(cropProfile, false);
  } else {
    raiseAlert(ALERT_FETCH_TIMEOUT, "No stored profile - using failsafe");
    activateProfile(failsafeProfile, true);
  }

  readTemperature();
  readSumpLevel();
  live_ph  = readSump_pH();
  live_ec  = readSump_EC_mS();
  live_ppm = (uint16_t)(live_ec * 1000.0f * EC_TO_PPM_FACTOR);

  Serial.printf("[BOOT] pH=%.2f  EC=%.3f mS/cm (%u ppm)  T=%.1f C  wtlvl=%s cm\n",
                live_ph, live_ec, live_ppm, waterTemp,
                isnan(sumpDistCm) ? "err" : String(sumpDistCm, 1).c_str());
  Serial.printf("[BOOT] Setpoints EC=%.2f pH=%.2f  Ratios N1:N2:N3=%.1f:%.1f:%.1f\n",
                target_ec, target_ph, n1_ratio, n2_ratio, n3_ratio);

  loraSendTelemetry();
}

// ============================================================
//  SECTION 16 — LOOP
// ============================================================

void loop() {
  static uint32_t tLora  = 0;
  static uint32_t tTemp  = 0;
  static uint32_t tLevel = 0;
  static uint32_t tCtrl  = 0;
  static uint32_t tLog   = 0;
  static uint32_t tTele  = 0;
  static bool     firstLoop = true;

  uint32_t now = millis();

  if (firstLoop) {
    firstLoop = false;
    tTele     = now;
    Serial.println("[TELE] Timer started — next TX in 60 s");
  }

  if (now - tLora >= 100) {
    tLora = now;
    String body = loraReceiveIfAvailable();
    if (body.length() > 0) handleLoraCommand(body);
  }

  if (now - tTemp >= 3000) {
    tTemp = now;
    readTemperature();
  }

  if (now - tLevel >= 5000) {
    tLevel = now;
    readSumpLevel();
  }

  if (now - tCtrl >= EC_CTRL_MS) {
    tCtrl = now;
    runControlCycle();
  }

  if (now - tLog >= 2000) {
    tLog = now;
    Serial.printf("[LIVE] pH=%.2f  EC=%.3f(%u ppm)  T=%.1f C  lvl=%s cm  %s%s\n",
                  live_ph, live_ec, live_ppm, waterTemp,
                  isnan(sumpDistCm) ? "err" : String(sumpDistCm, 1).c_str(),
                  sumpReady     ? "READY "   : "DOSING",
                  emergencyStop ? " [E-STOP]" : "");
  }

  if (now - tTele >= TELE_INT) {
    tTele = now;
    Serial.println("[TELE] 60 s tick — transmitting...");
    loraSendTelemetry();
  }

  if (sumpReady && pendingUpdateCount > 0) {
    applyPendingUpdates();
    activateProfile(cropProfile, false);
    sumpReady = false;
    dosing    = true;
    resetPIIntegrals();
  }
}