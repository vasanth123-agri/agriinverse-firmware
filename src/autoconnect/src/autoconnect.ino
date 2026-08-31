#include <WiFiManager.h> // https://github.com/tzapu/WiFiManager

// Button pin for triggering reconnection/configuration
#define TRIGGER_PIN 4  // Change this to your preferred GPIO pin

WiFiManager wm;
unsigned long lastConnectionCheck = 0;
const unsigned long connectionCheckInterval = 30000; // Check every 30 seconds

void setup() {
    // Set WiFi mode
    WiFi.mode(WIFI_STA);
    
    Serial.begin(115200);
    Serial.println("\n Starting");
    
    // Configure trigger pin
    pinMode(TRIGGER_PIN, INPUT_PULLUP);
    
    // WiFiManager local initialization
    
    // Uncomment to reset settings for testing
    // wm.resetSettings();
    
    // Set config portal timeout (in seconds)
    wm.setConfigPortalTimeout(120); // 2 minutes timeout
    
    // Set callbacks
    wm.setAPCallback(configModeCallback);
    wm.setSaveConfigCallback(saveConfigCallback);
    
    // Custom parameters (optional)
    // WiFiManagerParameter custom_text("<p>Custom configuration</p>");
    // wm.addParameter(&custom_text);
    
    // Try to connect with saved credentials
    connectToWiFi();
}

void loop() {
    // Check button press for manual trigger
    checkTriggerButton();
    
    // Periodic connection check
    if (millis() - lastConnectionCheck > connectionCheckInterval) {
        lastConnectionCheck = millis();
        
        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("WiFi connection lost. Attempting to reconnect...");
            connectToWiFi();
        } else {
            Serial.print("WiFi connected. IP: ");
            Serial.println(WiFi.localIP());
        }
    }
    
    // Your main code here
    delay(1000);
}

void connectToWiFi() {
    Serial.println("Attempting WiFi connection...");
    
    // Automatically connect using saved credentials
    // If connection fails, start access point for configuration
    bool res = wm.autoConnect("AutoConnectAP", "password");
    
    if (!res) {
        Serial.println("Failed to connect or hit timeout");
        // Could restart ESP here if needed
        // ESP.restart();
    } else {
        Serial.println("Connected to WiFi successfully!");
        Serial.print("IP Address: ");
        Serial.println(WiFi.localIP());
        Serial.print("SSID: ");
        Serial.println(WiFi.SSID());
    }
}

void checkTriggerButton() {
    // Check if button is pressed (LOW due to INPUT_PULLUP)
    if (digitalRead(TRIGGER_PIN) == LOW) {
        delay(50); // Debounce
        if (digitalRead(TRIGGER_PIN) == LOW) {
            Serial.println("Button pressed - Starting configuration portal");
            
            // Disconnect current WiFi
            WiFi.disconnect();
            
            // Start configuration portal
            if (!wm.startConfigPortal("AutoConnectAP", "password")) {
                Serial.println("Failed to start config portal");
            } else {
                Serial.println("Config portal started successfully");
            }
            
            // Wait for button release
            while (digitalRead(TRIGGER_PIN) == LOW) {
                delay(100);
            }
        }
    }
}

// Callback when entering config mode
void configModeCallback(WiFiManager *myWiFiManager) {
    Serial.println("Entered config mode");
    Serial.println(WiFi.softAPIP());
    Serial.println(myWiFiManager->getConfigPortalSSID());
}

// Callback when config is saved
void saveConfigCallback() {
    Serial.println("Configuration saved");
    Serial.println("Restarting ESP...");
    delay(1000);
    ESP.restart();
}