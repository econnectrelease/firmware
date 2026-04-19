/* Copyright (c) 2026 Đinh Trung Kiên. All rights reserved. */

#include "board_support.h"
#include <Arduino.h>
#include <ArduinoJson.h>
#include <MD5Builder.h>
#include <PubSubClient.h>
#include <Wire.h>

#ifdef ECONNECT_HAS_DHT
#include <DHT.h>
#endif

#if __has_include("generated_firmware_config.h")
#include "generated_firmware_config.h"
#endif
#if __has_include("firmware_revision.h")
#include "firmware_revision.h"
#endif
#include "secrets.h"

#define DEVICE_MODE_STRING "no-code"

#ifndef ECONNECT_FIRMWARE_REVISION
#define ECONNECT_FIRMWARE_REVISION "1.0.0"
#endif

#ifndef ECONNECT_DEBUG_SERIAL
#define ECONNECT_DEBUG_SERIAL 0
#endif

#ifndef ECONNECT_WIFI_SCAN_BEFORE_CONNECT
#define ECONNECT_WIFI_SCAN_BEFORE_CONNECT 0
#endif

#ifndef ECONNECT_HEARTBEAT_INTERVAL_MS
#define ECONNECT_HEARTBEAT_INTERVAL_MS 5000UL
#endif

#ifndef ECONNECT_HANDSHAKE_RETRY_DELAY_MS
#define ECONNECT_HANDSHAKE_RETRY_DELAY_MS 5000UL
#endif

#ifndef ECONNECT_HANDSHAKE_ACK_TIMEOUT_MS
#define ECONNECT_HANDSHAKE_ACK_TIMEOUT_MS 5000UL
#endif

#ifndef ECONNECT_MQTT_RECONNECT_RETRY_DELAY_MS
#define ECONNECT_MQTT_RECONNECT_RETRY_DELAY_MS 2500UL
#endif

#ifndef ECONNECT_MQTT_KEEPALIVE_SECONDS
#define ECONNECT_MQTT_KEEPALIVE_SECONDS 10
#endif

#ifndef ECONNECT_MQTT_SOCKET_TIMEOUT_SECONDS
#define ECONNECT_MQTT_SOCKET_TIMEOUT_SECONDS 3
#endif

#ifndef ECONNECT_WIFI_CONNECT_TIMEOUT_MS
#define ECONNECT_WIFI_CONNECT_TIMEOUT_MS 12000UL
#endif

#ifndef ECONNECT_HAS_PIN_CONFIGS
struct EConnectPinConfig {
  int gpio;
  const char *mode;
  const char *function_name;
  const char *label;
  int active_level;
  int pwm_min;
  int pwm_max;
  const char *i2c_role;
  const char *i2c_address;
  const char *i2c_library;
  const char *i2c_device_version;
  const char *input_type;
  const char *switch_type;
  const char *dht_version;
};

static const EConnectPinConfig ECONNECT_PIN_CONFIGS[] = {
    {ECONNECT_BUILTIN_LED_PIN, "OUTPUT", "builtin_led", "Built-in LED", 1, 0,
     255, "", "", "", "", "switch", "momentary", ""},
};
#endif

constexpr unsigned long HEARTBEAT_INTERVAL_MS =
    ECONNECT_HEARTBEAT_INTERVAL_MS;
constexpr unsigned long HANDSHAKE_RETRY_DELAY_MS =
    ECONNECT_HANDSHAKE_RETRY_DELAY_MS;
constexpr unsigned long HANDSHAKE_ACK_TIMEOUT_MS =
    ECONNECT_HANDSHAKE_ACK_TIMEOUT_MS;
constexpr unsigned long MQTT_RECONNECT_RETRY_DELAY_MS =
    ECONNECT_MQTT_RECONNECT_RETRY_DELAY_MS;
constexpr uint16_t MQTT_KEEPALIVE_SECONDS = ECONNECT_MQTT_KEEPALIVE_SECONDS;
constexpr uint16_t MQTT_SOCKET_TIMEOUT_SECONDS =
    ECONNECT_MQTT_SOCKET_TIMEOUT_SECONDS;
constexpr unsigned long WIFI_CONNECT_TIMEOUT_MS =
    ECONNECT_WIFI_CONNECT_TIMEOUT_MS;

struct PinRuntimeState {
  int gpio;
  const char *mode;
  const char *functionName;
  const char *label;
  int activeLevel;
  int value;
  int brightness;
  int pwmMin;
  int pwmMax;
  int lastPhysicalValue;
  int stablePhysicalValue;
  unsigned long lastDebounceTime;
  float temperature;
  float humidity;
};

constexpr size_t PIN_CONFIG_COUNT =
    sizeof(ECONNECT_PIN_CONFIGS) / sizeof(ECONNECT_PIN_CONFIGS[0]);
constexpr size_t PIN_STATE_CAPACITY =
    PIN_CONFIG_COUNT > 0 ? PIN_CONFIG_COUNT : 1;

WiFiClient espClient;
PubSubClient mqttClient(espClient);
PinRuntimeState pinStates[PIN_STATE_CAPACITY];

#ifdef ECONNECT_HAS_DHT
DHT *dhtSensors[PIN_STATE_CAPACITY] = {nullptr};
unsigned long lastDHTReadTime[PIN_STATE_CAPACITY] = {0};
#endif

// Tachometer state
volatile unsigned long tachoPulseCounts[PIN_STATE_CAPACITY] = {0};
unsigned long lastTachoReadTime[PIN_STATE_CAPACITY] = {0};

#ifndef IRAM_ATTR
#define IRAM_ATTR
#endif

void IRAM_ATTR tachoInterruptHandler(void *arg) {
  int idx = (int)(intptr_t)arg;
  tachoPulseCounts[idx]++;
}

String deviceId = ECONNECT_DEVICE_ID;
unsigned long lastHeartbeatAt = 0;
bool securePairingVerified = false;
bool forcePairingRequestOnNextHandshake = false;
bool pairingRejectedUntilPowerCycle = false;
bool manualReflashRequired = false;
unsigned long lastReconnectAttemptAt = 0;
bool deferredStatePublishPending = false;
bool deferredStatePublishApplied = false;

bool connectToWiFi();
bool performSecureHandshake();
void setupMQTT();
void reconnectMQTT();
void mqttCallback(char *topic, byte *payload, unsigned int length);
void publishState(bool applied);
void publishFailedCommandAck(const String &commandId);
void publishPinCommandAckState(const EConnectPinConfig &config,
                               const PinRuntimeState &pinState,
                               const String &commandId);

void queueDeferredStatePublish(bool applied);
void flushDeferredStatePublish();
String commandTopic();
String registerTopic();
String registerAckTopic();
String stateAckTopic();
void subscribeCommandTopic();
void subscribeRegisterAckTopic();
void subscribeStateAckTopic();
void initializePinStates();
void initializeI2CBus();
int findPinIndex(int gpio);
void appendPinConfigMetadata(JsonObject pin, const EConnectPinConfig &config);
void appendRuntimePinState(JsonObject pin, PinRuntimeState &pinState,
                           const EConnectPinConfig &config,
                           bool includeMetadata);
bool modeEquals(const char *left, const char *right);
bool isOutputMode(const char *mode);
bool isPwmMode(const char *mode);
bool isNumericInputMode(const char *mode, const char *inputType);
bool isReadableMode(const char *mode);
bool isPwmInverted(const PinRuntimeState &pinState);
int pwmLowerBound(const PinRuntimeState &pinState);
int pwmUpperBound(const PinRuntimeState &pinState);
int pwmOffOutputValue(const PinRuntimeState &pinState);
int pwmOnOutputValue(const PinRuntimeState &pinState);
int clampPwmBrightness(const PinRuntimeState &pinState, int brightness);
int resolvePwmLogicalValue(const PinRuntimeState &pinState, int brightness);
int readRuntimeValue(PinRuntimeState &pinState);
int readRuntimeBrightness(PinRuntimeState &pinState);
bool applyCommandToPin(PinRuntimeState &pinState, int value, int brightness);
int resolvePhysicalLevel(const PinRuntimeState &pinState, int logicalValue);
#if ECONNECT_WIFI_SCAN_BEFORE_CONNECT
WifiTarget scanWifiTarget();
void logVisibleWifiTargets(const WifiTarget &target);
void formatBssid(const uint8_t *bssid, char *buffer, size_t bufferSize);
#endif
const char *getActiveWifiSsid();
const char *wifiPassphrase();
size_t totalReportedPinCount();
template <typename TDocument> void appendEmbeddedNetworkTargets(TDocument &doc);
template <typename TDocument>
void appendStateEnvelope(TDocument &doc, bool applied,
                         const String *commandId = nullptr);
bool runtimeNetworkDiffers(JsonVariantConst runtimeNetwork);
void requireManualReflash(JsonVariantConst runtimeNetwork,
                          const String &message);
const char *wifiStatusName(wl_status_t status);
void logConnectivitySnapshot(const char *context);
void markMqttConnectionFaulted(const char *context);
bool publishMqttPayload(const String &topic, const String &payload,
                        const char *context);
void logPublishedPayload(const char *context, const String &payload);

void setup() {
  Serial.begin(115200);
  delay(1500);
  Serial.println("\n--- Starting E-Connect K-Firmware ---");
  pairingRejectedUntilPowerCycle = restoreRejectedPairingLock();
  Serial.printf("Reset reason: %s | persisted reject lock: %s\n",
                boardResetReasonSummary().c_str(),
                pairingRejectedUntilPowerCycle ? "true" : "false");
  initializeBoardNetworking();

  if (pairingRejectedUntilPowerCycle) {
    shutdownBoardNetworkingAfterPairingReject();
    Serial.println("Reject lock restored. Keeping Wi-Fi and MQTT offline until "
                   "power loss.");
    return;
  }

  initializePinStates();
  initializeI2CBus();
  Serial.printf("Provisioned MQTT broker: %s:%d\n", MQTT_BROKER, MQTT_PORT);
  Serial.printf("Provisioned API base URL: %s\n", API_BASE_URL);



  while (!connectToWiFi()) {
    Serial.println("Wi-Fi provisioning failed. Retrying...");
    delay(HANDSHAKE_RETRY_DELAY_MS);
  }

  setupMQTT();

  while (!securePairingVerified && !pairingRejectedUntilPowerCycle &&
         !manualReflashRequired) {
    if (performSecureHandshake()) {
      break;
    }
    if (pairingRejectedUntilPowerCycle || manualReflashRequired) {
      break;
    }
    Serial.println("Secure handshake failed. Retrying...");
    delay(HANDSHAKE_RETRY_DELAY_MS);
  }
}

void updateInputs() {
  unsigned long now = millis();
  bool stateChanged = false;

  for (size_t i = 0; i < PIN_CONFIG_COUNT; i++) {
    if (modeEquals(pinStates[i].mode, "INPUT") &&
        strcmp(ECONNECT_PIN_CONFIGS[i].input_type, "switch") == 0) {
      if (strcmp(ECONNECT_PIN_CONFIGS[i].switch_type, "momentary_toggle") ==
          0) {
        int currentRaw = digitalRead(pinStates[i].gpio);
        if (currentRaw != pinStates[i].lastPhysicalValue) {
          pinStates[i].lastDebounceTime = now;
          pinStates[i].lastPhysicalValue = currentRaw;
        }

        if ((now - pinStates[i].lastDebounceTime) > 50) {
          if (currentRaw != pinStates[i].stablePhysicalValue) {
            int oldStable = pinStates[i].stablePhysicalValue;
            pinStates[i].stablePhysicalValue = currentRaw;

            if (oldStable != -1) {
              int activeState = pinStates[i].activeLevel;
              // If it transitioned from inactive to active, toggle the logical
              // value
              if (currentRaw == activeState) {
                pinStates[i].value = pinStates[i].value == 0 ? 1 : 0;
                // Output pins bound to this logic will be handled if any...
                // wait, we only report state.
                stateChanged = true;
              }
            }
          }
        }
      } else if (strcmp(ECONNECT_PIN_CONFIGS[i].switch_type, "momentary") ==
                     0 ||
                 strcmp(ECONNECT_PIN_CONFIGS[i].switch_type, "toggle") == 0) {
        // Immediate reaction for regular switches if state changed
        int currentRaw = digitalRead(pinStates[i].gpio);
        if (currentRaw != pinStates[i].lastPhysicalValue) {
          pinStates[i].lastDebounceTime = now;
          pinStates[i].lastPhysicalValue = currentRaw;
        }

        if ((now - pinStates[i].lastDebounceTime) > 50) {
          if (currentRaw != pinStates[i].stablePhysicalValue) {
            pinStates[i].stablePhysicalValue = currentRaw;
            int newLogical = (currentRaw == pinStates[i].activeLevel) ? 1 : 0;
            if (newLogical != pinStates[i].value) {
              pinStates[i].value = newLogical;
              stateChanged = true;
            }
          }
        }
      }
    }
  }

  if (stateChanged && securePairingVerified) {
    publishState(true);
  }
}

void loop() {
  if (pairingRejectedUntilPowerCycle || manualReflashRequired) {
    delay(HANDSHAKE_RETRY_DELAY_MS);
    return;
  }



  if (!securePairingVerified) {
    securePairingVerified = performSecureHandshake();
    delay(HANDSHAKE_RETRY_DELAY_MS);
    return;
  }

  if (WiFi.status() != WL_CONNECTED) {
    if (!connectToWiFi()) {
      delay(HANDSHAKE_RETRY_DELAY_MS);
      return;
    }
  }

  if (!mqttClient.connected()) {
    reconnectMQTT();
  }

  if (mqttClient.connected() && !mqttClient.loop()) {
    Serial.println("mqttClient.loop() reported a transport problem.");
    markMqttConnectionFaulted("mqttClient.loop()");
  }



  flushDeferredStatePublish();

  if (millis() - lastHeartbeatAt >= HEARTBEAT_INTERVAL_MS) {
    publishState(false);
    lastHeartbeatAt = millis();
  }

  updateInputs();

  delay(10);
}

bool connectToWiFi() {
  if (strlen(getActiveWifiSsid()) == 0) {
    Serial.println("BLOCKER: WIFI_SSID is empty. Build firmware from the "
                   "server before flashing.");
    return false;
  }

  prepareBoardForWifiConnection();
  delay(250);

  auto awaitWifiConnection = []() {
    const unsigned long startedAt = millis();
    unsigned long lastDotAt = 0;
    while (WiFi.status() != WL_CONNECTED &&
           millis() - startedAt < WIFI_CONNECT_TIMEOUT_MS) {
      delay(50);
      if (millis() - lastDotAt >= 500) {
        Serial.print(".");
        lastDotAt = millis();
      }
    }
    return WiFi.status() == WL_CONNECTED;
  };

#if ECONNECT_WIFI_SCAN_BEFORE_CONNECT
  const WifiTarget target = scanWifiTarget();
  logVisibleWifiTargets(target);

  auto beginWifiConnection = [&](bool useLockedTarget, bool useChannelHint) {
    Serial.printf("Connecting to Wi-Fi SSID: %s ", getActiveWifiSsid());
    if (useLockedTarget && target.found) {
      char bssidBuffer[18];
      formatBssid(target.bssid, bssidBuffer, sizeof(bssidBuffer));
      Serial.printf("(locked channel=%d bssid=%s auth=%s rssi=%d) ",
                    target.channel, bssidBuffer,
                    boardAuthModeName(target.authMode), target.rssi);
      WiFi.begin(getActiveWifiSsid(), wifiPassphrase(), target.channel,
                 target.bssid, true);
      return awaitWifiConnection();
    }

    if (useChannelHint && target.found) {
      Serial.printf("(channel-hint=%d) ", target.channel);
      WiFi.begin(getActiveWifiSsid(), wifiPassphrase(), target.channel);
      return awaitWifiConnection();
    }

    Serial.print("(broadcast lookup) ");
    WiFi.begin(getActiveWifiSsid(), wifiPassphrase());
    return awaitWifiConnection();
  };

  bool connected = beginWifiConnection(false, false);
  if (!connected && target.found) {
    Serial.println(" failed");
    Serial.printf("Wi-Fi status code: %d\n", static_cast<int>(WiFi.status()));
    Serial.println(
        "Generic connect failed. Retrying with matched channel hint.");
    WiFi.disconnect(false, true);
    delay(500);
    connected = beginWifiConnection(false, true);
  }
  if (!connected && target.found && target.candidateCount == 1) {
    Serial.println(" failed");
    Serial.printf("Wi-Fi status code: %d\n", static_cast<int>(WiFi.status()));
    Serial.println(
        "Channel-hint connect failed. Retrying with exact BSSID lock.");
    WiFi.disconnect(false, true);
    delay(500);
    connected = beginWifiConnection(true, true);
  }
#else
  Serial.printf("Connecting to Wi-Fi SSID: %s (broadcast lookup) ",
                getActiveWifiSsid());
  WiFi.begin(getActiveWifiSsid(), wifiPassphrase());
  const bool connected = awaitWifiConnection();
#endif

  if (connected) {
    Serial.println(" connected");
    Serial.printf("IP Address: %s\n", WiFi.localIP().toString().c_str());
    return true;
  }

  Serial.println(" failed");
  Serial.printf("Wi-Fi status code: %d\n", static_cast<int>(WiFi.status()));
  return false;
}

#if ECONNECT_WIFI_SCAN_BEFORE_CONNECT
WifiTarget scanWifiTarget() {
  WifiTarget target = {
      false, 0, 0, -127, defaultBoardAuthMode(), {0, 0, 0, 0, 0, 0},
  };
  const int networkCount = WiFi.scanNetworks(false, true);
  if (networkCount <= 0) {
    Serial.printf("Visible Wi-Fi networks: %d\n", networkCount);
    return target;
  }

  for (int index = 0; index < networkCount; index++) {
    const String ssid = WiFi.SSID(index);
    if (!ssid.equals(getActiveWifiSsid())) {
      continue;
    }

    target.candidateCount++;
    const int32_t rssi = WiFi.RSSI(index);
    if (target.found && rssi <= target.rssi) {
      continue;
    }

    target.found = true;
    target.channel = WiFi.channel(index);
    target.rssi = rssi;
    target.authMode = WiFi.encryptionType(index);
    memcpy(target.bssid, WiFi.BSSID(index), sizeof(target.bssid));
  }

  return target;
}

void logVisibleWifiTargets(const WifiTarget &target) {
  const int networkCount = WiFi.scanComplete();
  Serial.printf("Visible Wi-Fi networks: %d\n", networkCount);
  if (networkCount <= 0) {
    return;
  }

  if (!target.found) {
    Serial.println(
        "Target SSID not found in scan. Falling back to broadcast lookup.");
    return;
  }

  char bssidBuffer[18];
  formatBssid(target.bssid, bssidBuffer, sizeof(bssidBuffer));
  Serial.printf(
      "Matched AP: ssid=%s candidates=%d channel=%d rssi=%d auth=%s bssid=%s\n",
      getActiveWifiSsid(), target.candidateCount, target.channel, target.rssi,
      boardAuthModeName(target.authMode), bssidBuffer);
}

void formatBssid(const uint8_t *bssid, char *buffer, size_t bufferSize) {
  snprintf(buffer, bufferSize, "%02X:%02X:%02X:%02X:%02X:%02X", bssid[0],
           bssid[1], bssid[2], bssid[3], bssid[4], bssid[5]);
}
#endif

const char *getActiveWifiSsid() {

  return WIFI_SSID;
}

const char *wifiPassphrase() {

  return strlen(WIFI_PASS) > 0 ? WIFI_PASS : nullptr;
}

const char *wifiStatusName(wl_status_t status) {
  switch (status) {
#ifdef WL_IDLE_STATUS
  case WL_IDLE_STATUS:
    return "WL_IDLE_STATUS";
#endif
#ifdef WL_NO_SSID_AVAIL
  case WL_NO_SSID_AVAIL:
    return "WL_NO_SSID_AVAIL";
#endif
#ifdef WL_SCAN_COMPLETED
  case WL_SCAN_COMPLETED:
    return "WL_SCAN_COMPLETED";
#endif
#ifdef WL_CONNECTED
  case WL_CONNECTED:
    return "WL_CONNECTED";
#endif
#ifdef WL_CONNECT_FAILED
  case WL_CONNECT_FAILED:
    return "WL_CONNECT_FAILED";
#endif
#ifdef WL_CONNECTION_LOST
  case WL_CONNECTION_LOST:
    return "WL_CONNECTION_LOST";
#endif
#ifdef WL_DISCONNECTED
  case WL_DISCONNECTED:
    return "WL_DISCONNECTED";
#endif
  default:
    return "WL_UNKNOWN";
  }
}

void logConnectivitySnapshot(const char *context) {
  const wl_status_t wifiStatus = WiFi.status();
  const String ipAddress = WiFi.localIP().toString();
  Serial.printf(
      "%s | WiFi.status()=%d (%s) | mqttClient.state()=%d | "
      "mqttClient.connected()=%s | IP=%s\n",
      context, static_cast<int>(wifiStatus), wifiStatusName(wifiStatus),
      mqttClient.state(), mqttClient.connected() ? "true" : "false",
      ipAddress.c_str());
}

void markMqttConnectionFaulted(const char *context) {
  Serial.printf("%s | Forcing MQTT disconnect to recover transport.\n",
                context);
  logConnectivitySnapshot(context);
  if (mqttClient.connected()) {
    mqttClient.disconnect();
  }
  lastReconnectAttemptAt = millis() - MQTT_RECONNECT_RETRY_DELAY_MS;
}

bool publishMqttPayload(const String &topic, const String &payload,
                        const char *context) {
  if (!mqttClient.connected()) {
    Serial.printf("%s skipped because MQTT is disconnected.\n", context);
    logConnectivitySnapshot(context);
    return false;
  }

  if (mqttClient.publish(topic.c_str(), payload.c_str())) {
    return true;
  }

  Serial.printf("%s publish failed.\n", context);
  markMqttConnectionFaulted(context);
  return false;
}

bool performSecureHandshake() {
  if (WiFi.status() != WL_CONNECTED) {
    return false;
  }

  if (strlen(MQTT_BROKER) == 0 || strlen(ECONNECT_PROJECT_ID) == 0 ||
      strlen(ECONNECT_SECRET_KEY) == 0) {
    Serial.println("BLOCKER: Missing secure pairing metadata in firmware.");
    return false;
  }

  if (!mqttClient.connected()) {
    reconnectMQTT();
  }

  if (!mqttClient.connected()) {
    return false;
  }

  DynamicJsonDocument doc(4096);
  doc["device_id"] = deviceId;
  doc["project_id"] = ECONNECT_PROJECT_ID;
  doc["secret_key"] = ECONNECT_SECRET_KEY;
  doc["force_pairing_request"] = forcePairingRequestOnNextHandshake;
  doc["mac_address"] = WiFi.macAddress();
  doc["ip_address"] = WiFi.localIP().toString();
  doc["name"] = ECONNECT_DEVICE_NAME;
  doc["mode"] = DEVICE_MODE_STRING;
  doc["firmware_revision"] = ECONNECT_FIRMWARE_REVISION;
  doc["firmware_version"] = ECONNECT_FIRMWARE_VERSION;
  appendEmbeddedNetworkTargets(doc);

  JsonArray pins = doc.createNestedArray("pins");
  for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
    JsonObject pin = pins.createNestedObject();
    pin["gpio_pin"] = ECONNECT_PIN_CONFIGS[index].gpio;
    pin["mode"] = ECONNECT_PIN_CONFIGS[index].mode;
    appendPinConfigMetadata(pin, ECONNECT_PIN_CONFIGS[index]);
  }


  String requestBody;
  serializeJson(doc, requestBody);
  const String topic = registerTopic();
  Serial.printf("Publishing secure handshake to MQTT topic: %s\n",
                topic.c_str());

  if (!publishMqttPayload(topic, requestBody, "Secure handshake")) {
    return false;
  }

  const unsigned long startedAt = millis();
  while (!securePairingVerified && !pairingRejectedUntilPowerCycle &&
         millis() - startedAt < HANDSHAKE_ACK_TIMEOUT_MS) {
    if (!mqttClient.connected()) {
      Serial.println(
          "MQTT disconnected while waiting for pairing acknowledgement.");
      break;
    }
    if (!mqttClient.loop()) {
      Serial.println(
          "mqttClient.loop() failed while waiting for pairing acknowledgement.");
      markMqttConnectionFaulted("Secure handshake ack wait");
      break;
    }
    delay(10);
  }

  if (pairingRejectedUntilPowerCycle) {
    Serial.println("Pairing was rejected by the server. Waiting for reboot "
                   "before retrying.");
    return false;
  }

  if (manualReflashRequired) {
    Serial.println(
        "Manual reflash is required before this board can resume pairing.");
    return false;
  }

  if (!securePairingVerified) {
    Serial.println("Timed out waiting for MQTT pairing acknowledgement.");
  }

  return securePairingVerified;
}

void setupMQTT() {
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(4096);
  mqttClient.setKeepAlive(MQTT_KEEPALIVE_SECONDS);
  mqttClient.setSocketTimeout(MQTT_SOCKET_TIMEOUT_SECONDS);
  Serial.printf("MQTT configured with keepalive=%us socket_timeout=%us\n",
                MQTT_KEEPALIVE_SECONDS, MQTT_SOCKET_TIMEOUT_SECONDS);
}

String commandTopic() {
  return String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId +
         "/command";
}

String registerTopic() {
  return String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId +
         "/register";
}

String registerAckTopic() {
  return String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId +
         "/register/ack";
}

String stateAckTopic() {
  return String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId +
         "/state/ack";
}

void subscribeCommandTopic() {
  const String topic = commandTopic();
  mqttClient.subscribe(topic.c_str());
  Serial.printf("Subscribed to command topic: %s\n", topic.c_str());
}

void subscribeRegisterAckTopic() {
  const String topic = registerAckTopic();
  mqttClient.subscribe(topic.c_str());
  Serial.printf("Subscribed to pairing ack topic: %s\n", topic.c_str());
}

void subscribeStateAckTopic() {
  const String topic = stateAckTopic();
  mqttClient.subscribe(topic.c_str());
  Serial.printf("Subscribed to heartbeat ack topic: %s\n", topic.c_str());
}

void reconnectMQTT() {
  if (mqttClient.connected())
    return;

  if (WiFi.status() != WL_CONNECTED) {
    return;
  }

  unsigned long now = millis();
  if (now - lastReconnectAttemptAt < MQTT_RECONNECT_RETRY_DELAY_MS)
    return;

  lastReconnectAttemptAt = now;

  String clientId = "econnect-";
  clientId += deviceId;
  clientId += "-";
  clientId += String(random(0xffff), HEX);

  logConnectivitySnapshot("Attempting MQTT reconnect");
  if (mqttClient.connect(clientId.c_str())) {
    Serial.println("MQTT connected.");
    subscribeRegisterAckTopic();
    subscribeStateAckTopic();
    if (securePairingVerified) {
      subscribeCommandTopic();

      publishState(true);
      lastHeartbeatAt = millis();
    }
  } else {
    Serial.print("MQTT connect failed, rc=");
    Serial.println(mqttClient.state());
    logConnectivitySnapshot("MQTT reconnect failed");
    Serial.println("If the server was moved to a new IP, rebuild and reflash "
                   "this board so the embedded server/MQTT host is updated.");
  }
}

void mqttCallback(char *topic, byte *payload, unsigned int length) {
  Serial.print("MQTT message received on topic: ");
  Serial.println(topic);

  const size_t jsonCapacity = static_cast<size_t>(length) + 768;
  DynamicJsonDocument doc(jsonCapacity);
  const DeserializationError error =
      deserializeJson(doc, reinterpret_cast<const char *>(payload), length);
  if (error) {
    Serial.printf("Failed to parse MQTT JSON (len=%u, capacity=%u): %s\n",
                  length, static_cast<unsigned int>(jsonCapacity),
                  error.c_str());
    return;
  }

  const String incomingTopic = String(topic);

  if (incomingTopic == registerAckTopic()) {
    if (String(doc["status"] | "") == "manual_reflash_required" ||
        runtimeNetworkDiffers(doc["runtime_network"])) {
      requireManualReflash(
          doc["runtime_network"],
          String(
              doc["message"] |
              "Server reports the current firmware network target is stale."));
      return;
    }

    const bool verified = doc["secret_verified"] | false;
    if (!verified || String(doc["status"] | "") != "ok") {
      Serial.printf("Secure MQTT handshake rejected: %s\n",
                    String(doc["message"] | "Unknown error").c_str());
      return;
    }

    const String assignedDeviceId = doc["device_id"] | deviceId;
    if (assignedDeviceId.length() > 0) {
      deviceId = assignedDeviceId;
    }

    securePairingVerified = true;
    pairingRejectedUntilPowerCycle = false;
    forcePairingRequestOnNextHandshake = false;
    persistRejectedPairingLock(false);
    subscribeRegisterAckTopic();
    subscribeStateAckTopic();
    subscribeCommandTopic();

    publishState(true);
    lastHeartbeatAt = millis();
    Serial.printf("Secure MQTT handshake complete. Device id: %s\n",
                  deviceId.c_str());
    return;
  }

  if (incomingTopic == stateAckTopic()) {
    const String status = doc["status"] | "";
    if (status == "manual_reflash_required" ||
        runtimeNetworkDiffers(doc["runtime_network"])) {
      requireManualReflash(
          doc["runtime_network"],
          String(
              doc["message"] |
              "Server reports the current firmware network target is stale."));
      return;
    }
    if (status == "pairing_rejected") {
      pairingRejectedUntilPowerCycle = true;
      forcePairingRequestOnNextHandshake = false;
      securePairingVerified = false;
      persistRejectedPairingLock(true);

      if (mqttClient.connected()) {
        mqttClient.disconnect();
      }
      shutdownBoardNetworkingAfterPairingReject();
      Serial.printf(
          "Server rejected pairing: %s\n",
          String(doc["message"] | "Pairing rejected by server.").c_str());
      return;
    }
    if (pairingRejectedUntilPowerCycle) {
      Serial.println(
          "Ignoring re-pair request after server rejection until reboot.");
      return;
    }
    if (status == "re_pair_required") {
      pairingRejectedUntilPowerCycle = false;
      forcePairingRequestOnNextHandshake = true;
      securePairingVerified = false;
      persistRejectedPairingLock(false);

      Serial.printf("Server requested re-pair: %s\n",
                    String(doc["message"] | "Unknown reason").c_str());
    }
    return;
  }

  if (!securePairingVerified) {
    Serial.println("Ignoring MQTT command before secure pairing completes.");
    return;
  }

  if (String(doc["kind"] | "") == "system") {
    if (String(doc["action"] | "") == "ota") {
      const String url = doc["url"] | "";
      const String jobId = doc["job_id"] | "";
      const String expectedMd5 = doc["md5"] | "";
      const String expectedSignature = doc["signature"] | "";

      if (url.length() > 0) {
        Serial.printf("Received OTA command for URL: %s\n", url.c_str());

        bool signatureValid = false;
        if (expectedMd5.length() > 0 && expectedSignature.length() > 0) {
          MD5Builder md5;
          md5.begin();
          md5.add(expectedMd5 + String(ECONNECT_SECRET_KEY));
          md5.calculate();
          String calculatedSignature = md5.toString();
          signatureValid = (calculatedSignature == expectedSignature);
        }

        OtaUpdateResult otaResult;

        if (!signatureValid) {
          Serial.println(
              "OTA Error: Invalid signature or missing MD5. Update rejected.");
          otaResult.status = "failed";
          otaResult.message = "Invalid OTA signature";
          otaResult.shouldRestart = false;
        } else {
          otaResult = runBoardOtaUpdate(url, expectedMd5);
        }

        StaticJsonDocument<512> statusDoc;
        statusDoc["event"] = "ota_status";
        statusDoc["job_id"] = jobId;
        statusDoc["status"] = otaResult.status;
        if (otaResult.message.length() > 0) {
          statusDoc["message"] = otaResult.message;
          Serial.printf("OTA update %s: %s\n", otaResult.status,
                        otaResult.message.c_str());
        } else {
          Serial.printf("OTA update %s\n", otaResult.status);
        }

        String payload;
        serializeJson(statusDoc, payload);
        const String stateTopic = String("econnect/") + MQTT_NAMESPACE +
                                  "/device/" + deviceId + "/state";
        if (publishMqttPayload(stateTopic, payload, "OTA status")) {
          mqttClient.loop();
          delay(500);
        }

        if (otaResult.shouldRestart) {
          ESP.restart();
        }
      }
    }
    return;
  }

  if (String(doc["kind"] | "") != "action") {
    return;
  }

  const int targetPin = doc["pin"] | -1;
  const int value = doc["value"] | -1;
  const int brightness = doc["brightness"] | -1;
  const String commandId = doc["command_id"] | "";



  const int pinIndex = findPinIndex(targetPin);
  if (pinIndex < 0) {
    Serial.printf("Ignoring command for unmapped GPIO %d\n", targetPin);
    publishFailedCommandAck(commandId);
    return;
  }

  const bool applied =
      applyCommandToPin(pinStates[pinIndex], value, brightness);
  if (applied) {
    publishPinCommandAckState(ECONNECT_PIN_CONFIGS[pinIndex],
                              pinStates[pinIndex], commandId);
    queueDeferredStatePublish(true);
  } else {
    publishFailedCommandAck(commandId);
  }
}

void initializePinStates() {
  for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
    pinStates[index].gpio = ECONNECT_PIN_CONFIGS[index].gpio;
    pinStates[index].mode = ECONNECT_PIN_CONFIGS[index].mode;
    pinStates[index].functionName = ECONNECT_PIN_CONFIGS[index].function_name;
    pinStates[index].label = ECONNECT_PIN_CONFIGS[index].label;
    pinStates[index].activeLevel =
        ECONNECT_PIN_CONFIGS[index].active_level == 0 ? 0 : 1;
    pinStates[index].value = 0;
    pinStates[index].brightness = 0;
    pinStates[index].pwmMin = ECONNECT_PIN_CONFIGS[index].pwm_min;
    pinStates[index].pwmMax = ECONNECT_PIN_CONFIGS[index].pwm_max;
    pinStates[index].lastPhysicalValue = -1;
    pinStates[index].stablePhysicalValue = -1;
    pinStates[index].lastDebounceTime = 0;
    pinStates[index].temperature = NAN;
    pinStates[index].humidity = NAN;

    if (isOutputMode(pinStates[index].mode)) {
      pinMode(pinStates[index].gpio, OUTPUT);
      digitalWrite(pinStates[index].gpio,
                   resolvePhysicalLevel(pinStates[index], 0) == 1 ? HIGH : LOW);
    } else if (isPwmMode(pinStates[index].mode)) {
      pinStates[index].brightness = pwmOffOutputValue(pinStates[index]);
      pinMode(pinStates[index].gpio, OUTPUT);
      analogWrite(pinStates[index].gpio, pinStates[index].brightness);
    } else if (isReadableMode(pinStates[index].mode)) {
      pinMode(pinStates[index].gpio, INPUT);

      if (modeEquals(pinStates[index].mode, "INPUT")) {
        if (strcmp(ECONNECT_PIN_CONFIGS[index].input_type, "dht") == 0) {
#ifdef ECONNECT_HAS_DHT
          int dhtType = DHT11;
          if (strcmp(ECONNECT_PIN_CONFIGS[index].dht_version, "DHT22") == 0) {
            dhtType = DHT22;
          } else if (strcmp(ECONNECT_PIN_CONFIGS[index].dht_version, "DHT21") ==
                     0) {
            dhtType = DHT21;
          }
          dhtSensors[index] = new DHT(pinStates[index].gpio, dhtType);
          dhtSensors[index]->begin();
          Serial.printf("Initialized DHT%d on GPIO %d\n",
                        dhtType == DHT22 ? 22 : 11, pinStates[index].gpio);
#endif
        } else if (strcmp(ECONNECT_PIN_CONFIGS[index].input_type,
                          "tachometer") == 0) {
          pinMode(pinStates[index].gpio, INPUT_PULLUP);
          // Attach interrupt for tachometer
          attachInterruptArg(digitalPinToInterrupt(pinStates[index].gpio),
                             tachoInterruptHandler, (void *)(intptr_t)index,
                             RISING);
          lastTachoReadTime[index] = millis();
          Serial.printf("Initialized Tachometer Interrupt on GPIO %d\n",
                        pinStates[index].gpio);
        }
      }
    }
  }
}

void initializeI2CBus() {
  int sdaPin = -1;
  int sclPin = -1;

  // First pass: look for explicit roles
  for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
    if (!modeEquals(ECONNECT_PIN_CONFIGS[index].mode, "I2C"))
      continue;

    if (strcmp(ECONNECT_PIN_CONFIGS[index].i2c_role, "SDA") == 0) {
      sdaPin = ECONNECT_PIN_CONFIGS[index].gpio;
    } else if (strcmp(ECONNECT_PIN_CONFIGS[index].i2c_role, "SCL") == 0) {
      sclPin = ECONNECT_PIN_CONFIGS[index].gpio;
    }
  }

  // Second pass: fallback for legacy configs missing explicit roles
  if (sdaPin < 0 || sclPin < 0) {
    sdaPin = -1;
    sclPin = -1;
    for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
      if (!modeEquals(ECONNECT_PIN_CONFIGS[index].mode, "I2C"))
        continue;

      if (sdaPin < 0) {
        sdaPin = ECONNECT_PIN_CONFIGS[index].gpio;
      } else if (sclPin < 0) {
        sclPin = ECONNECT_PIN_CONFIGS[index].gpio;
        break;
      }
    }
  }

  if (sdaPin >= 0 && sclPin >= 0) {
    Wire.begin(sdaPin, sclPin);
    Serial.printf("Initialized I2C bus on SDA=%d SCL=%d\n", sdaPin, sclPin);
  }
}

int findPinIndex(int gpio) {
  for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
    if (pinStates[index].gpio == gpio) {
      return static_cast<int>(index);
    }
  }

  return -1;
}

bool modeEquals(const char *left, const char *right) {
  return strcmp(left, right) == 0;
}

bool isOutputMode(const char *mode) { return modeEquals(mode, "OUTPUT"); }

bool isPwmMode(const char *mode) { return modeEquals(mode, "PWM"); }

bool isNumericInputMode(const char *mode, const char *inputType) {
  if (isPwmMode(mode) || modeEquals(mode, "ADC")) {
    return true;
  }
  if (modeEquals(mode, "INPUT") && inputType != nullptr) {
    if (strcmp(inputType, "dht") == 0 || strcmp(inputType, "tachometer") == 0) {
      return true;
    }
  }
  return false;
}

bool isReadableMode(const char *mode) {
  return modeEquals(mode, "INPUT") || modeEquals(mode, "ADC") ||
         modeEquals(mode, "I2C");
}

bool isPwmInverted(const PinRuntimeState &pinState) {
  return pinState.pwmMin > pinState.pwmMax;
}

int pwmLowerBound(const PinRuntimeState &pinState) {
  return pinState.pwmMin < pinState.pwmMax ? pinState.pwmMin : pinState.pwmMax;
}

int pwmUpperBound(const PinRuntimeState &pinState) {
  return pinState.pwmMin > pinState.pwmMax ? pinState.pwmMin : pinState.pwmMax;
}

int pwmOffOutputValue(const PinRuntimeState &pinState) {
  return isPwmInverted(pinState) ? pinState.pwmMin : 0;
}

int pwmOnOutputValue(const PinRuntimeState &pinState) {
  return pinState.pwmMax;
}

int clampPwmBrightness(const PinRuntimeState &pinState, int brightness) {
  return constrain(brightness, pwmLowerBound(pinState),
                   pwmUpperBound(pinState));
}

int resolvePwmLogicalValue(const PinRuntimeState &pinState, int brightness) {
  return brightness == pwmOffOutputValue(pinState) ? 0 : 1;
}

int readRuntimeValue(PinRuntimeState &pinState) {
  if (isPwmMode(pinState.mode)) {
    pinState.value = resolvePwmLogicalValue(pinState, pinState.brightness);
    return pinState.value;
  }

  if (isOutputMode(pinState.mode)) {
    return pinState.value;
  }

  if (modeEquals(pinState.mode, "ADC")) {
    pinState.value = analogRead(pinState.gpio);
    return pinState.value;
  }

  if (modeEquals(pinState.mode, "INPUT")) {
    int index = findPinIndex(pinState.gpio);
    if (index >= 0) {
      if (strcmp(ECONNECT_PIN_CONFIGS[index].input_type, "dht") == 0) {
#ifdef ECONNECT_HAS_DHT
        if (dhtSensors[index] != nullptr) {
          unsigned long now = millis();
          if (now - lastDHTReadTime[index] >= 2000) {
            float t = dhtSensors[index]->readTemperature();
            float h = dhtSensors[index]->readHumidity();
            if (!isnan(t)) {
              pinState.temperature = t;
              pinState.value = (int)(t * 10);
            }
            if (!isnan(h)) {
              pinState.humidity = h;
            }
            lastDHTReadTime[index] = now;
          }
        }
#endif
        return pinState.value;
      } else if (strcmp(ECONNECT_PIN_CONFIGS[index].input_type, "tachometer") ==
                 0) {
        unsigned long now = millis();
        unsigned long elapsed = now - lastTachoReadTime[index];
        if (elapsed >= 1000) {
          noInterrupts();
          unsigned long pulses = tachoPulseCounts[index];
          tachoPulseCounts[index] = 0;
          interrupts();

          pinState.value = (int)((pulses * 60000ULL) / elapsed);
          lastTachoReadTime[index] = now;
        }
        return pinState.value;
      } else if (strcmp(ECONNECT_PIN_CONFIGS[index].input_type, "switch") ==
                 0) {
        // Switch values are updated by the polling loop (updateInputs),
        // returning the last known stable state
        return pinStates[index].value;
      }
    }

    // Default Digital Read
    pinState.value = digitalRead(pinState.gpio);
    return pinState.value;
  }

  if (modeEquals(pinState.mode, "I2C")) {
    pinState.value = digitalRead(pinState.gpio);
    return pinState.value;
  }

  return pinState.value;
}

int readRuntimeBrightness(PinRuntimeState &pinState) {
  return isPwmMode(pinState.mode) ? pinState.brightness : 0;
}

bool applyCommandToPin(PinRuntimeState &pinState, int value, int brightness) {
  if (isOutputMode(pinState.mode) && value != -1) {
    pinState.value = value == 0 ? 0 : 1;
    digitalWrite(pinState.gpio,
                 resolvePhysicalLevel(pinState, pinState.value) == 1 ? HIGH
                                                                     : LOW);
    return true;
  }

  if (isPwmMode(pinState.mode)) {
    int nextBrightness = brightness;
    if (value == 0) {
      nextBrightness = pwmOffOutputValue(pinState);
    } else if (nextBrightness < 0 && value != -1) {
      nextBrightness = pwmOnOutputValue(pinState);
    }

    if (nextBrightness < 0) {
      return false;
    }

    if (value != 0) {
      // Treat brightness as a raw analog output value clamped to the configured
      // boundaries.
      nextBrightness = clampPwmBrightness(pinState, nextBrightness);
    }

    pinState.brightness = nextBrightness;
    pinState.value = resolvePwmLogicalValue(pinState, nextBrightness);
    analogWrite(pinState.gpio, nextBrightness);
    return true;
  }

  return false;
}

int resolvePhysicalLevel(const PinRuntimeState &pinState, int logicalValue) {
  const int normalizedLogical = logicalValue == 0 ? 0 : 1;
  const int activeLevel = pinState.activeLevel == 0 ? 0 : 1;
  return normalizedLogical == 1 ? activeLevel : (activeLevel == 1 ? 0 : 1);
}

template <typename TDocument>
void appendStateEnvelope(TDocument &doc, bool applied,
                         const String *commandId) {
  doc["kind"] = "state";
  doc["device_id"] = deviceId;
  doc["applied"] = applied;
  doc["firmware_revision"] = ECONNECT_FIRMWARE_REVISION;
  doc["firmware_version"] = ECONNECT_FIRMWARE_VERSION;
  doc["ip_address"] = WiFi.localIP().toString();
  if (commandId != nullptr && commandId->length() > 0) {
    doc["command_id"] = *commandId;
  }
  appendEmbeddedNetworkTargets(doc);
}

void publishFailedCommandAck(const String &commandId) {
  if (!mqttClient.connected()) {
    return;
  }

  const String stateTopic =
      String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId + "/state";

  DynamicJsonDocument doc(512);
  appendStateEnvelope(doc, false, &commandId);

  String payload;
  serializeJson(doc, payload);

  if (publishMqttPayload(stateTopic, payload,
                         "Failed command acknowledgement")) {
    logPublishedPayload("Published failed command acknowledgement payload",
                        payload);
  }
}

void publishPinCommandAckState(const EConnectPinConfig &config,
                               const PinRuntimeState &pinState,
                               const String &commandId) {
  if (!mqttClient.connected()) {
    return;
  }

  const String stateTopic =
      String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId + "/state";
  const bool pwmMode = isPwmMode(config.mode);

  DynamicJsonDocument doc(1024);
  appendStateEnvelope(doc, true, &commandId);
  doc["pin"] = pinState.gpio;
  doc["mode"] = pinState.mode;
  doc["value"] = pinState.value;
  if (pwmMode) {
    doc["brightness"] = pinState.brightness;
  }

  JsonArray pins = doc.createNestedArray("pins");
  JsonObject pin = pins.createNestedObject();
  pin["pin"] = pinState.gpio;
  pin["mode"] = pinState.mode;
  pin["value"] = pinState.value;
  if (isOutputMode(config.mode)) {
    pin["active_level"] = pinState.activeLevel;
  }
  appendPinConfigMetadata(pin, config);
  if (pwmMode) {
    pin["brightness"] = pinState.brightness;
  }

  String payload;
  serializeJson(doc, payload);

  if (publishMqttPayload(stateTopic, payload,
                         "Fast command acknowledgement")) {
    logPublishedPayload("Published fast command acknowledgement payload",
                        payload);
  }
}



void queueDeferredStatePublish(bool applied) {
  deferredStatePublishPending = true;
  deferredStatePublishApplied = applied;
}

void flushDeferredStatePublish() {
  if (!deferredStatePublishPending || !mqttClient.connected()) {
    return;
  }

  const bool applied = deferredStatePublishApplied;
  deferredStatePublishPending = false;
  publishState(applied);
}

void publishState(bool applied) {
  if (!mqttClient.connected()) {
    return;
  }

  const String stateTopic =
      String("econnect/") + MQTT_NAMESPACE + "/device/" + deviceId + "/state";

  const bool compactHeartbeat = !applied;
  DynamicJsonDocument doc(compactHeartbeat ? 2048 : 4096);
  appendStateEnvelope(doc, applied);

  JsonArray pins = doc.createNestedArray("pins");
  for (size_t index = 0; index < PIN_CONFIG_COUNT; index++) {
    PinRuntimeState &pinState = pinStates[index];
    JsonObject pin = pins.createNestedObject();
    appendRuntimePinState(pin, pinState, ECONNECT_PIN_CONFIGS[index],
                          !compactHeartbeat);
  }


  if (totalReportedPinCount() == 1) {

    {
      PinRuntimeState &pinState = pinStates[0];
      doc["pin"] = pinState.gpio;
      doc["value"] = readRuntimeValue(pinState);
      if (strcmp(ECONNECT_PIN_CONFIGS[0].input_type, "dht") == 0) {
        if (!isnan(pinState.temperature)) {
          doc["temperature"] = serialized(String(pinState.temperature, 1));
        }
        if (!isnan(pinState.humidity)) {
          doc["humidity"] = serialized(String(pinState.humidity, 1));
        }
      }
      if (isPwmMode(pinState.mode)) {
        doc["brightness"] = readRuntimeBrightness(pinState);
      }
    }
  }

  String payload;
  serializeJson(doc, payload);

  if (publishMqttPayload(stateTopic, payload,
                         applied ? "Applied state payload"
                                 : "Heartbeat state payload")) {
    logPublishedPayload(applied ? "Published applied state payload"
                                : "Published heartbeat state payload",
                        payload);
  }
}

void appendRuntimePinState(JsonObject pin, PinRuntimeState &pinState,
                           const EConnectPinConfig &config,
                           bool includeMetadata) {
  pin["pin"] = pinState.gpio;
  if (includeMetadata) {
    pin["mode"] = pinState.mode;
  }

  const int runtimeValue = readRuntimeValue(pinState);
  pin["value"] = runtimeValue;

  if (includeMetadata) {
    if (isOutputMode(pinState.mode)) {
      pin["active_level"] = pinState.activeLevel;
    }
    appendPinConfigMetadata(pin, config);
  }

  if (strcmp(config.input_type, "dht") == 0) {
    if (!isnan(pinState.temperature)) {
      pin["temperature"] = serialized(String(pinState.temperature, 1));
    }
    if (!isnan(pinState.humidity)) {
      pin["humidity"] = serialized(String(pinState.humidity, 1));
    }
  }

  const int brightness = readRuntimeBrightness(pinState);
  if (brightness > 0 || isPwmMode(pinState.mode)) {
    pin["brightness"] = brightness;
  }
}

void appendPinConfigMetadata(JsonObject pin, const EConnectPinConfig &config) {
  pin["function"] = config.function_name;
  pin["label"] = config.label;
  pin["datatype"] =
      isNumericInputMode(config.mode, config.input_type) ? "number" : "boolean";

  JsonObject extraParams = pin.createNestedObject("extra_params");
  bool hasExtraParams = false;

  if (modeEquals(config.mode, "OUTPUT")) {
    extraParams["active_level"] = config.active_level;
    hasExtraParams = true;
  } else if (modeEquals(config.mode, "PWM")) {
    extraParams["min_value"] = config.pwm_min;
    extraParams["max_value"] = config.pwm_max;
    hasExtraParams = true;
  } else if (modeEquals(config.mode, "I2C")) {
    if (strlen(config.i2c_role) > 0) {
      extraParams["i2c_role"] = config.i2c_role;
      hasExtraParams = true;
    }
    if (strlen(config.i2c_address) > 0) {
      extraParams["i2c_address"] = config.i2c_address;
      hasExtraParams = true;
    }
    if (strlen(config.i2c_library) > 0) {
      extraParams["i2c_library"] = config.i2c_library;
      hasExtraParams = true;
    }
  }

  if (strlen(config.input_type) > 0) {
    extraParams["input_type"] = config.input_type;
    hasExtraParams = true;
  }
  if (strlen(config.switch_type) > 0) {
    extraParams["switch_type"] = config.switch_type;
    hasExtraParams = true;
  }
  if (strlen(config.dht_version) > 0) {
    extraParams["dht_version"] = config.dht_version;
    hasExtraParams = true;
  }

  if (!hasExtraParams) {
    pin.remove("extra_params");
  }
}

size_t totalReportedPinCount() {
  size_t reportedPinCount = PIN_CONFIG_COUNT;

  return reportedPinCount;
}

template <typename TDocument>
void appendEmbeddedNetworkTargets(TDocument &doc) {
  JsonObject firmwareNetwork = doc.createNestedObject("firmware_network");
  firmwareNetwork["api_base_url"] = API_BASE_URL;
  firmwareNetwork["mqtt_broker"] = MQTT_BROKER;
  firmwareNetwork["mqtt_port"] = MQTT_PORT;
}

bool runtimeNetworkDiffers(JsonVariantConst runtimeNetwork) {
  if (runtimeNetwork.isNull()) {
    return false;
  }

  const String runtimeApiBaseUrl = String(runtimeNetwork["api_base_url"] | "");
  const String runtimeMqttBroker = String(runtimeNetwork["mqtt_broker"] | "");
  const int runtimeMqttPort = runtimeNetwork["mqtt_port"] | MQTT_PORT;
  if (runtimeApiBaseUrl.length() == 0 || runtimeMqttBroker.length() == 0) {
    return false;
  }

  return runtimeApiBaseUrl != String(API_BASE_URL) ||
         runtimeMqttBroker != String(MQTT_BROKER) ||
         runtimeMqttPort != MQTT_PORT;
}

void requireManualReflash(JsonVariantConst runtimeNetwork,
                          const String &message) {
  manualReflashRequired = true;
  securePairingVerified = false;
  forcePairingRequestOnNextHandshake = false;

  if (mqttClient.connected()) {
    mqttClient.disconnect();
  }

  Serial.println("MANUAL REFLASH REQUIRED.");
  if (message.length() > 0) {
    Serial.println(message.c_str());
  }
  Serial.printf("Embedded target in this firmware: API %s | MQTT %s:%d\n",
                API_BASE_URL, MQTT_BROKER, MQTT_PORT);

  const String runtimeApiBaseUrl = String(runtimeNetwork["api_base_url"] | "");
  const String runtimeMqttBroker = String(runtimeNetwork["mqtt_broker"] | "");
  const int runtimeMqttPort = runtimeNetwork["mqtt_port"] | MQTT_PORT;
  if (runtimeApiBaseUrl.length() > 0 || runtimeMqttBroker.length() > 0) {
    Serial.printf("Current server target: API %s | MQTT %s:%d\n",
                  runtimeApiBaseUrl.length() > 0 ? runtimeApiBaseUrl.c_str()
                                                 : "(unknown)",
                  runtimeMqttBroker.length() > 0 ? runtimeMqttBroker.c_str()
                                                 : "(unknown)",
                  runtimeMqttPort);
  }
}

void logPublishedPayload(const char *context, const String &payload) {
#if ECONNECT_DEBUG_SERIAL
  Serial.printf("%s (%u bytes):\n", context,
                static_cast<unsigned int>(payload.length()));
  Serial.println(payload);
#else
  Serial.printf("%s (%u bytes).\n", context,
                static_cast<unsigned int>(payload.length()));
#endif
}
