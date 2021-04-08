#include <WiFi.h>   //#include <ESP8266WiFi.h>
#include <AzureIoTHub.h>
#include <AzureIoTProtocol_MQTT.h>
#include <AzureIoTUtility.h>
#include <DHT.h>
#include <ArduinoJson.h>

#define DHT_PIN 21
#define DHT_TYPE DHT11

char *ssid = "Alfred";
char *pass = "nobel2019";
char *conn = "HostName=NS-IOT2020.azure-devices.net;DeviceId=C4:4F:33:64:8B:01;SharedAccessKey=BFwVWF3xh935cdoOctcDsygdSckj/NSm7u8mG92Np9s=";

bool messagePending = false;
int interval = 1000 * 15;
unsigned long prevMillis = 0;
time_t epochTime;
float prevTemp = -1;
float temperature;
float humidity;

DHT dht(DHT_PIN,DHT_TYPE);
IOTHUB_CLIENT_LL_HANDLE deviceClient;

void setup() {
  initSerial();
  initWifi();
  initEpochTime();
  initDHT();
  initDevice();
  delay(2000);
}

void loop() {
  unsigned long currentMillis = millis();
  epochTime = time(NULL);
  temperature = dht.readTemperature();
  humidity = dht.readHumidity();
  
  if(!messagePending) {
    if(!(std::isnan(temperature)) && !(std::isnan(humidity)))  {
      if ((temperature > prevTemp + 0.1) || (temperature < prevTemp - 1 && (prevTemp != -0.1)))  {   
           prevTemp = temperature;
           char payload[320];
           char epochTimeBuf[12];
           printLocalTime();
           StaticJsonBuffer<sizeof(payload)> buf;
           JsonObject &root = buf.createObject();
           root["Temperature"] = temperature;
           root["Humidity"] = humidity;
           root["EpochTime"] = epochTime;
           if (temperature > 28) {
            root["TemperatureAlert"] = "2";
           }
           else {
            root["TemperatureAlert"] = "1";
           }
           root.printTo(payload, sizeof(payload));
           sendMessage (payload);
      }
    }
  }
  

  IoTHubClient_LL_DoWork(deviceClient);
  delay(10);
}
