void initSerial() {
  Serial.begin(115200);
  delay(2000);
  Serial.println("Serial initialized.");
}

void printLocalTime(){
  struct tm timeinfo;
  if(!getLocalTime(&timeinfo)){
    Serial.println("Failed to obtain time");
    return;
  }
    
}

void initWifi() {
  WiFi.begin(ssid, pass);

  while(WiFi.status() != WL_CONNECTED) {
    delay(1000);
  }

  Serial.println("WiFi initialized.");
}

void initEpochTime() {
  configTime(3600, 0, "pool.ntp.org", "time.nist.gov");

  while(true) {
    epochTime = time(NULL);

    if(epochTime == 28800) {
      delay(2000);
    } else {
      break;
    }
  }

  Serial.printf("Epochtime initialized. Current Time: %lu \n", epochTime);
}

void initDHT() {
  dht.begin();
  Serial.println("DHT initialized.");
}
