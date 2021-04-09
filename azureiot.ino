

void initDevice() {
  deviceClient = IoTHubClient_LL_CreateFromConnectionString(conn, MQTT_Protocol);
}

void sendCallback(IOTHUB_CLIENT_CONFIRMATION_RESULT result, void *userContextCallback) {
  if(IOTHUB_CLIENT_CONFIRMATION_OK == result) {
    Serial.println("Sending message to Azure IoT Hub - succeeded.");
    Serial.print("Temperature :");
    Serial.println(temperature);
  }
  
  messagePending = false;
}

  void sendMessage(char *payload) {
  
  IOTHUB_MESSAGE_HANDLE message = IoTHubMessage_CreateFromByteArray((const unsigned char *) payload, strlen(payload));

  MAP_HANDLE properties = IoTHubMessage_Properties(message);
  Map_Add(properties, "Type", "dht");
  Map_Add(properties, "DeviceName", "C4:4F:33:64:8B:01");
  Map_Add(properties, "Vendor", "HUZZAH");
  Map_Add(properties, "Model", "Esp32");
  Map_Add(properties, "Latitude", "59.27958");
  Map_Add(properties, "Longitude", "17.90018");
    
  if(IoTHubClient_LL_SendEventAsync(deviceClient, message, sendCallback, NULL) == IOTHUB_CLIENT_OK) {
    messagePending = true;
  }

  IoTHubMessage_Destroy(message);
}
