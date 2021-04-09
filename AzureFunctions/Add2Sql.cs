using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using AzureFunctions.Models;
using System.Data.SqlClient;
using System;
using Newtonsoft.Json;

namespace AzureFunctions
{
    public static class Add2Sql
    {
        private static HttpClient client = new HttpClient();
        
        [FunctionName("Add2Sql")]
        public static void Run([IoTHubTrigger("messages/events", Connection = "IotHub", ConsumerGroup = "sa")]EventData data, ILogger log)
        {
            log.LogInformation($"IoT Hub function processed a message: {Encoding.UTF8.GetString(data.Body.Array)}");

            using (var conn = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnection")))
            {
                conn.Open();

                using (var cmd = new SqlCommand("", conn))
                {
                    /* DeviceVendors */
                    cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceVendors WHERE VendorName = @Vendor) INSERT INTO DeviceVendors OUTPUT inserted.Id VALUES(@Vendor) ELSE SELECT Id FROM DeviceVendors WHERE VendorName = @Vendor";
                    cmd.Parameters.AddWithValue("@Vendor", data.Properties["Vendor"].ToString());
                    var vendorId = int.Parse(cmd.ExecuteScalar().ToString());

                    /* DeviceModels */
                    cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceModels WHERE ModelName = @ModelName)INSERT INTO DeviceModels OUTPUT inserted.Id VALUES(@ModelName, @VendorId) ELSE SELECT Id FROM DeviceModels WHERE ModelName = @ModelName";
                    cmd.Parameters.AddWithValue("@ModelName", data.Properties["Model"].ToString());
                    cmd.Parameters.AddWithValue("@VendorId", vendorId);
                    var modelId = int.Parse(cmd.ExecuteScalar().ToString());

                    /* DeviceTypes */
                    cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM DeviceTypes WHERE TypeName = @TypeName) INSERT INTO DeviceTypes OUTPUT inserted.Id VALUES(@TypeName) ELSE SELECT Id FROM DeviceTypes WHERE TypeName = @TypeName";
                    cmd.Parameters.AddWithValue("@TypeName", data.Properties["Type"].ToString());
                    var deviceTypeId = int.Parse(cmd.ExecuteScalar().ToString());

                    /* GeoLocations */
                    cmd.CommandText = "IF NOT EXISTS (SELECT Id FROM GeoLocations WHERE Latitude = @Latitude AND Longitude = @Longitude) INSERT INTO GeoLocations OUTPUT inserted.Id VALUES(@Latitude, @Longitude) ELSE SELECT Id FROM GeoLocations WHERE Latitude = @Latitude AND Longitude = @Longitude";
                    cmd.Parameters.AddWithValue("@Latitude", data.Properties["Latitude"].ToString());
                    cmd.Parameters.AddWithValue("@Longitude", data.Properties["Longitude"].ToString());
                    var geoLocationId = long.Parse(cmd.ExecuteScalar().ToString());

                    /* Devices */
                    cmd.CommandText = "IF NOT EXISTS (SELECT DeviceName FROM Devices WHERE DeviceName = @DeviceName) INSERT INTO Devices OUTPUT inserted.DeviceName VALUES(@DeviceName, @DeviceTypeId, @ModelId) ELSE SELECT DeviceName FROM Devices WHERE DeviceName = @DeviceName";
                    cmd.Parameters.AddWithValue("@DeviceName", data.Properties["DeviceName"].ToString());
                    cmd.Parameters.AddWithValue("@DeviceTypeId", deviceTypeId);
                    cmd.Parameters.AddWithValue("@ModelId", modelId);
                    var deviceName = cmd.ExecuteScalar().ToString();

                    /* DhtMeasurements */
                    cmd.CommandText = "IF NOT EXISTS (SELECT 1 FROM TimeTable WHERE UnixUtcTime = @MeasureUnixTime) INSERT INTO TimeTable OUTPUT inserted.UnixUtcTime VALUES (@MeasureUnixTime) ELSE SELECT UnixUtcTime FROM TimeTable WHERE UnixUtcTime = @MeasureUnixTime INSERT INTO DhtMeasurements VALUES(@DeviceId, @MeasureUnixTime, @GeoLocationId, @Temperature, @Humidity, @TemperatureAlert)";
                    DhtMeasurement dhtdata = JsonConvert.DeserializeObject<DhtMeasurement>(Encoding.UTF8.GetString(data.Body.Array));
                    cmd.Parameters.AddWithValue("@MeasureUnixTime", dhtdata.EpochTime);
                    cmd.Parameters.AddWithValue("@Temperature", dhtdata.Temperature);
                    cmd.Parameters.AddWithValue("@Humidity", dhtdata.Humidity);
                    cmd.Parameters.AddWithValue("@DeviceId", deviceTypeId);
                    cmd.Parameters.AddWithValue("@GeoLocationId", geoLocationId);
                    cmd.Parameters.AddWithValue("@TemperatureAlert", dhtdata.TemperatureAlert);
                    cmd.ExecuteNonQuery();

                }

            }

        
        }
    }
}