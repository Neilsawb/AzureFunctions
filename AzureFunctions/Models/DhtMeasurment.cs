using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions.Models
{

    public class DhtMeasurement
    {
        public string DeviceId { get; set; }
        public float Temperature { get; set; }
        public float Humidity { get; set; }
        public long EpochTime { get; set; }
        public string TemperatureAlert { get; set; }
        
    }

}
