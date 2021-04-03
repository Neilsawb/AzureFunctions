using System;
using System.Collections.Generic;
using System.Text;

namespace AzureFunctions.Models
{
    public class RegisterDevice
    {
        public string DeviceName { get; set; }
        public string Type { get; set; }
        public string Vendor { get; set; }
        public string Model { get; set; }
        public string Longitude { get; set; }
        public string Latitude { get; set; }
    }

}
