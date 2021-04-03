using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace AzureFunctions
{
    public static class PopulateTimeTable
    {
        [FunctionName("PopulateTimeTable")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            string unixUtcTime = req.Query["unixutctime"];
            log.LogInformation(unixUtcTime);

            using (var conn = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnection")))
            {
                await conn.OpenAsync();
                long current = 0;

                using (var cmd = new SqlCommand("", conn))
                {
                    try
                    {
                        cmd.CommandText = "SELECT MAX(UnixUtcTime) FROM TimeTable";
                        current = long.Parse(cmd.ExecuteScalar().ToString());
                    }
                    catch
                    {

                    }

                    var goalValue = DateTimeOffset.Now.AddMinutes(1).ToUnixTimeSeconds();

                    if (current == 0)
                        current = long.Parse(unixUtcTime);

                    while(current < goalValue)
                    {
                        cmd.CommandText = $"IF NOT EXISTS(SELECT 1 FROM TimeTable WHERE UnixUtcTime = {current}) INSERT INTO TimeTable VALUES({current})";
                        cmd.ExecuteNonQuery();

                        current++;
                    }
                }
            }

            return new OkResult();
        }
    }
}

