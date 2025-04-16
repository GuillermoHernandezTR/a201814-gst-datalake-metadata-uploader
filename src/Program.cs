using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace BigQueryUpload
{
    class Program
    {
        public static string Year;
        public static string TaxReturnType;
        static async Task Main(string[] args)
        {
            Console.WriteLine("Program started.");
            var stopwatch = Stopwatch.StartNew();

            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory) // Use AppContext.BaseDirectory for better compatibility
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true) // Ensure the file is required
                .Build();

            var bigQueryService = new BigQueryService(configuration["GoogleCloud:ProjectId"]);
            var xmlUploader = new XmlUploader(bigQueryService);

            Year = configuration["BigQuery:Year"];
            TaxReturnType = configuration["BigQuery:TaxReturnType"];

            await xmlUploader.UploadEfileJurisdictionSchemaAsync(Year, TaxReturnType);

            stopwatch.Stop();
            Console.WriteLine($"Program finished. Execution time: {stopwatch.Elapsed.TotalSeconds} seconds.");
        }
    }
}