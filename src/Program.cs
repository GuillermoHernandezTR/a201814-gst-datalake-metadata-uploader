using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace BigQueryUpload
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory) // Use AppContext.BaseDirectory for better compatibility
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true) // Ensure the file is required
                .Build();

            var bigQueryService = new BigQueryService(configuration["GoogleCloud:ProjectId"]);
            var xmlUploader = new XmlUploader(bigQueryService);
            await xmlUploader.UploadXmlAsync(
                configuration["BigQuery:Year"],
                configuration["BigQuery:TaxReturnType"],
                configuration["BigQuery:DatasetId"],
                configuration["BigQuery:TableId"]
            );
        }
    }
}