using System;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace BigQueryUpload
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory) // Use AppContext.BaseDirectory for better compatibility
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true) // Ensure the file is required
                .Build();

            var bigQueryService = new BigQueryService(configuration["GoogleCloud:ProjectId"]);
            var xmlUploader = new XmlUploader(bigQueryService);
            xmlUploader.UploadXml(configuration["BigQuery:FilePath"], configuration["BigQuery:DatasetId"], configuration["BigQuery:TableId"]);
        }
    }
}