using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;

namespace BigQueryUpload
{
    public class XmlUploader
    {
        private readonly BigQueryService _bigQueryService;

        public XmlUploader(BigQueryService bigQueryService)
        {
            _bigQueryService = bigQueryService;
        }

        public async Task UploadEfileJurisdictionSchemaAsync(string year, string taxreturntype)
        {
            string datasetId = "mdr";
            string tableId = "efile_xsd";
            
            // Verificar y crear la tabla si es necesario
            TableReference tableReference = await _bigQueryService.GetTableAsync(datasetId, tableId);
            
            List<string> files = new List<string>();
            string baseDirectory = @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps";
            string directory = $@"{baseDirectory}/{year}/{taxreturntype}ta{year.Last()}/EFile";

            // Log the start of file retrieval
            Console.WriteLine("Starting to retrieve files...");
            var startTime = DateTime.Now;

            // Retrieve all files asynchronously
            files = (await FetchModifiedFilesAsync(directory, ".xsd", tableReference)).ToList();

            // Log the end of file retrieval
            var endTime = DateTime.Now;
            Console.WriteLine($"File retrieval completed. Time elapsed: {(endTime - startTime).TotalSeconds} seconds. Total files: {files.Count}");


            int totalFiles = files.Count;

            // Log the start of data extraction
            Console.WriteLine("Starting data extraction and upload to BigQuery...");
            var extractionStartTime = DateTime.Now;

            await Parallel.ForEachAsync(files, async (file, _) =>
            {
                await _bigQueryService.UploadXmlToBigQueryAsync(file, tableReference);
            });

            // Log the end of data extraction
            var extractionEndTime = DateTime.Now;
            Console.WriteLine($"Data extraction and upload completed. Time elapsed: {(extractionEndTime - extractionStartTime).TotalSeconds} seconds.");

        }

        public async Task<IEnumerable<string>> FetchModifiedFilesAsync(string directoryPath, string fileExtension, TableReference tableReference)
        {
            var files = new List<string>();
            var startTime = DateTime.Now;
            DateTime? maxModifiedTime = await _bigQueryService.GetMaxModifiedTimeAsync(tableReference);

            try
            {
                // Retrieve subdirectories asynchronously
                var subdirectories = Directory.EnumerateDirectories(directoryPath)
                                             .Where(subdir => Directory.GetLastWriteTimeUtc(subdir) > (maxModifiedTime ?? DateTime.MinValue));

                var tasks = new List<Task<IEnumerable<string>>>();

                // Create a task for each subdirectory
                foreach (var subdirectory in subdirectories)
                {
                    tasks.Add(Task.Run(() => FetchModifiedFilesAsync(subdirectory, fileExtension, tableReference)));
                }

                // Wait for all tasks to complete
                var results = await Task.WhenAll(tasks);

                // Add files from each subdirectory to the result
                foreach (var result in results)
                {
                    files.AddRange(result);
                }
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine($"Access denied to {directoryPath}: {e.Message}");
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine($"Directory not found: {e.Message}");
            }
            catch (IOException e)
            {
                Console.WriteLine($"I/O error: {e.Message}");
            }
            return files;
        }

    }
}