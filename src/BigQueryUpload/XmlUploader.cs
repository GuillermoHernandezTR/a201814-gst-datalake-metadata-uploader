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
            TableReference tableReference = await _bigQueryService.EnsureTableExistsAsync(datasetId, tableId);
            
            DateTime lastModifiedTime = await XmlUploader.GetMaxModifiedTimeAsync(tableReference);
            List<string> files = new List<string>();
            string baseDirectory = @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps";
            string directory = $@"{baseDirectory}/{year}/{taxreturntype}ta{year.Last()}/EFile";

            // Log the start of file retrieval
            Console.WriteLine("Starting to retrieve files...");
            var startTime = DateTime.Now;

            // Retrieve all files asynchronously
            files = (await GetFilesWithExtensionAsync(directory, ".xsd")).ToList();

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

        static async Task<IEnumerable<string>> GetFilesWithExtensionAsync(string directoryPath, string fileExtension)
        {
            var files = new List<string>();
            var startTime = DateTime.Now;

            try
            {
                // Retrieve subdirectories asynchronously
                var subdirectories = Directory.EnumerateDirectories(directoryPath);

                var tasks = new List<Task<IEnumerable<string>>>();

                // Create a task for each subdirectory
                foreach (var subdirectory in subdirectories)
                {
                    tasks.Add(Task.Run(() => GetFilesWithExtensionAsync(subdirectory, fileExtension)));
                }

                // Wait for all tasks to complete
                var results = await Task.WhenAll(tasks);

                // Add files from each subdirectory to the result
                foreach (var result in results)
                {
                    files.AddRange(result);
                }

                // Add files from the current directory
                foreach (var file in Directory.EnumerateFiles(directoryPath, "*" + fileExtension))
                {
                    files.Add(file);
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