using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Collections.Concurrent;
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
            Directory.SetCurrentDirectory(baseDirectory);
            string directory = $@"{year}/{taxreturntype}ta{year.Last()}/EFile";

            // Log the start of file retrieval
            Console.WriteLine("Starting to retrieve files...");
            var startTime = DateTime.Now;

            // Retrieve all files asynchronously
            files = (await GetFilesWithExtensionAsync(directory, ".xsd")).ToList();

            // Log the end of file retrieval
            var endTime = DateTime.Now;
            Console.WriteLine($"File retrieval completed. Time elapsed: {(endTime - startTime).TotalSeconds} seconds. Total files: {files.Count}");

            // Delete retrieved files in destination table
            await _bigQueryService.DeleteFilesFromDestinationTableAsync(files, tableReference);

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

            try
            {
                // Obtenemos los subdirectorios de manera asincrónica
                var subdirectories = Directory.EnumerateDirectories(directoryPath);

                var tasks = new List<Task<IEnumerable<string>>>();

                // Crea una tarea para cada subdirectorio
                foreach (var subdirectory in subdirectories)
                {
                    tasks.Add(Task.Run(() => GetFilesWithExtensionAsync(subdirectory, fileExtension)));
                }

                // Espera a que todas las tareas se completen
                var results = await Task.WhenAll(tasks);

                // Agrega los archivos de cada subdirectorio al resultado
                foreach (var result in results)
                {
                    files.AddRange(result);
                }

                // Agrega los archivos del directorio actual
                foreach (var file in Directory.EnumerateFiles(directoryPath, "*" + fileExtension))
                {
                    files.Add(file);
                }
            }
            catch (UnauthorizedAccessException e)
            {
                Console.WriteLine($"Acceso denegado a {directoryPath}: {e.Message}");
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine($"Directorio no encontrado: {e.Message}");
            }
            catch (IOException e)
            {
                Console.WriteLine($"Error de E/S: {e.Message}");
            }

            return files;
        }

    }
}