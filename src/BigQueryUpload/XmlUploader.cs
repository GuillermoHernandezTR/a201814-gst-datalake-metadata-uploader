using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BigQueryUpload
{
    public class XmlUploader
    {
        private readonly BigQueryService _bigQueryService;

        public XmlUploader(BigQueryService bigQueryService)
        {
            _bigQueryService = bigQueryService;
        }

        public async Task UploadXmlAsync(string year, string taxreturntype, string datasetId, string tableId)
        {
            List<string> files = new List<string>();
            string baseDirectory = @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps";
            string directory = $@"{baseDirectory}/{year}/{taxreturntype}ta{year.Last()}/EFile";

            // Obtener todos los archivos de manera asíncrona
            files = (await GetFilesWithExtensionAsync(directory, ".xsd")).ToList();
            Console.WriteLine($"Total files found: {files.Count}");
            foreach (string file in files)
            {
                await _bigQueryService.UploadXmlToBigQueryAsync(file, datasetId, tableId); // Use the async method
            }
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