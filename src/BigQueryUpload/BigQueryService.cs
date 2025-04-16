using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace BigQueryUpload
{
    public class BigQueryService
    {
        private readonly BigQueryClient _client;

        public BigQueryService(string projectId)
        {
            _client = BigQueryClient.Create(projectId);
        }

        public async Task<TableReference> GetTableAsync(string datasetId, string tableId)
        {
            TableReference tableReference = _client.GetTableReference(datasetId, tableId);

            try
            {
                await _client.GetTableAsync(tableReference);
                Console.WriteLine($"Table {tableId} exists.");
            }
            catch (Google.GoogleApiException)
            {
                // Si la tabla no existe, crearla
                Console.WriteLine($"Table {tableId} does not exist. Creating table...");

                // Definir el esquema de la tabla
                var schema = new TableSchemaBuilder
                {
                    { "year", BigQueryDbType.String },
                    { "taxreturntype", BigQueryDbType.String },
                    { "_file", BigQueryDbType.String },
                    { "_modified", BigQueryDbType.Timestamp },
                    { "_inserted_at", BigQueryDbType.Timestamp },
                    { "node_id", BigQueryDbType.Int64 },
                    { "parent_id", BigQueryDbType.Int64 },
                    { "tag", BigQueryDbType.String },
                    { "attributes", BigQueryDbType.String },
                    { "text", BigQueryDbType.String }
                }.Build();

                await _client.CreateTableAsync(tableReference, schema);
                Console.WriteLine($"Table {tableId} created successfully.");
            }

            return tableReference;
        }

        public async Task<DateTime?> GetMaxModifiedTimeAsync(TableReference tableReference)
        {
            try
            {
                // Construir la consulta SQL para obtener la fecha máxima
                string query = $@"
                    SELECT MAX(_modified) AS max_modified_time
                    FROM `{tableReference.ProjectId}.{tableReference.DatasetId}.{tableReference.TableId}`";

                // Ejecutar la consulta
                var queryResults = await _client.ExecuteQueryAsync(query, parameters: null);

                // Extraer el valor de la fecha máxima
                foreach (var row in queryResults)
                {
                    if (row["max_modified_time"] != null)
                    {
                        return (DateTime)row["max_modified_time"];
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving max modified time: {ex.Message}");
            }

            // Retornar null si no se encuentra un valor o ocurre un error
            return null;
        }

        public async Task UploadXmlToBigQueryAsync(string xmlFilePath, TableReference tableReference)
        {
            string fileName = xmlFilePath.Substring(xmlFilePath.IndexOf(@"Efile\") + 6);
            var idCounter = new Counter { Value = 1 }; // Use the Counter class to track the ID
            var modificationTime = File.GetLastWriteTime(xmlFilePath); // Get file modification time
            var xmlDocument = new System.Xml.XmlDocument();
            xmlDocument.Load(xmlFilePath);

            var root = xmlDocument.DocumentElement;
            int totalRows = 0;

            if (root != null)
            {
                var rowsBatch = new List<BigQueryInsertRow>();
                await ParseElementAsync(root, null, idCounter, rowsBatch, fileName, modificationTime, tableReference);

                // Upload any remaining rows in the batch
                if (rowsBatch.Count > 0)
                {
                    await _client.InsertRowsAsync(tableReference, rowsBatch);
                    totalRows += rowsBatch.Count;
                }
            }

            Console.WriteLine($"Upload completed. File: {fileName}, Rows added: {totalRows}.");
        }

        private async Task ParseElementAsync(System.Xml.XmlElement element, int? parentId, Counter idCounter, List<BigQueryInsertRow> rowsBatch, string fileName, DateTime modificationTime, TableReference tableReference)
        {
            int currentId = idCounter.Value++; // Assign a unique ID to the current node
            
            // Convert attributes to a dictionary
            var attributesDict = new Dictionary<string, string>();
            foreach (System.Xml.XmlAttribute attribute in element.Attributes)
            {
                attributesDict[attribute.Name] = attribute.Value;
            }
            
            // Add the current node as a row
            var row = new BigQueryInsertRow
            {
                { "year", Program.Year },
                { "taxreturntype", Program.TaxReturnType },
                { "_file", fileName },
                { "_modified", modificationTime.ToUniversalTime() },
                { "_inserted_at", DateTime.UtcNow },
                { "node_id", currentId },
                { "parent_id", parentId },
                { "tag", element.Name.StartsWith("xsd:") ? element.Name.Substring(4) : element.Name },
                { "attributes", JsonConvert.SerializeObject(attributesDict) },
                { "text", element.InnerText.Trim() }
            };
            rowsBatch.Add(row);

            // If the batch size reaches 1000, upload it to BigQuery
            if (rowsBatch.Count >= 1000)
            {
                Console.WriteLine($"Uploading batch of {rowsBatch.Count} rows to BigQuery.");
                var insertRows = await _client.InsertRowsAsync(tableReference, rowsBatch);
                if (insertRows.Errors != null && insertRows.Errors.Count() > 0)
                {
                    Console.WriteLine($"Errors while inserting rows into BigQuery: {insertRows}");
                    throw new Exception($"Errors while inserting rows into BigQuery: {insertRows}");
                }
                rowsBatch.Clear(); // Clear the batch after uploading
            }

            // Process child nodes
            foreach (System.Xml.XmlNode childNode in element.ChildNodes)
            {
                if (childNode is System.Xml.XmlElement child)
                {
                    await ParseElementAsync(child, currentId, idCounter, rowsBatch, fileName, modificationTime, tableReference);
                }
            }
        }

        public async Task DeleteFilesFromDestinationTableAsync(List<string> files, TableReference tableReference)
        {
            try
            {
                // Construir la lista de nombres de archivos como una cadena separada por comas
                var fileList = files.Select(file => file.Contains("Efile/") ? file.Substring(file.IndexOf("Efile/") + "Efile/".Length) : file).ToList();
                var parameters = new List<BigQueryParameter>
                {
                    new BigQueryParameter
                    {
                        Name = "fileList",
                        Value = fileList,
                        Type = BigQueryDbType.Array
                    }
                };

                // Construir la consulta SQL para eliminar los registros
                string query = $@"
                    DELETE FROM `{tableReference.ProjectId}.{tableReference.DatasetId}.{tableReference.TableId}`
                    WHERE _file IN UNNEST(@fileList)";

                // Ejecutar la consulta
                Console.WriteLine("Deleting files from destination table...");
                await _client.ExecuteQueryAsync(query, parameters);
                Console.WriteLine("Files deleted successfully from destination table.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting files from destination table: {ex.Message}");
                throw;
            }
        }
    }

    public class Counter
    {
        public int Value { get; set; }
    }
}