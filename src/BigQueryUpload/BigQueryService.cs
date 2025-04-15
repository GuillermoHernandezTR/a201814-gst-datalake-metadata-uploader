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

        public async Task<TableReference> EnsureTableExistsAsync(string datasetId, string tableId)
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
                var schema = new TableSchemaBuilder
                {
                    { "id", BigQueryDbType.Int64 },
                    { "parent_id", BigQueryDbType.Int64 },
                    { "tag", BigQueryDbType.String },
                    { "attributes", BigQueryDbType.String },
                    { "text", BigQueryDbType.String },
                    { "file_name", BigQueryDbType.String },
                    { "modification_time", BigQueryDbType.Timestamp }
                }.Build();
                await _client.CreateTableAsync(tableReference, schema);
                Console.WriteLine($"Table {tableId} created successfully.");
            }

            return tableReference;
        }

        public async Task UploadXmlToBigQueryAsync(string xmlFilePath, TableReference tableReference)
        {
            string fileName = Path.GetFileName(xmlFilePath);
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
                { "id", currentId },
                { "parent_id", parentId },
                { "tag", element.Name.StartsWith("xsd:") ? element.Name.Substring(4) : element.Name },
                { "attributes", JsonConvert.SerializeObject(attributesDict) }, // Serialize attributes dictionary as JSON string
                { "text", element.InnerText.Trim() },
                { "file_name", fileName },
                { "modification_time", modificationTime.ToUniversalTime() }
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
    }

    public class Counter
    {
        public int Value { get; set; }
    }
}