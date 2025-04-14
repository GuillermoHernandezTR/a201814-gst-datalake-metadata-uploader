using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace BigQueryUpload
{
    public class BigQueryService
    {
        private readonly BigQueryClient _client;

        public BigQueryService(string projectId)
        {
            _client = BigQueryClient.Create(projectId);
        }

        public void UploadXmlToBigQuery(string xmlFilePath, string datasetId, string tableId)
        {
            Console.WriteLine($"Starting upload process for XML file: {xmlFilePath} to BigQuery table: {tableId}");

            // Check if the table exists
            TableReference tableReference = _client.GetTableReference(datasetId, tableId);
            try
            {
                _client.GetTable(tableReference);
                Console.WriteLine($"Table {tableId} exists.");
            }
            catch (Google.GoogleApiException)
            {
                // If the table does not exist, create it
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
                _client.CreateTable(tableReference, schema);
                Console.WriteLine($"Table {tableId} created successfully.");
            }

            // Read and process the XML file
            Console.WriteLine($"Reading XML file: {xmlFilePath}");
            int idCounter = 1; // Counter to generate unique IDs for nodes
            var modificationTime = File.GetLastWriteTime(xmlFilePath); // Get file modification time
            var xmlDocument = new System.Xml.XmlDocument();
            xmlDocument.Load(xmlFilePath);

            var root = xmlDocument.DocumentElement;
            if (root != null)
            {
                var rowsBatch = new List<BigQueryInsertRow>();
                ParseElement(root, null, ref idCounter, rowsBatch, xmlFilePath, modificationTime, tableReference);
                
                // Upload any remaining rows in the batch
                if (rowsBatch.Count > 0)
                {
                    Console.WriteLine($"Uploading final batch of {rowsBatch.Count} rows to BigQuery.");
                    var insertRows = _client.InsertRows(tableReference, rowsBatch);
                    if (insertRows.Errors != null && insertRows.Errors.Count() > 0)
                    {
                        Console.WriteLine($"Errors while inserting rows into BigQuery: {insertRows}");
                        throw new Exception($"Errors while inserting rows into BigQuery: {insertRows}");
                    }
                }
            }

            Console.WriteLine($"The data from the XML file in {xmlFilePath} has been successfully uploaded to the table {tableId}.");
        }

        private void ParseElement(System.Xml.XmlElement element, int? parentId, ref int idCounter, List<BigQueryInsertRow> rowsBatch, string filePath, DateTime modificationTime, TableReference tableReference)
        {
            int currentId = idCounter++; // Assign a unique ID to the current node

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
                { "tag", element.Name },
                { "attributes", JsonConvert.SerializeObject(attributesDict) }, // Serialize attributes dictionary as JSON string
                { "text", element.InnerText.Trim() },
                { "file_name", filePath },
                { "modification_time", modificationTime.ToUniversalTime() }
            };
            rowsBatch.Add(row);

            // If the batch size reaches 1000, upload it to BigQuery
            if (rowsBatch.Count >= 10000)
            {
                Console.WriteLine($"Uploading batch of {rowsBatch.Count} rows to BigQuery.");
                var insertRows = _client.InsertRows(tableReference, rowsBatch);
                if (insertRows.Errors is not null && insertRows.Errors.Count() > 0)
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
                    ParseElement(child, currentId, ref idCounter, rowsBatch, filePath, modificationTime, tableReference);
                }
            }
        }
    }
}