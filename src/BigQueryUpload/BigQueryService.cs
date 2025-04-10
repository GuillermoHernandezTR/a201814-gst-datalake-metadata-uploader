using Google.Cloud.BigQuery.V2;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;

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

            // Read and process the XML file
            var xmlData = ReadXmlRecursively(xmlFilePath);

            // Prepare the data for BigQuery
            var rowsToInsert = new List<BigQueryInsertRow>();
            foreach (var xmlEntry in xmlData)
            {
                var filePath = xmlEntry["file_path"];
                var modificationTime = File.GetLastWriteTime(filePath.ToString());
                rowsToInsert.Add(new BigQueryInsertRow
                {
                    { "_file", filePath },
                    { "_modified", modificationTime },
                    { "_data", JsonConvert.SerializeObject(xmlEntry["content"]) }
                });
            }
            Console.WriteLine($"Prepared {rowsToInsert.Count} rows for insertion into BigQuery.");

            // Check if the table exists
            var tableReference = _client.GetTableReference(datasetId, tableId);
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
                    { "_file", BigQueryDbType.String },
                    { "_modified", BigQueryDbType.Timestamp },
                    { "_data", BigQueryDbType.String }
                }.Build();
                _client.CreateTable(tableReference, schema);
                Console.WriteLine($"Table {tableId} created successfully.");
            }

            // Upload the data to BigQuery
            Console.WriteLine($"Uploading data to BigQuery table: {tableId}");
            var insertErrors = _client.InsertRows(tableReference, rowsToInsert);
            if (insertErrors != null)
            {
                Console.WriteLine($"Errors while inserting rows into BigQuery: {insertErrors}");
                throw new Exception($"Errors while inserting rows into BigQuery: {insertErrors}");
            }

            Console.WriteLine($"The data from the XML file in {xmlFilePath} has been successfully uploaded to the table {tableId}.");
        }

        private List<Dictionary<string, object>> ReadXmlRecursively(string xmlFilePath)
        {
            Console.WriteLine($"Reading XML file: {xmlFilePath}");
            var xmlData = new List<Dictionary<string, object>>();

            try
            {
                var xmlDocument = new System.Xml.XmlDocument();
                xmlDocument.Load(xmlFilePath);

                var root = xmlDocument.DocumentElement;
                if (root != null)
                {
                    xmlData.Add(new Dictionary<string, object>
                    {
                        { "file_path", xmlFilePath },
                        { "root_tag", root.Name },
                        { "attributes", root.Attributes },
                        { "content", ParseElement(root) }
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading XML file {xmlFilePath}: {ex.Message}");
                throw;
            }

            Console.WriteLine($"Finished reading XML file: {xmlFilePath}");
            return xmlData;
        }

        private Dictionary<string, object> ParseElement(System.Xml.XmlElement element)
        {
            var parsedData = new Dictionary<string, object>
            {
                { "tag", element.Name },
                { "attributes", element.Attributes },
                { "text", element.InnerText.Trim() },
                { "children", new List<Dictionary<string, object>>() }
            };

            foreach (System.Xml.XmlElement child in element.ChildNodes)
            {
                ((List<Dictionary<string, object>>)parsedData["children"]).Add(ParseElement(child));
            }

            return parsedData;
        }
    }
}