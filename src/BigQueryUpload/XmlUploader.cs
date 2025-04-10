using System.IO;

namespace BigQueryUpload
{
    public class XmlUploader
    {
        private readonly BigQueryService _bigQueryService;

        public XmlUploader(BigQueryService bigQueryService)
        {
            _bigQueryService = bigQueryService;
        }

        public void UploadXml(string directoryPath, string datasetId, string tableId)
        {
            var files = Directory.GetFiles(directoryPath, "*.xsd");
            foreach (var file in files)
            {
                using (var reader = new StreamReader(file))
                {
                    var xmlContent = reader.ReadToEnd();
                    _bigQueryService.UploadXmlToBigQuery(xmlContent, datasetId, tableId);
                }
            }
        }
    }
}