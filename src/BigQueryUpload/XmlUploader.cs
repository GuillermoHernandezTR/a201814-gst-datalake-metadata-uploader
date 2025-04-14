using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

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
            List<string> files = Directory.GetFiles(directoryPath, "*.xsd").ToList();
            foreach (string file in files)
            {
                _bigQueryService.UploadXmlToBigQuery(file, datasetId, tableId);
            }
        }
    }
}