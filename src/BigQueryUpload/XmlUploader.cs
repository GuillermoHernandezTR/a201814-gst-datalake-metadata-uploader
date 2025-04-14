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
            List<string> files = new List<string>();
            string[] directories = new string[]
            {
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/1040ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/1041ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/1065ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/1120ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/5500ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/7060ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/7090ta4/EFile",
                @"//cisprod-0301.int.thomsonreuters.com/taxapptech$/TaxApps/2024/F990ta4/EFile"
            };

            foreach (string dir in directories)
            {
                if (Directory.Exists(dir))
                {
                    files.AddRange(Directory.GetFiles(dir, "*.xsd"));
                }
            }
            foreach (string file in files)
            {
                _bigQueryService.UploadXmlToBigQuery(file, datasetId, tableId);
            }
        }
    }
}