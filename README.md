# BigQuery Upload Application

This project provides functionality to upload CSV and XML files to Google BigQuery. It includes separate modules for handling the upload processes for each file type.

## Project Structure

```
BigQueryUploadApp
├── src
│   ├── Program.cs               # Entry point of the application
│   ├── BigQueryUpload           # Namespace for BigQuery upload functionalities
│   │   ├── CsvUploader.cs       # Contains CSV upload functionality
│   │   ├── XmlUploader.cs       # Contains XML upload functionality
│   │   └── BigQueryService.cs   # Contains methods for interacting with the BigQuery API
├── appsettings.json             # Configuration file for application settings
├── BigQueryUploadApp.csproj     # Project file for .NET
└── README.md                    # Documentation for the project
```

## Setup Instructions

1. **Clone the repository**:
   ```
   git clone <repository-url>
   cd BigQueryUploadApp
   ```

2. **Install dependencies**:
   Use the .NET CLI to manage dependencies. Restore the required packages by running:
   ```
   dotnet restore
   ```

3. **Set up Google Cloud credentials**:
   Ensure that your Google Cloud credentials are configured to allow access to BigQuery. Add the path to your service account key file in the `appsettings.json` file.

## Usage

### Upload CSV Files
To upload CSV files, modify the `filePath` and `tableId` variables in `src/Program.cs` and run the application:
```
dotnet run
```

### Upload XML Files
To upload XML files, adjust the `directoryPath` and `tableId` variables in `src/Program.cs` and run the application:
```
dotnet run
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any enhancements or bug fixes.