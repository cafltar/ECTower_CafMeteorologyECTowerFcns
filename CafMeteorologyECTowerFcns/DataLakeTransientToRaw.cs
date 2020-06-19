// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Text.Json;

using Caf.Etl.Models.LoggerNet.TOA5;
using Caf.Etl.Nodes.LoggerNet.Extract;
using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Identity;
using Caf.SciEtl.Nodes.AzureDataLake.Services;
using Caf.SciEtl.Nodes.AzureDataLake.Core;
using System.Xml;

namespace Caf.Projects.CafMeteorologyECTowerFcns
{
    public static class DataLakeTransientToRaw
    {
        [FunctionName("DataLakeTransientToRaw")]
        public static async Task Run(
            [EventGridTrigger]EventGridEvent eventGridEvent, 
            ILogger log)
        {
            log.LogInformation(eventGridEvent.Subject);
            string eventCallerName = "CafMeteorologyECTowerFcns.DataLakeTransientToRaw()";
            string eventCallerVersion = "v0.1.7";

            log.LogInformation(eventCallerVersion);

            EtlEventService etlEventService = new EtlEventService(
                eventCallerName,
                eventCallerVersion,
                "AzureFunction");

            etlEventService.LogInformation(
                eventCallerName,
                eventCallerVersion,
                eventGridEvent.ToString());
            etlEventService.AddInput($"EventGridEvent.Subject: {eventGridEvent.Subject}");
            etlEventService.LogInformation(
                eventCallerName,
                eventCallerVersion, 
                $"EventGridEvent.Data: {eventGridEvent.Data}");

            // Authenticate the Function for access to blob containers
            string objectId =
                Environment.GetEnvironmentVariable("FUNCTION_OBJECT_ID");
            ManagedIdentityCredential credential =
                new ManagedIdentityCredential(objectId);

            // Read parameters
            string OUTPUT_CONTAINER =
                    Environment.GetEnvironmentVariable("OUTPUT_CONTAINER");
            string PROJECT_ID =
                Environment.GetEnvironmentVariable("PROJECT_ID");
            string DATALAKE_ENDPOINT =
                Environment.GetEnvironmentVariable("DATALAKE_ENDPOINT");

            if (eventGridEvent.EventType != "Microsoft.Storage.BlobCreated")
            {
                string msg = "EventType not BlobCreated, aborting";
                etlEventService.LogError(
                    eventCallerName,
                    eventCallerVersion,
                    msg);
                throw new Exception(msg);
            }

            // Get info from the event
            log.LogInformation("Parsing Event");
            JsonDocument json =
                JsonDocument.Parse(eventGridEvent.Data.ToString());
            
            string apiCall = json.RootElement.GetProperty("api").GetString();
            log.LogInformation($"api: {apiCall}");
            if (!(apiCall == "FlushWithClose" |
                apiCall == "PutBlob" |
                apiCall == "PutBlockList" |
                apiCall == "CopyBlob"))
            {
                string msg = "EventGridEvent api not completely committed, aborting";
                log.LogInformation(msg);
                return;
            }

            try
            {
                string inputBlobUri = json.RootElement
                    .GetProperty("url")
                    .GetString();
                BlobUriBuilder inputBlobUriBuilder = new BlobUriBuilder(
                    new Uri(inputBlobUri));

                // Get input blob contents
                log.LogInformation("Creating blob container client");
                var blobContainerClient =
                    new BlobContainerClient(
                        new Uri(
                            $"https://{inputBlobUriBuilder.Host}/{inputBlobUriBuilder.BlobContainerName}"),
                    credential);

                var inputBlobClient = blobContainerClient.GetBlobClient(inputBlobUriBuilder.BlobName);

                if(!inputBlobClient.Exists())
                {
                    log.LogInformation("Blob does not exist, exiting");
                    return;
                }

                log.LogInformation("Found blob, downloading content");
                BlobDownloadInfo download = await inputBlobClient.DownloadAsync();

                string blobContent;
                using (StreamReader reader = new StreamReader(download.Content))
                    blobContent = await reader.ReadToEndAsync();

                string blobName =
                    Path.GetFileName(new Uri(inputBlobUri).AbsolutePath);

                log.LogInformation($"Blob length: {blobContent.Length}");
                if(blobContent.Length <= 0)
                {
                    log.LogInformation("Blob is empty, exiting");
                    return;
                }
                // Get metadata from input blob
                log.LogInformation("Parsing Blob into TOA5 metadata");
                TOA5Extractor extractor = new TOA5Extractor(
                    blobName,
                    blobContent,
                    -8);
                Metadata blobMetadata = extractor.GetMetadata();

                string outputBlobDataset = GetOutputDatasetName(blobMetadata);
                string outputBlobDirPath = GetOutputSubDirPath(blobName);

                // Move blob
                log.LogInformation("Moving blob");
                AzureDataLakeService dataLakeService = 
                    new AzureDataLakeService(
                        DATALAKE_ENDPOINT,
                        credential);

                string outputBlobPath = $"{PROJECT_ID}/{outputBlobDataset}/{outputBlobDirPath}/{blobName}";
                log.LogInformation(outputBlobPath);
                string outputUri = await dataLakeService.MoveBlob(
                    inputBlobUriBuilder.BlobContainerName,
                    inputBlobUriBuilder.BlobName,
                    OUTPUT_CONTAINER,
                    outputBlobPath,
                    etlEventService);

                log.LogInformation("Blob moved");
                etlEventService.AddOutput(outputUri);
            }
            catch(XmlException xmlException)
            {
                log.LogError(xmlException.Message);
            }
            catch(Exception e)
            {
                etlEventService.LogError(
                    eventCallerName,
                    eventCallerVersion, 
                    $"Exception occured: {e}");

                throw new Exception("Error in function", e);
            }
            finally
            {
                // Write EtlEvent
                EtlEventServiceConfig etlEventServiceConfig = new EtlEventServiceConfig()
                {
                    Zone = OUTPUT_CONTAINER,
                    Project = PROJECT_ID,
                    Endpoint = new Uri(DATALAKE_ENDPOINT),
                    Credential = credential
                };

                log.LogInformation("Writing EtlEvent");
                string etlEventUri = await etlEventService.WriteAsync(etlEventServiceConfig);

                log.LogInformation($"Wrote EtlEvent to: {etlEventUri}");
            }
        }

        private static string GetOutputDatasetName(
            Metadata blobMeta)
        {
            string fieldId = blobMeta.StationName.Replace("LTAR_", "");
            string dataTable = blobMeta.TableName.Replace("LTAR_", "");
            string programSignature = 
                blobMeta.DataloggerProgramSignature.ToString();

            string datasetName = $"{fieldId}{dataTable}_V{programSignature}";

            return datasetName;
        }

        private static string GetOutputSubDirPath(
            string inputBlobFilename)
        {
            // Expects name in format: {StationId}{EcTower}_{DataTableName}_Raw_{YYYY}_{MM}_{DD}_{HHMM}.dat
            string[] sections = inputBlobFilename.Split("_");
            string year = sections[^4];
            string month = sections[^3];

            string directoryPath = $"{year}/{month}";

            return directoryPath;
        }
    }
}
