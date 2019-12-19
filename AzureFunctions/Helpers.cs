using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace NsrFunctions
{
    static class Helpers
    {
        private static readonly Lazy<CloudBlobClient> _blobClient = new Lazy<CloudBlobClient>(() => _storageAccount.Value.CreateCloudBlobClient());

        private static readonly Lazy<CloudStorageAccount> _storageAccount = new Lazy<CloudStorageAccount>(() =>
        {
            if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"Bottler_Storage"), out var storageAccount))
            {
                throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
            }

            return storageAccount;
        });

        public static CloudBlobClient GetCloudBlobClient() => _blobClient.Value;
        public static CloudStorageAccount GetCloudStorageAccount() => _storageAccount.Value;

        public static CloudTable GetLockTable(CloudStorageAccount storageAccount = null)
        {
            CloudTable bottlerFilesTable;
            if (storageAccount == null)
            {
                if (!CloudStorageAccount.TryParse(Environment.GetEnvironmentVariable(@"LockTableStorage"), out var sa))
                {
                    throw new Exception(@"Can't create a storage account accessor from app setting connection string, sorry!");
                }
                else
                {
                    storageAccount = sa;
                }
            }

            try
            {
                bottlerFilesTable = storageAccount.CreateCloudTableClient().GetTableReference(Environment.GetEnvironmentVariable(@"LockTableName"));
            }
            catch (Exception ex)
            {
                throw new Exception($@"Error creating table client for locks: {ex}", ex);
            }

            bottlerFilesTable.CreateIfNotExists();

            return bottlerFilesTable;
        }

        static readonly Regex v3landingFileBlobUrlRegex = new Regex(@"[^/\S]*/(?:[^/]+)/(?<bottlerFolder>[^/]+)/landing-files/(?<bottlerFilenamePrefix>[^_]+)_.+_(?:volume|revenue|stddisc|bulkdisc|offdisc)_.+\.csv$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        internal static (bool Is, string FileParentFolder) IsV3BottlerLandingFile(string fileUrl, TraceWriter log = null)
        {
            var matchesUrl = v3landingFileBlobUrlRegex.Match(fileUrl);
            if (matchesUrl.Success)
            {
                log?.Verbose($@"{fileUrl} looks like a v3 file, checking bottler specs w/in the URL to ensure they match...");
                // Double check the bottler subfolder matches the bottler file prefix
                return (matchesUrl.Groups[@"bottlerFolder"].Value.Equals(matchesUrl.Groups[@"bottlerFilenamePrefix"].Value, StringComparison.OrdinalIgnoreCase), matchesUrl.Groups[@"bottlerFolder"].Value);
            }

            log?.Verbose($@"{fileUrl} doesn't match the v3 regex; not a v3 file");

            return (false, null);
        }

        static readonly Regex landingZipFilesRegex = new Regex(@"^\S*/(?<container>[^/]+)/(?<pathInContainer>(?<bottlerName>[^/]+)/landing-files)/(?<fullFilename>(?<filenameWithoutExtension>[^/]+)\.zip)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        internal static (bool Is, string FileParentFolder, string bottlerName) IsZipLandingFile(string fileUrl, TraceWriter log = null)
        {
            var matchesUrl = landingZipFilesRegex.Match(fileUrl);
            if (matchesUrl.Success)
            {
                log?.Verbose($@"{fileUrl} looks like a zip file, trying to unzip file...");
                // Double check the bottler subfolder matches the bottler file prefix
                return (true, matchesUrl.Groups[@"pathInContainer"].Value, matchesUrl.Groups[@"bottlerName"].Value);
            }

            log?.Verbose($@"{fileUrl} doesn't match the zip file regex; not a zip file");

            return (false, null, null);
        }

        public static void CreateErrorFile(CloudBlobClient client, string containerName, string bottlerName, string fileName,
            IEnumerable<string> fileContent, TraceWriter log, bool hasNewLine = false)
        {
            var outputDirectory = $@"{bottlerName}/invalid-data/errors";
            CloudBlockBlob blockBlob = client.GetContainerReference(containerName)
                .GetDirectoryReference(outputDirectory)
                .GetBlockBlobReference(fileName);

            log.Info($@"Writing ERROR file - Container: {containerName} and Directory: {outputDirectory} and fileName: {fileName}");
            blockBlob.DeleteIfExists();
            var options = new BlobRequestOptions
            {
                ServerTimeout = TimeSpan.FromMinutes(10)
            };

            string text = string.Empty;

            text = string.Join("\r\n", fileContent);

            if (hasNewLine)
                text = text.Replace("\r\n", "");

            using (var stream = blockBlob.OpenWrite())
            using (StreamWriter sw = new StreamWriter(stream))
            {
                sw.Write(text);
                sw.Flush();
            }

            log.Info($@"Error File Created Successfully...");
        }

        public static void LogMessage(CloudBlobClient client, string fileName, IEnumerable<string> fileContent, string prefix, TraceWriter log)
        {
            var prefixParts = prefix.Split('/');
            var directoryPath = $@"{prefixParts[1]}/invalid-data/errors/logs";

            CloudBlobContainer container = client.GetContainerReference(prefixParts[0]); 

            CloudBlobDirectory directory = container.GetDirectoryReference(directoryPath);
            CloudBlockBlob blockBlob = directory.GetBlockBlobReference(fileName);

            log.Info($@"Container: {prefixParts[0]} and Directory: {directoryPath} and fileName: {fileName}");

            blockBlob.DeleteIfExists();

            var options = new BlobRequestOptions()
            {
                ServerTimeout = TimeSpan.FromMinutes(10)
            };

            var text = string.Join("\r\n", fileContent);

            using (MemoryStream stream = new MemoryStream())
            using (StreamWriter sw = new StreamWriter(stream))
            {
                sw.Write(text);
                sw.Flush();
                stream.Position = 0;

                blockBlob.UploadFromStream(stream);
            }

            log.Info($@"Log File Created Successfully...");
        }

        // TODO: These should be data-driven
        public static string GetFactType(string fileType)
        {
            switch (fileType.ToLowerInvariant())
            {
                case @"volume":
                    return "ACT-VOL";
                case @"revenue":
                    return "ACT-REV";
                case @"stddisc":
                    return "ACT-DSTD";
                case @"bulkdisc":
                    return "ACT-DBLK";
                case @"offdisc":
                    return "ACT-DOFF";
                default:
                    return string.Empty;
            }
        }

        /// <summary>
        /// Uploads the BLOB as put block list. This is required for blobs above a certain size, see https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks
        /// </summary>
        /// <param name="blobClient">The BLOB client.</param>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="blobName">Name of the BLOB.</param>
        /// <param name="fileContentLines">The file content lines.</param>
        public static void UploadBlobAsPutBlockList(CloudBlobClient blobClient, string containerName, string directoryPath,
            string blobName, IEnumerable<string> fileContentLines, string contentType = @"text/csv; charset=utf-8",
            bool newlineAvailable = false)
        {
            var directory = blobClient.GetContainerReference(containerName)
                .GetDirectoryReference(directoryPath);

            var blob = directory.GetBlockBlobReference(blobName);
            blob.Properties.ContentType = contentType;
            blob.Properties.ContentEncoding = "utf-8";
            const int numBytesPerChunk = 250 * 1024;    // 256KB, max 100MB per Azure Storage docs here: https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#request-headers

            int thisBlockId = 0;
            var blockIds = new List<string>();
            int spotInBuffer = 0;
            var blockBuffer = new byte[numBytesPerChunk];

            var fileContent = fileContentLines.ToList();
            for (int lineNum = 0; lineNum < fileContent.Count; lineNum++)
            {
                var isLastLine = (lineNum == (fileContent.Count - 1));
                var line = fileContent[lineNum];
                if (!isLastLine && newlineAvailable == false)
                {   // Put newlines at the end of every line except the last one
                    line = string.Concat(line, "\r\n");
                }

                var lineBytes = Encoding.UTF8.GetBytes(line);
                for (int byteInLine = 0; byteInLine < lineBytes.Length; byteInLine++)
                {
                    var isLastByteInLine = (byteInLine == (lineBytes.Length - 1));
                    var lineByte = lineBytes[byteInLine];

                    blockBuffer[spotInBuffer] = lineByte;

                    var bufferIsFull = spotInBuffer == (numBytesPerChunk - 1);    // 0-index

                    if (isLastByteInLine && isLastLine)
                    {
                        // If we're the last byte in the last line; the buffer we want to write out should only be as big as we have; not a full-size buffer. Basically, truncate the buffer size to match what we've got in it right now
                        var lastBlockBuffer = new byte[spotInBuffer + 1];
                        Array.Copy(blockBuffer, lastBlockBuffer, lastBlockBuffer.Length);

                        blockBuffer = lastBlockBuffer;
                        // consider the buffer full now, so we do the write out to the file
                        bufferIsFull = true;
                    }

                    if (bufferIsFull)
                    {
                        var newBlockId = Convert.ToBase64String(BitConverter.GetBytes(thisBlockId++));

                        // Upload the block to this file's spot in Blob Storage
                        blob.PutBlock(newBlockId, new MemoryStream(blockBuffer, false), null);

                        // Add the ID of this block to the list of blocks that will eventually compose our Blob
                        blockIds.Add(newBlockId);

                        //reset the buffer & buffer pointer
                        blockBuffer.Initialize();
                        spotInBuffer = 0;
                    }
                    else
                    {
                        ++spotInBuffer;
                    }
                }
            }

            // Set this blob as being composed of all the IDs which were uploaded
            blob.PutBlockList(blockIds);
        }

        public static void MoveBlobs(CloudBlobClient blobClient, IListBlobItem blobToMove, string targetFolderName, Func<string, string> getTargetFilename = null, (TimeSpan? Timespan, Guid? LeaseId)? lease = null, TraceWriter log = null) => MoveBlobs(blobClient, new[] { blobToMove }, targetFolderName, getTargetFilename, lease, log);
        public static void MoveBlobs(CloudBlobClient blobClient, IEnumerable<IListBlobItem> blobsToMove, string targetFolderName, Func<string, string> getTargetFilename = null, (TimeSpan? Timespan, Guid? LeaseId)? lease = null, TraceWriter log = null)
        {
            var useLeases = lease.HasValue;
            foreach (var b in blobsToMove)
            {
                var blobRef = blobClient.GetBlobReferenceFromServer(b.StorageUri);
                var sourceBlob = b.Container.GetBlockBlobReference(blobRef.Name);
                string sourceLeaseId = null;
                if (useLeases) sourceLeaseId = sourceBlob.AcquireLease(lease.Value.Timespan, lease.Value.LeaseId?.ToString());

                var targetBlob = blobRef.Container
                    .GetDirectoryReference(targetFolderName)
                    .GetBlockBlobReference(getTargetFilename?.Invoke(blobRef.Name) ?? Path.GetFileName(blobRef.Name));
                string targetLeaseId = null;
                if (useLeases) targetLeaseId = targetBlob.AcquireLease(lease.Value.Timespan, lease.Value.LeaseId?.ToString());

                targetBlob.StartCopy(sourceBlob);

                while (targetBlob.CopyState.Status == CopyStatus.Pending) ;     // spinlock until the copy completes

                bool copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                if (!copySucceeded)
                {
                    log?.Error($@"Error copying {sourceBlob.Name} to {targetFolderName} folder. Retrying once...");

                    targetBlob.StartCopy(sourceBlob);

                    while (targetBlob.CopyState.Status == CopyStatus.Pending) ;     // spinlock until the copy completes

                    copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                    if (!copySucceeded)
                    {
                        log?.Error($@"Error retrying copy of {sourceBlob.Name} to {targetFolderName} folder. File not moved.");
                    }
                }

                if (copySucceeded)
                {
#if DEBUG
                    try
                    {
#endif
                        sourceBlob.Delete();
#if DEBUG
                    }
                    catch (StorageException ex)
                    {
                        log?.Error($@"Error deleting blob {sourceBlob.Name}", ex);
                    }
#endif
                }

                if (useLeases)
                {
                    sourceBlob.ReleaseLease(new AccessCondition { LeaseId = sourceLeaseId });
                    targetBlob.ReleaseLease(new AccessCondition { LeaseId = targetLeaseId });
                }
            }
        }

        public static void CopyBlobs(CloudBlobClient blobClient, IListBlobItem blobToMove, string fileNamePrefix, string targetFolderName, Func<string, string> getTargetFilename = null, (TimeSpan? Timespan, Guid? LeaseId)? lease = null, TraceWriter log = null) => CopyBlobs(blobClient, new[] { blobToMove }, fileNamePrefix, targetFolderName, getTargetFilename, lease, log);
        public static void CopyBlobs(CloudBlobClient blobClient, IEnumerable<IListBlobItem> blobsToMove, string fileNamePrefix, string targetFolderName, Func<string, string> getTargetFilename = null, (TimeSpan? Timespan, Guid? LeaseId)? lease = null, TraceWriter log = null)
        {            
            var useLeases = lease.HasValue;
            foreach (var b in blobsToMove)
            {
                var blobRef = blobClient.GetBlobReferenceFromServer(b.StorageUri);
                
                var sourceBlob = b.Container.GetBlockBlobReference(blobRef.Name);
                string sourceLeaseId = null;
                if (useLeases) sourceLeaseId = sourceBlob.AcquireLease(lease.Value.Timespan, lease.Value.LeaseId?.ToString());

                var newName = $@"{fileNamePrefix}{sourceBlob.Uri.Segments.Last()}";

                var targetBlob = blobRef.Container
                    .GetDirectoryReference(targetFolderName)
                    .GetBlockBlobReference(getTargetFilename?.Invoke(newName) ?? Path.GetFileName(newName));

                string targetLeaseId = null;
                if (useLeases) targetLeaseId = targetBlob.AcquireLease(lease.Value.Timespan, lease.Value.LeaseId?.ToString());

                targetBlob.StartCopy(sourceBlob);

                while (targetBlob.CopyState.Status == CopyStatus.Pending) ;     // spinlock until the copy completes

                bool copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                if (!copySucceeded)
                {
                    log?.Error($@"Error copying {sourceBlob.Name} to {targetFolderName} folder. Retrying once...");

                    targetBlob.StartCopy(sourceBlob);

                    while (targetBlob.CopyState.Status == CopyStatus.Pending) ;     // spinlock until the copy completes

                    copySucceeded = targetBlob.CopyState.Status == CopyStatus.Success;
                    if (!copySucceeded)
                    {
                        log?.Error($@"Error retrying copy of {sourceBlob.Name} to {targetFolderName} folder. File not moved.");
                    }
                }
                
                if (useLeases)
                {
                    sourceBlob.ReleaseLease(new AccessCondition { LeaseId = sourceLeaseId });
                    targetBlob.ReleaseLease(new AccessCondition { LeaseId = targetLeaseId });
                }
            }
        }

        public static async Task<(HttpResponseMessage ResponseToSend, dynamic ParsedEventGridItem)> ProcessEventGridMessageAsync(HttpRequestMessage req, TraceWriter log = null)
        {
            var payloadFromEventGrid = JToken.ReadFrom(new JsonTextReader(new StreamReader(await req.Content.ReadAsStreamAsync())));
            //log?.Verbose($@"Event Grid Payload: {payloadFromEventGrid.ToString()}");

            dynamic eventGridSoleItem = (payloadFromEventGrid as JArray)?.SingleOrDefault();
            if (eventGridSoleItem == null)
            {
                log?.Info("Expecting only one item in the Event Grid message...");
                return (req.CreateErrorResponse(HttpStatusCode.BadRequest, $@"Expecting only one item in the Event Grid message"), null);
            }

            log?.Info($@"eventGridSoleItem received");
            if (eventGridSoleItem.eventType == @"Microsoft.EventGrid.SubscriptionValidationEvent")
            {
                log?.Verbose(@"Event Grid Validation event received.");
                return (new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent($"{{ \"validationResponse\" : \"{((dynamic)payloadFromEventGrid)[0].data.validationCode}\" }}")
                }, null);
            }

            return (null, eventGridSoleItem);
        }

        public static bool IsCurrencyNeutralFile(string fileName)
        {
            if (fileName.Contains("actual_exchange_rates", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("rolling_estimate_exchange_rates", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("business_plan_exchange_rates", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
            return false;
        }

        public static bool IsManualOrAutoSubmissionFile(string fileName)
        {
            if (fileName.Contains("_volume_", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("_revenue_", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("_stddisc_", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("_bulkdisc_", StringComparison.OrdinalIgnoreCase)
                        || fileName.Contains("_offdisc_", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
            return false;
        }

        public static string ReplaceDoubleQuoteAndCommaInValues(string line)
        {

            StringBuilder sbNewLine = new StringBuilder();
            StringBuilder sbValue = new StringBuilder();
            var charArr = line.Trim().ToCharArray();
            bool isStartDoubleQuoteFound = false;
            bool isEndDoubleQuoteFound = false;
            bool isEndCommaAfterDoubleQuoteFound = false;
            int count = 1;
            bool lastchar = false;
            foreach (var c in charArr)
            {

                if (count == charArr.Count())
                {
                    lastchar = true;
                }

                if (isStartDoubleQuoteFound)
                {
                    if (c == '"')
                    {
                        isEndDoubleQuoteFound = true;
                        if (lastchar)
                        {
                            sbValue.Replace("\"", "#q#").Replace(",", "#c#");
                            sbNewLine.Append($@"{sbValue}");
                            isEndCommaAfterDoubleQuoteFound = true;
                        }

                    }
                    else if (isEndDoubleQuoteFound)
                    {
                        if (c == ',')
                        {
                            isEndCommaAfterDoubleQuoteFound = true;
                            sbValue.Remove(sbValue.Length - 1, 1);
                            sbValue.Replace("\"", "#q#").Replace(",", "#c#");
                            sbNewLine.Append($@"{sbValue}");
                            sbNewLine.Append($@",");
                        }
                        else
                            isEndDoubleQuoteFound = false;
                    }
                }

                if (isStartDoubleQuoteFound && !isEndCommaAfterDoubleQuoteFound)
                    sbValue.Append(c.ToString());
                else if (isStartDoubleQuoteFound == false && c != '"')
                    sbNewLine.Append(c.ToString());

                if (isStartDoubleQuoteFound && isEndDoubleQuoteFound && isEndCommaAfterDoubleQuoteFound)
                {
                    isStartDoubleQuoteFound = false;
                    isEndCommaAfterDoubleQuoteFound = false;
                    isEndDoubleQuoteFound = false;
                    sbValue.Clear();
                }

                if (c == '"' && isStartDoubleQuoteFound == false)
                {
                    isStartDoubleQuoteFound = true;
                }
                count++;
            }

            return sbNewLine.ToString();
        }

        public static void CopySourceFileToArchive(TraceWriter log, CloudStorageAccount storageAccount, string containerName, string directoryPath, string fileName, string fileNamePrefix, string fileMovePath)
        {
            try
            {
                var bClient = storageAccount.CreateCloudBlobClient();

                var archiveBlobRef = bClient.GetContainerReference(containerName)
                    .GetDirectoryReference(directoryPath)
                    .GetBlobReference(fileName);

                Helpers.CopyBlobs(bClient, archiveBlobRef, fileNamePrefix, fileMovePath, log: log);

                log.Info($@"File {archiveBlobRef.Uri.Segments.Last()} Copied...");
                log.Info($@"Copy Path - {fileMovePath} ");                
            }
            catch (Exception ex)
            {
                log.Info($@"Exception while copy source file - {ex.Message.ToString()}");                
            }
        }

    }
}

