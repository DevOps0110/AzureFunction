using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace NsrFunctions
{
    public static class EnsureAllFiles
    {
        const string VALID_FILES_SUBFOLDER = @"valid-set-files";
        const string INVALID_FILES_SUBFOLDER = @"invalid-set-files";

        [FunctionName("EnsureAllFiles")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, @"post")]HttpRequestMessage req, [ServiceBus(@"ready-for-validation", Connection = @"ServiceBus_Send")]IAsyncCollector<object> readyToValidateMessage, TraceWriter log)
        {
            //log.Info("EnsureAllFiles - 1st Level Validation ...");

            var (EventGridParseResponseToReturn, eventGridSoleItem) = await Helpers.ProcessEventGridMessageAsync(req, log);
            if (EventGridParseResponseToReturn != null) return EventGridParseResponseToReturn;

            // EnsureAllFiles only cares about EG messages that pertain to us (new blobs in the folder)
            // We don't need to filter on api (putblob, etc) because an uploaded OR copied blob can trigger this Function
            if (!((string)eventGridSoleItem.eventType).Equals(@"Microsoft.Storage.BlobCreated"))
            {
                return req.CreateResponse(HttpStatusCode.NoContent, @"Ignoring event that is not BlobCreated", @"text/plain");
            }

            bool isInvalidFile = false;
            BottlerBlobfileAttributes newExchangeFile = ParseExchangeRateEventGridPayload(eventGridSoleItem, log, Helpers.GetCloudStorageAccount(), out isInvalidFile);
            if (newExchangeFile == null)
            {
                log.Info("The request is not for auto-curr-ntrl/valid-set-files)");

                if (!isInvalidFile)
                {
                    BottlerBlobfileAttributes newCustomerFile = ParseEventGridPayload(eventGridSoleItem, log, Helpers.GetCloudStorageAccount());
                    if (newCustomerFile == null)
                    {
                        log.Info("The request either wasn't valid (filename couldn't be parsed) or not applicable (put in to a folder other than /inbound)");

                        return req.CreateResponse(HttpStatusCode.NoContent);
                    }

                    string bottlerName = newCustomerFile.Subfolder, name = newCustomerFile.FilenameWithoutExtension, containerName = newCustomerFile.ContainerName;

                    // get the prefix for the name so we can check for others in the same container with in the customer blob storage account
                    var prefix = newCustomerFile.BatchPrefix.ToLowerInvariant();

                    //log.Info($@"Processing new file. bottlerName: {bottlerName}, filename: {name}, prefix: {prefix}, containerName: {containerName}");

                    string sourceSysId;
                    IList<string> expectedFiles;
                    using (var sqlClient = new NsrSqlClient())
                    {
                        sourceSysId = newCustomerFile.GetSrcSysId(sqlClient);
                        expectedFiles = sqlClient.GetExpectedFilesForBottler(sourceSysId, newCustomerFile.FactType).ToList();
                    }

                    //log.Info(" FileName Validation Started ");

                    var blobClient = Helpers.GetCloudBlobClient();
                    var matches = (newCustomerFile.FilenameWithoutExtension.Any(c => char.IsUpper(c))
                        ? blobClient.ListBlobs(containerName, $@"{bottlerName}/inbound", prefix, log: log)
                        : blobClient.ListBlobs(prefix: $@"{containerName}/{bottlerName}/inbound/{prefix}")).ToList();

                    var fileNames = matches.Select(m => Path.GetFileName(blobClient.GetBlobReferenceFromServer(m.StorageUri).Name)).ToList();
                    var matchNames = fileNames.Select(f => Path.GetFileNameWithoutExtension(f).Split('_').Last().ToLowerInvariant()).ToList();
                    var filesStillWaitingFor = expectedFiles.Except(matchNames, new BlobFilenameVsDatabaseFileMaskComparer()).ToList();

                    log.Info($@" Files available in storage are {string.Join(", ", fileNames)}");

                    //var fileUri = blobClient.ListBlobs(prefix: $@"{containerName}/inbound/{prefix}").First().Uri.ToString();
                    log.Info($@" Files expected are {string.Join(", ", expectedFiles.OrderBy(i => i))}");
                    log.Info($@" Files received are {string.Join(", ", matchNames.OrderBy(i => i))}");

                    if (filesStillWaitingFor.Any())
                    {
                        log.Info($@" Files waiting for {string.Join(", ", filesStillWaitingFor.OrderBy(i => i))}");
                    }
                    else
                    {
                        log.Info(" All files received ");

                        // Verify that this prefix isn't already in the BottlerFiles table for processings
                        // NSRD-14524
                        (var lockSuccessful, var lockEntity, var lockErrorResponse) = LockTableEntity.TryLock(newCustomerFile.BatchPrefix, () => req.CreateResponse(HttpStatusCode.NoContent), Helpers.GetLockTable(), @"Waiting");
                        if (!lockSuccessful) return lockErrorResponse;

                        using (var sqlClient = new NsrSqlClient())
                        {
                            var fileSetId = sqlClient.GetFileSetId();

                            try
                            {
                                foreach (var i in matches
                                    .Select(m => BottlerBlobfileAttributes.Parse(m.Uri.OriginalString, @"inbound"))
                                    // Can't use the 'GetDbFiletype' request here, because that requires Module ID and that hasn't been inserted in to the DB for this file yet at this point
                                    .Select(a => new { Attributes = a, DbFileType = sqlClient.GetTypeForBottler(sourceSysId, a.FactType, a.Filetype) }))
                                {
                                    sqlClient.BottlerFileReceived(sourceSysId, i.Attributes.Filename, "AF-READY", 2, fileSetId, i.Attributes.FactType, i.DbFileType);
                                    log.Info($@"SourceSysId : {sourceSysId} and File : '{i.Attributes.Filename}'  set to AF-READY ");
                                }

                                Helpers.MoveBlobs(blobClient, matches, $@"{bottlerName}/{VALID_FILES_SUBFOLDER}", log: log);

                                log.Info(@"Got all the files! Moving on...");

                                // Setting the value of this object sends it so the service bus queue specified by the parameter's attribute in the signature of this method
                                await readyToValidateMessage.AddAsync(new Validate.ValidationRequest
                                {
                                    Type = Validate.ValidationRequest.ValidationRequestType.Batch,
                                    //Prefix = $@"{containerName}/{bottlerName}/{VALID_FILES_SUBFOLDER}/{prefix}",
                                    // NSRD-14524
                                    Prefix = $@"{containerName}/{bottlerName}/{VALID_FILES_SUBFOLDER}/{newCustomerFile.BatchPrefix}",
                                    Filetypes = expectedFiles
                                });
                            }
                            catch (StorageException ex)
                            {
                                log.Info($@"Storage Exception: {ex.Message}");
                                var rowAffected = sqlClient.DeleteInvalidData(sourceSysId, fileSetId.ToString());
                                log.Info($@"Row Affected: {rowAffected.ToString()} and Storage Exception: {ex.Message}");
                            }
                        }
                    }
                }
            }
            else
            {
                (var lockSuccessful, var lockEntity, var lockErrorResponse) = LockTableEntity.TryLock(newExchangeFile.Filename, () => req.CreateResponse(HttpStatusCode.NoContent), Helpers.GetLockTable(), @"Waiting");
                if (!lockSuccessful) return lockErrorResponse;

                log.Info("The request is for auto-curr-ntrl/valid-set-files)");

                //Make db entry as AF-READY
                using (var sqlClient = new NsrSqlClient())
                {
                    var fileSetId = sqlClient.GetFileSetId();
                    var sourceSysId = newExchangeFile.GetSrcSysIdForFileType(sqlClient);
                    var moduleId = Convert.ToInt32(sqlClient.GetModuleIdForSrcId(sourceSysId));

                    log.Info($@"Insert - Source System ID {sourceSysId} | File Name {newExchangeFile.Filename} | File Set ID {fileSetId} | AF-READY");
                    sqlClient.BottlerFileReceived(sourceSysId, $@"{newExchangeFile.Filename}", "AF-READY", moduleId, fileSetId, newExchangeFile.FactType, newExchangeFile.FactType.ToLowerInvariant());
                }

                List<string> expectedfile = new List<string>();
                expectedfile.Add($@"{newExchangeFile.FullUrl}");

                // Setting the value of this object sends it so the service bus queue specified by the parameter's attribute in the signature of this method
                await readyToValidateMessage.AddAsync(new Validate.ValidationRequest
                {
                    Type = Validate.ValidationRequest.ValidationRequestType.Batch,
                    //Prefix = $@"{containerName}/{bottlerName}/{VALID_FILES_SUBFOLDER}/{prefix}",
                    // NSRD-14524
                    Prefix = $@"{newExchangeFile.ContainerName}/{newExchangeFile.BottlerName.ToLowerInvariant()}/{VALID_FILES_SUBFOLDER}/{newExchangeFile.Filename}",
                    Filetypes = expectedfile
                });
                log.Info("The request for auto-curr-ntrl/valid-set-files) is processed sucessfully");
            }
            return req.CreateResponse(HttpStatusCode.OK);
        }

        private static BottlerBlobfileAttributes ParseEventGridPayload(dynamic eventGridItem, TraceWriter log, CloudStorageAccount storageAccount)
        {
            log.Info($@"BottlerBlobfileAttributes.Parse URL {(string)eventGridItem.data.url} ");
            log.Info($@"eventGridItem.eventType - {(string)eventGridItem.eventType} and eventGridItem.data.api {(string)eventGridItem.data.api} ");

            if (eventGridItem.eventType == @"Microsoft.Storage.BlobCreated"
                && (eventGridItem.data.api == @"PutBlob" || eventGridItem.data.api == @"PutBlockList" || eventGridItem.data.api == @"CopyBlob"))
            //&& (eventGridItem.data.contentType == @"text/csv" || eventGridItem.data.contentType == "application/vnd.ms-excel"))
            {
                try
                {
                    BottlerBlobfileAttributes retVal = null;
                    try
                    {
                        retVal = BottlerBlobfileAttributes.Parse((string)eventGridItem.data.url, @"inbound");
                    }
                    catch (Exception ex)
                    {
                        log.Error(@"Error parsing Event Grid payload", ex);
                    }

                    #region Check if folder is inbound but fileName is not valid then move to invalid-set

                    var itemUrl = (string)eventGridItem.data.url;
                    var urlParts = itemUrl.Split('/');
                    var isMoveToInvalidSet = false;

                    if (retVal == null)
                    {
                        if (urlParts[urlParts.Length - 2].Equals("inbound", StringComparison.OrdinalIgnoreCase))
                        {
                            isMoveToInvalidSet = true;
                        }
                    }
                    else if (!retVal.Subfolder.Equals(retVal.BottlerName, StringComparison.OrdinalIgnoreCase))
                    {
                        isMoveToInvalidSet = true;
                    }
                    else
                    {
                        //if fact type not found or file mask doesn't match - Move to invalid-set-files
                        if (string.IsNullOrEmpty(retVal.FactType))
                        {
                            isMoveToInvalidSet = true;
                        }
                        else
                        {
                            string sourceSysId;
                            IList<string> expectedFiles;
                            using (var sqlClient = new NsrSqlClient())
                            {
                                sourceSysId = retVal.GetSrcSysId(sqlClient);
                                expectedFiles = sqlClient.GetExpectedFilesForBottler(sourceSysId, retVal.FactType).ToList();
                            }

                            if (!expectedFiles.Contains(retVal.Filetype, StringComparer.OrdinalIgnoreCase))
                            {
                                isMoveToInvalidSet = true;
                            }
                        }
                    }

                    if (isMoveToInvalidSet)
                    {
                        log.Info("File Name Validation Fails");
                        List<IListBlobItem> moveList = new List<IListBlobItem>();
                        var blobClient = storageAccount.CreateCloudBlobClient();
                        var item = blobClient.GetContainerReference(urlParts[urlParts.Length - 4]).GetDirectoryReference($@"{urlParts[urlParts.Length - 3]}/inbound").GetBlobReference(urlParts[urlParts.Length - 1]);
                        moveList.Add(item);

                        string sourceSysId;
                        using (var sqlClient = new NsrSqlClient())
                        {
                            sourceSysId = sqlClient.GetSourceSysIdForBottlerName(urlParts[urlParts.Length - 3], urlParts[urlParts.Length - 4]);

                            var fileSetId = sqlClient.GetFileSetId();
                            sqlClient.BottlerFileReceived(sourceSysId, itemUrl.Split('/').Last(), "AF-SET-REJECTED", 2, fileSetId, "", "");
                            Helpers.MoveBlobs(blobClient, moveList, $@"{urlParts[urlParts.Length - 3]}/{INVALID_FILES_SUBFOLDER}", log: log);

                            try
                            {
                                var err = new List<string>
                                {
                                    $@"Line_Number,Error_Record,Error_Description",
                                    $@"NA,NA,File Name Validation Failed"
                                };

                                var errorFileName = $@"{fileSetId}_Error_{itemUrl.Split('/').Last()}";
                                Helpers.CreateErrorFile(blobClient, urlParts[urlParts.Length - 4], urlParts[urlParts.Length - 3], errorFileName, err, log);

                                sqlClient.InsertIntoErrorTracker(sourceSysId, itemUrl.Split('/').Last(), "ERROR", "AF-SET-REJECTED");
                            }
                            catch (Exception ex)
                            {
                                log.Info($@"Exception during Error File Creation: {ex.Message}");
                            }
                        }

                        retVal = null;
                    }
                    #endregion

                    return retVal;
                }
                catch (Exception ex)
                {
                    log.Error(@"Error parsing Event Grid payload", ex);
                }
            }
            else
            {
                log.Info($@" Parse skipped because not right eventType ({eventGridItem.eventType})");
            }
            return null;
        }

        private static BottlerBlobfileAttributes ParseExchangeRateEventGridPayload(dynamic eventGridItem, TraceWriter log, CloudStorageAccount storageAccount, out bool isInvalidFile)
        {
            isInvalidFile = false;
            log.Info($@"BottlerBlobfileAttributes.Parse URL {(string)eventGridItem.data.url} ");
            log.Info($@"eventGridItem.eventType - {(string)eventGridItem.eventType} and eventGridItem.data.api {(string)eventGridItem.data.api} ");

            if (eventGridItem.eventType == @"Microsoft.Storage.BlobCreated"
                && (eventGridItem.data.api == @"PutBlob" || eventGridItem.data.api == @"PutBlockList" || eventGridItem.data.api == @"CopyBlob"))
            //&& (eventGridItem.data.contentType == @"text/csv" || eventGridItem.data.contentType == "application/vnd.ms-excel"))
            {
                try
                {
                    BottlerBlobfileAttributes retVal = null;
                    try
                    {
                        retVal = BottlerBlobfileAttributes.Parse((string)eventGridItem.data.url, @"auto-curr-ntrl");
                    }
                    catch (Exception ex)
                    {
                        log.Error(@"Error parsing Event Grid payload", ex);
                    }

                    #region Check if folder is auto-curr-ntrl/valid-set-files but fileName is not valid then move to invalid-set

                    var itemUrl = (string)eventGridItem.data.url;
                    var urlParts = itemUrl.Split('/');
                    var isMoveToInvalidSet = false;

                    if (retVal == null)
                    {
                        if (urlParts[urlParts.Length - 2].Equals("valid-set-files", StringComparison.OrdinalIgnoreCase)
                            && urlParts[urlParts.Length - 3].Equals("auto-curr-ntrl", StringComparison.OrdinalIgnoreCase))
                        {
                            isMoveToInvalidSet = true;
                        }
                    }

                    if (isMoveToInvalidSet)
                    {
                        isInvalidFile = true;
                        log.Info("File Name Validation Fails");
                        List<IListBlobItem> moveList = new List<IListBlobItem>();
                        var blobClient = storageAccount.CreateCloudBlobClient();
                        var item = blobClient.GetContainerReference(urlParts[urlParts.Length - 4]).GetDirectoryReference($@"auto-curr-ntrl/valid-set-files").GetBlobReference(urlParts[urlParts.Length - 1]);
                        moveList.Add(item);

                        string sourceSysId;
                        using (var sqlClient = new NsrSqlClient())
                        {
                            sourceSysId = sqlClient.GetSourceSysIdForBottlerName(urlParts[urlParts.Length - 3], urlParts[urlParts.Length - 4]);
                            var moduleId = Convert.ToInt32(sqlClient.GetModuleIdForSrcId(sourceSysId));

                            var fileSetId = sqlClient.GetFileSetId();
                            sqlClient.BottlerFileReceived(sourceSysId, itemUrl.Split('/').Last(), "AF-SET-REJECTED", moduleId, fileSetId, "", "");
                            Helpers.MoveBlobs(blobClient, moveList, $@"{urlParts[urlParts.Length - 3]}/{INVALID_FILES_SUBFOLDER}", log: log);

                            try
                            {
                                var err = new List<string>
                                {
                                    $@"Line_Number,Error_Record,Error_Description",
                                    $@"NA,NA,File Name Validation Failed"
                                };

                                var errorFileName = $@"{fileSetId}_Error_{itemUrl.Split('/').Last()}";
                                Helpers.CreateErrorFile(blobClient, urlParts[urlParts.Length - 4], urlParts[urlParts.Length - 3], errorFileName, err, log);

                                sqlClient.InsertIntoErrorTracker(sourceSysId, itemUrl.Split('/').Last(), "ERROR", "AF-SET-REJECTED");
                            }
                            catch (Exception ex)
                            {
                                log.Info($@"Exception during Error File Creation: {ex.Message}");
                            }
                        }

                        retVal = null;
                    }
                    #endregion

                    return retVal;
                }
                catch (Exception ex)
                {
                    log.Error(@"Error parsing Event Grid payload", ex);
                }
            }
            else
            {
                log.Info($@" Parse skipped because not right eventType ({eventGridItem.eventType})");
            }
            return null;
        }
    }
}