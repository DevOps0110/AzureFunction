using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using ExcelDataReader;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using NsrFunctions;

namespace NsrFunctions
{
    public static class ProcessNonNSRFiles
    {
        private const string UTF8_ENCODED_LOCK_STATUS = @"utf8encoded";
        private const string NON_NSR_PROCESSING_LOCK_STATUS = @"nonNsrProcessing";
        private const string NON_NSR_VALID_SET_LOCK_STATUS = @"nonNsrValidSetMove";

        [FunctionName("ProcessNonNSRFiles")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, @"post")]HttpRequestMessage req, TraceWriter log)
        {
            List<string> logInfoList = new List<string>();
            #region PayLoad Events
            log.Info($@"File Conversion Process Started V-1.0.72.0");
            logInfoList.Add($@"File Conversion Process Started V-1.0.72.0");

            var (EventGridParseResponseToReturn, eventGridSoleItem) = await Helpers.ProcessEventGridMessageAsync(req, log);
            if (EventGridParseResponseToReturn != null) return EventGridParseResponseToReturn;

            var storageAccount = Helpers.GetCloudStorageAccount();
            #endregion
            logInfoList.Add($@"file eventtype {eventGridSoleItem.eventType} and contenttype {eventGridSoleItem.data.contentType} and api {eventGridSoleItem.data.api} and url {eventGridSoleItem.data.url} ");
            log.Info($@"file eventtype {eventGridSoleItem.eventType} and contenttype {eventGridSoleItem.data.contentType} and api {eventGridSoleItem.data.api} and url {eventGridSoleItem.data.url} ");
            if (eventGridSoleItem.eventType == @"Microsoft.Storage.BlobCreated"
               // CopyBlob events should never be handled by this Function, for instance
               && (eventGridSoleItem.data.api == @"PutBlob" || eventGridSoleItem.data.api == @"PutBlockList"))
            {
                string eventGridItemUrl = (string)eventGridSoleItem.data.url;

                // NSRD-18800
                var (fileIsZip, parentZipFolder, parentZipBottlerFolder) = Helpers.IsZipLandingFile(eventGridItemUrl);
                if (fileIsZip)
                {
                    var zipFileParts = NonNSRBlobfileAttributes.ParseLandingZipFile(eventGridItemUrl);

                    var blobClient = Helpers.GetCloudBlobClient();
                    var blobToUnzip = blobClient.GetBlobReferenceFromServer(new Uri(eventGridItemUrl));
                    var targetFolder = $@"{blobToUnzip.Container.Name}/{parentZipFolder}";
                    string sourceSysId = new NsrSqlClient().GetSourceSysIdForNonNSRFile(targetFolder);
                    NsrFileCnvrDataRow zipRecord = new NsrSqlClient().GetConfigurationRecord(sourceSysId);
                    log.Info($@"It's zip file - {blobToUnzip.Name} and path - {targetFolder} and sourceSysId - {sourceSysId}");
                    logInfoList.Add($@"It's zip file - {blobToUnzip.Name} and path - {targetFolder} and sourceSysId - {sourceSysId}");

                    try
                    {
                        using (var zipBlobFileStream = new MemoryStream())
                        {
                            var directory = blobToUnzip.Container.GetDirectoryReference(parentZipFolder);

                            await blobToUnzip.DownloadToStreamAsync(zipBlobFileStream);
                            await zipBlobFileStream.FlushAsync();
                            zipBlobFileStream.Position = 0;

                            using (var zip = new ZipArchive(zipBlobFileStream))
                            {
                                foreach (var entry in zip.Entries)
                                {
                                    var blob = directory.GetBlockBlobReference($@"{entry.FullName}");
                                    logInfoList.Add($@"unzip file - {entry.FullName}");

                                    using (var stream = entry.Open())
                                    {
                                        await blob.UploadFromStreamAsync(stream);
                                        logInfoList.Add($@"unzip file upload completed for - {entry.FullName}");
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Info($@"{ex.Message}");
                        logInfoList.Add($@"{ex.Message}");

                        //log error in standard error file
                        LogStandardError(storageAccount, zipFileParts, sourceSysId,
                            $@"Error in unzipping of file {zipFileParts.FullFilename}",
                            zipRecord, zipFileParts.FullFilename, log);
                    }

                    var zipArchiveFolder = $@"{parentZipBottlerFolder}/archive";
                    Helpers.MoveBlobs(blobClient, blobToUnzip, zipArchiveFolder, log: log);

                    log.Info($@"zip file {blobToUnzip.Name} has been archived to path {zipArchiveFolder}");
                    logInfoList.Add($@"zip file {blobToUnzip.Name} has been archived to path {zipArchiveFolder}");
                    Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{blobToUnzip.Name.Split('/').Last().ToLowerInvariant().Replace(".zip", ".csv")}", logInfoList, $@"{blobToUnzip.Container.Name}/{parentZipBottlerFolder}", log);
                }

                // NSRP-4110
                var (fileIsV3, parentFolder) = Helpers.IsV3BottlerLandingFile(eventGridItemUrl);
                if (fileIsV3)
                {
                    var blobClient = Helpers.GetCloudBlobClient();
                    var blobToMove = blobClient.GetBlobReferenceFromServer(new Uri(eventGridItemUrl));

                    var targetFolder = $@"{parentFolder}/inbound";
                    Helpers.MoveBlobs(blobClient, blobToMove, targetFolder, log: log);

                    // Would love to return 301 Moved here, but Event Grid will see that as a failure and keep retrying. Instead, return NoContent with a message that the file was moved
                    log.Info($@"File determined to be a v3 file; moved to {targetFolder}");
                    logInfoList.Add($@"File determined to be a v3 file; moved to {targetFolder}");
                    return req.CreateResponse(HttpStatusCode.NoContent, $@"File determined to be a v3 file; moved to {targetFolder}", @"text/plain");
                }

                var landingFileParts = NonNSRBlobfileAttributes.ParseLandingFile(eventGridItemUrl);

                if (landingFileParts != null)
                {
                    string bottlerName = landingFileParts.BottlerName;
                    string fileName = landingFileParts.FullFilename;
                    string landingFilesPath = $@"{landingFileParts.ContainerName}/{landingFileParts.FullPathToFolderWithinContainer}";

                    // Don't handle file triggers coming from our UTF-8 conversion & upload
                    var lockTable = Helpers.GetLockTable();
                    var tableEntryFileName = $@"{landingFileParts.ContainerName}_{landingFileParts.BottlerName}_{fileName}";

                    (var lockSuccessful, var lockEntity, var lockErrorResponse) = LockTableEntity.TryLock(tableEntryFileName, () => req.CreateResponse(HttpStatusCode.NoContent), lockTable, status: NON_NSR_PROCESSING_LOCK_STATUS);
                    if (!lockSuccessful)
                    {
                        if (lockEntity?.DbState?.Equals(UTF8_ENCODED_LOCK_STATUS) == true)
                        {
                            // NSRP-4154 - We just fielded the storage event that was triggered when we re-uploaded the file as a utf-8-encoded CSV. Ignore this event, and delete the lock
                            LockTableEntity.DeleteWithWarning(tableEntryFileName);
                        }

                        return lockErrorResponse;
                    }

                    #region Get SourceSysId For BottlerPath
                    string sourceSysId;
                    try
                    {
                        sourceSysId = new NsrSqlClient().GetSourceSysIdForNonNSRFile(landingFilesPath);
                        log.Info($@"FileName - {fileName} and Src_Sys_Id - {sourceSysId}");

                        if (string.IsNullOrEmpty(sourceSysId))
                        {
                            // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                            log.Info("Deleting lock table entry as Source Sytem Id not found");
                            LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                            return req.CreateResponse(HttpStatusCode.NoContent, $@"Source Systme ID is not found in the configuration for FileName - {fileName}", @"text/plain");
                        }
                    }
                    catch (Exception ex)
                    {
                        log.Info($@"Exception for Get SourceSysId For BottlerPath - {ex.Message}");

                        // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                        log.Info("Deleting lock table entry as exception found while fetching source Sytem Id");
                        LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                        return req.CreateResponse(HttpStatusCode.NoContent);
                    }
                    #endregion

                    if (!string.IsNullOrEmpty(sourceSysId))
                    {
                        var allRecords = new NsrSqlClient().GetFilesRequiredForConversion(sourceSysId)
                            .ToList();  // avoid running SQL query multiple times
                        if (allRecords.Any())
                        {
                            //Copy source blob to archive with prefix 'SRC~' for reference
                            var fileCopyPath = $@"{landingFileParts.BottlerName}/archive";
                            //CopySourceFileToArchive(log, storageAccount, landingFileParts, fileName, "SRC~", fileCopyPath, ref logInfoList);
                            Helpers.CopySourceFileToArchive(log, storageAccount, landingFileParts.ContainerName, landingFileParts.FullPathToFolderWithinContainer, fileName, "SRC~", fileCopyPath);

                            log.Info($@"Get all configuration records");
                            NsrFileCnvrDataRow currentRecord; string suffix; HttpResponseMessage errorResponse;
                            try
                            {
                                (currentRecord, suffix, errorResponse) = GetCurrentRecordAndSuffix(req, fileName, allRecords, log);
                                if (errorResponse != null)
                                {
                                    log.Info($@"Got error response while fetching matching record from configuration for {fileName}");
                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - found error while fetching matching record from configuration");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                    return errorResponse;
                                }

                                //If configuration not found for uploaded file then file should be archived - NSRD-13164
                                if (!fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) && string.IsNullOrEmpty(currentRecord.FileNamePattern))
                                {
                                    log.Info($@"Configuration not found for file {fileName} hence file has been rejected");
                                    var fileMovePath = $@"{landingFileParts.BottlerName}/invalid-data/rejects";
                                    //ArchiveFiles(log, storageAccount, landingFileParts, fileName, fileMovePath);

                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - configuration not found for uploaded file");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                    //get required details for uploaded file                                    
                                    NsrFileCnvrDataRow archiveRecord = new NsrSqlClient().GetConfigurationRecord(sourceSysId);

                                    //log error in standard error file
                                    LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                        $@"Configuration not found for file {fileName} hence file has been rejected",
                                        archiveRecord, fileName, log, "FILE-NOT-REQUIRED");

                                    return req.CreateResponse(HttpStatusCode.NoContent);
                                }

                                //NSRP4888
                                #region Create ShipFrom LCF File Logic
                                log.Info($@"Check file name for LCF - {fileName}");

                                if (fileName.Contains("_"))
                                {
                                    //Identify the file is shipfrom file
                                    var shipFromFileNamePattern = new NsrSqlClient().GetShipFromConfig(sourceSysId);
                                    log.Info($@"LCF Shipfrom file name pattern - {shipFromFileNamePattern}");
                                    logInfoList.Add($@"LCF Shipfrom file name pattern - {shipFromFileNamePattern}");

                                    if (!string.IsNullOrEmpty(shipFromFileNamePattern))
                                    {                                        
                                        if (shipFromFileNamePattern.Split('_').Last().ToLowerInvariant() == fileName.Split('_').Last().ToLowerInvariant())
                                        {
                                            //Read LCF_IND Configuration from t_src_sys_nsr_file_cnvr table                                            
                                            var lcfCnfg = new NsrSqlClient().GetLCFCnfgForNonNSRFile(landingFilesPath);
                                            log.Info($@"lcfCnfg: {lcfCnfg} ");
                                            logInfoList.Add($@"lcfCnfg: {lcfCnfg} ");

                                            //if present then create in landing folder
                                            if (lcfCnfg == "True")
                                            {                                                
                                                var createNewFilePath = $@"{landingFileParts.BottlerName}/landing-files";
                                                log.Info($@"Create LCF file in {createNewFilePath}");
                                                logInfoList.Add($@"Create LCF file in {createNewFilePath}");

                                                var blobClient = storageAccount.CreateCloudBlobClient();
                                                var blob = blobClient
                                                    .GetContainerReference(landingFileParts.ContainerName)
                                                    .GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer)
                                                    .GetBlobReference(fileName);

                                                // I'm scoping usage of the this code so it can be GC'd ASAP
                                                {
                                                    var doubleQuotedContent = ExtractFileContentAsDoubleQuotedCsvLines(blob, log, true);
                                                    var lcfFileName = $@"{fileName.ToLowerInvariant().Replace(".csv", "").Replace(".txt", "")}";

                                                    if (fileName.ToLowerInvariant().Contains(".csv"))
                                                        lcfFileName = $@"{lcfFileName}lcf.csv";
                                                    else
                                                        lcfFileName = $@"{lcfFileName}lcf.txt";

                                                    Helpers.UploadBlobAsPutBlockList(blobClient, landingFileParts.ContainerName, landingFileParts.FullPathToFolderWithinContainer, lcfFileName, doubleQuotedContent);
                                                    log.Info($@"{lcfFileName} file created successfully in {createNewFilePath}");
                                                    logInfoList.Add($@"{lcfFileName} file created successfully in {createNewFilePath}");
                                                }
                                            }
                                        }
                                    }
                                }
                                #endregion
                            }
                            catch (Exception ex)
                            {
                                log.Info($@"Exception while fetching configuration record for uploaded file - {ex.Message}");

                                // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                log.Info("Deleting lock table entry - configuration not found for uploaded file");
                                LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                return req.CreateResponse(HttpStatusCode.NoContent);
                            }

                            //Receive partial file processing flag and error rows id available
                            int partialProcessed = 0;
                            List<string> rejectedErrorRows;

                            // Convert Latin Fixed Width File to Pipe Formatted File
                            try
                            {
                                log.Info($@"Pipe formatted file creation started for - suffix - {suffix}");
                                int processed = 0;

                                //received process status and rejected error rows if available
                                (processed, rejectedErrorRows) = ProcessNonNSRLatinFFFiles(storageAccount, currentRecord.FileNamePattern,
                                    sourceSysId, allRecords, landingFileParts, log, ref logInfoList);

                                if (processed == 0)
                                {
                                    log.Info(@"No conversion needed.");
                                    logInfoList.Add(@"No conversion needed.");
                                }
                                else if (processed == 2)
                                {
                                    log.Info(@"File processed partially");
                                    logInfoList.Add(@"File processed partially");
                                }
                                partialProcessed = processed;

                                //NSRD-13888 - check if all expected file conversion completed and received CONT then create CONT~conv~ file
                                //Create CONT~conv~ file only if the same file is not present otherwise don't create
                                if ((fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) && !fileName.Contains("~conv~", StringComparison.OrdinalIgnoreCase))
                                    || (!fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) && fileName.Contains("~conv~", StringComparison.OrdinalIgnoreCase)))

                                {
                                    log.Info($@"Checking for pending file conversion if any...");
                                    logInfoList.Add($@"Checking for pending file conversion if any...");

                                    var file = landingFileParts.FullFilename;
                                    var fileSuffix = fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) ? file.Substring(4) : file.Substring(currentRecord.FileNamePattern.Length).Replace("~conv~", "");
                                    var convFileSuffix = CreateFileNameForConvertedFile(fileSuffix);
                                    var contFileWithSuffix = string.Concat("CONT", convFileSuffix);

                                    //log.Info($@"FileName - {contFileWithSuffix} and filesuffix {fileSuffix} ");

                                    var directory = storageAccount.CreateCloudBlobClient().GetContainerReference(landingFileParts.ContainerName).GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer);

                                    var allBlobs = directory.ListBlobs();
                                    var missingFiles = allRecords.Where(record => !allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"{record.FileNamePattern}{convFileSuffix}", StringComparison.OrdinalIgnoreCase))).AsParallel();

                                    var isContReceived = allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"CONT{fileSuffix}", StringComparison.OrdinalIgnoreCase));

                                    var isContConvReceived = allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"CONT{convFileSuffix}", StringComparison.OrdinalIgnoreCase));

                                    if (!missingFiles.Any() && isContReceived && !isContConvReceived)
                                    {
                                        log.Info($@"No dependency pending - Create {contFileWithSuffix}");
                                        logInfoList.Add($@"No dependency pending - Create {contFileWithSuffix}");
                                        //create CONT~conv~ file
                                        var blockBlob = directory.GetBlockBlobReference(contFileWithSuffix);
                                        blockBlob.Properties.ContentType = "text/csv; charset=utf-8";

                                        // Create a 0-byte CONT~conv~ file if not exist
                                        if (!blockBlob.Exists())
                                        {
                                            // Create a 0-byte file w/ the required name
                                            //blockBlob.UploadFromByteArray(new byte[0], 0, 0, AccessCondition.GenerateIfNotExistsCondition());

                                            blockBlob.UploadFromByteArray(new byte[0], 0, 0);

                                            log.Info($@"filename: {contFileWithSuffix} file created successfully...");
                                            logInfoList.Add($@"filename: {contFileWithSuffix} file created successfully...");
                                        }

                                        log.Info($@"End for cont~conv~ file conversion...");
                                        logInfoList.Add($@"End for cont~conv~ file conversion...");
                                    }
                                }

                            }
                            catch (Exception ex)
                            {
                                log.Info($@"Exception for Convert Latin Fixed Width File to Pipe Formatted File - {ex.Message}");
                                logInfoList.Add($@"Exception for Convert Latin Fixed Width File to Pipe Formatted File - {ex.Message}");

                                // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                log.Info("Deleting lock table entry - exception in converting file to pipe formatted file");
                                logInfoList.Add("Deleting lock table entry - exception in converting file to pipe formatted file");

                                LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                //Set AF-REJECTED if configuration not matched with file content - NSRD-13156
                                LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                    $@"Error in convert latin fixed width file to pipe formatted file for {fileName}",
                                    currentRecord, fileName, log);

                                Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                                return req.CreateResponse(HttpStatusCode.NoContent);
                            }


                            bool isNonLatinFile = fileName.Contains('_');

                            // If file is not latin format then insert into database as and when upload - NSRP-4153 & NSRD-13850 
                            // NSRD-13110 - Non-Latin Files should wait until all dependent files arrived and then make entry into audit table
                            // If file is latin format 1,2,3 and 4 then wait until CONT file arrives then make entry in audit table for uploaded file
                            if (!string.IsNullOrEmpty(suffix) && isNonLatinFile)
                            {
                                log.Info($@"File name contains underscore");
                                logInfoList.Add($@"File name contains underscore");
                                try
                                {
                                    // Convert to double-quote and Upload if non-Latin
                                    // TODO: Why do we do this conversion & upload and continue on processing the file? Since this holds on to the whole file in memory (GetFileContentAsDoubleQuotedCsvLines) could we not process it from that if we needed to utilize the content?
                                    if (isNonLatinFile)
                                    {
                                        var blobClient = storageAccount.CreateCloudBlobClient();
                                        var blob = blobClient
                                            .GetContainerReference(landingFileParts.ContainerName)
                                            .GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer)
                                            .GetBlobReference(fileName);

                                        // I'm scoping usage of the doubleQuotedFileContent so it can be GC'd ASAP
                                        {
                                            var doubleQuotedContent = ExtractFileContentAsDoubleQuotedCsvLines(blob, log, true);

                                            // doing the following upload will trigger this Function to run again, but ShouldProceed will skip execution if it sees the lock table's entry indicates this was the reason
                                            LockTableEntity.Update(tableEntryFileName, UTF8_ENCODED_LOCK_STATUS);
                                            Helpers.UploadBlobAsPutBlockList(blobClient, landingFileParts.ContainerName, landingFileParts.FullPathToFolderWithinContainer, fileName, doubleQuotedContent);
                                        }
                                    }
                                }
                                catch
                                {
                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - exception in converting file to UTF8 format");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                    //log error in standard error file
                                    LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                        $@"Conversion error occured for file {fileName}",
                                        currentRecord, fileName, log);

                                    return req.CreateResponse(HttpStatusCode.NoContent);
                                }

                                // NSRD-   - Process files and make DB entries once entire set is available in landing-files folder
                                try
                                {
                                    log.Info($@"Checking for dependent files...");
                                    logInfoList.Add($@"Checking for dependent files...");
                                    // Find Dependent Records
                                    var dependentRecords = allRecords.ToList();  // ToList to keep from re-executing query

                                    (IList<string> requiredFiles, bool isLatinFileSet, string newSuffix) = GetRequiredFilesAndLatinFilesetIndicator(dependentRecords, isNonLatinFile, fileName, log, ref logInfoList);


                                    if (string.IsNullOrEmpty(newSuffix)) newSuffix = suffix;
                                    log.Info($@"Required files are {string.Join(",", requiredFiles)}");
                                    logInfoList.Add($@"Required files are {string.Join(",", requiredFiles)}");

                                    ProcessDepedendentRecordsAndDetermineFurtherRequiredFiles(ref dependentRecords, newSuffix, isLatinFileSet, landingFileParts, ref requiredFiles, log, ref logInfoList);

                                    //log.Info($@"Required files should be empty - Required files are {string.Join(",", requiredFiles)}");

                                    log.Info("Going to make entries in DB...");
                                    logInfoList.Add("Going to make entries in DB...");

                                    errorResponse = UpdateDbWorkflowEntriesForDependentRecords(req, landingFileParts, dependentRecords, requiredFiles, sourceSysId, newSuffix, lockTable, storageAccount, log, ref logInfoList);

                                    Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                                    if (errorResponse != null)
                                    {
                                        //log error in standard error file
                                        LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                            $@"Error occured in finding dependent records for file {fileName}",
                                            currentRecord, fileName, log);

                                        return errorResponse;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    log.Info($@"Exception while dependency check - {ex.Message}");

                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info($@"Deleting lock table entry - error while dependecy check for {fileName}");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);
                                }

                            }
                            else if (fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) && fileName.Contains("~conv~", StringComparison.OrdinalIgnoreCase))
                            {
                                log.Info($@"File is south latin - CONT~conv~ file");
                                logInfoList.Add($@"File is south latin - CONT~conv~ file");

                                try
                                {
                                    //check what all files are available
                                    var fileSuffix = fileName.Substring(4);

                                    var directory = storageAccount.CreateCloudBlobClient().GetContainerReference(landingFileParts.ContainerName).GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer);

                                    var allBlobs = directory.ListBlobs();
                                    var recordsForFilesArrived = allRecords.Where(record => allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"{record.FileNamePattern}{fileSuffix}", StringComparison.OrdinalIgnoreCase)));

                                    //TO DO: Need to check if all files are 0kb files then don't create entry as INFA-READY

                                    errorResponse = UpdateDbEntriesForLatinRecords(req, landingFileParts, recordsForFilesArrived, sourceSysId, fileSuffix, lockTable, storageAccount, log, ref logInfoList);
                                    Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                                    // update the "lock" record from the bottler filesets table; we're done processing the fileset
                                    // now all entries in db will be taken care by CONT file so no need to apply lock while inserting 
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);
                                    bool isException = false;

                                    //NSRD-13888 - Moving CONT file once all entries made to database so same filename can be reprocess again
                                    try
                                    {
                                        //Archive CONT ~conv~ file
                                        var blobClient = Helpers.GetCloudBlobClient();
                                        var blobToMove = directory.GetBlobReference(fileName);
                                        var targetFolder = $@"{landingFileParts.BottlerName}/archive";
                                        Helpers.MoveBlobs(blobClient, blobToMove, targetFolder, log: log);

                                        //Archive CONT source file
                                        var prefix = fileName.ToLowerInvariant().Replace("~conv~.csv", "").Replace("~conv~.txt", "");
                                        var matches = blobClient.ListBlobs(landingFileParts.ContainerName, $@"{landingFileParts.BottlerName}/landing-files", prefix, log: log);
                                        Helpers.MoveBlobs(blobClient, matches, targetFolder, log: log);

                                    }
                                    catch (Exception ex)
                                    {
                                        isException = true;

                                        if (errorResponse == null)
                                        {
                                            //log error in standard error file
                                            LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                                $@"Error occured in archiving CONT file {fileName}",
                                                currentRecord, fileName, log);
                                        }
                                        log.Info($@"Exception while archiving CONT files - {ex.Message}");
                                        logInfoList.Add($@"Exception while archiving CONT files - {ex.Message}");
                                    }

                                    if (errorResponse != null)
                                    {
                                        string errMessage = "Error occured in bottler filesets table update";

                                        if (isException)
                                            errMessage = "Error occured in archiving CONT files and bottler filesets table update";
                                        //log error in standard error file
                                        LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                            errMessage, currentRecord, fileName, log);
                                        return errorResponse;
                                    }
                                }
                                catch (Exception ex)
                                {
                                    log.Info($@"Exception for Convert Latin Fixed Width File to Pipe Formatted File - {ex.Message}");
                                    logInfoList.Add($@"Exception for Convert Latin Fixed Width File to Pipe Formatted File - {ex.Message}");

                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - exception while inserting records to database");
                                    logInfoList.Add("Deleting lock table entry - exception while inserting records to database");

                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                    //log error in standard error file
                                    LogStandardError(storageAccount, landingFileParts, sourceSysId,
                                        $@"Error occured in convert latin fixed width file {fileName} to pipe formatted file",
                                        currentRecord, fileName, log);

                                    return req.CreateResponse(HttpStatusCode.NoContent);
                                }
                            }
                            else if (!fileName.Contains('_') && !currentRecord.IsSouthLatinFile && !currentRecord.HasDatetimeFormat && !fileName.StartsWith("CONT", StringComparison.OrdinalIgnoreCase))
                            {
                                //Check for South Pacific files without underscore
                                // NSRD-14637: This is filename format with out datetime for South Pacific Bottler.

                                log.Info($@"File is south pacific file");
                                logInfoList.Add($@"File is south pacific file");

                                try
                                {

                                    var blobClient = storageAccount.CreateCloudBlobClient();
                                    var blob = blobClient
                                        .GetContainerReference(landingFileParts.ContainerName)
                                        .GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer)
                                        .GetBlobReference(fileName);

                                    // I'm scoping usage of the doubleQuotedFileContent so it can be GC'd ASAP
                                    {
                                        var doubleQuotedContent = ExtractFileContentAsDoubleQuotedCsvLines(blob, log, true);

                                        // doing the following upload will trigger this Function to run again, but ShouldProceed will skip execution if it sees the lock table's entry indicates this was the reason
                                        LockTableEntity.Update(tableEntryFileName, UTF8_ENCODED_LOCK_STATUS);
                                        Helpers.UploadBlobAsPutBlockList(blobClient, landingFileParts.ContainerName, landingFileParts.FullPathToFolderWithinContainer, fileName, doubleQuotedContent);
                                    }

                                }
                                catch
                                {
                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - exception in converting file to UTF8 format");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                                    return req.CreateResponse(HttpStatusCode.NoContent);
                                }

                                log.Info($@"Checking for dependencies");

                                try
                                {
                                    log.Info($@"Check for all dependent files");

                                    // Find Dependent Records
                                    var dependentRecords = allRecords.ToList();  // ToList to keep from re-executing query

                                    (IList<string> requiredFiles, bool isLatinFileSet) = GetRequiredFilesForNonSouthFilesetWithOutDT(dependentRecords, isNonLatinFile);

                                    log.Info($@"Required files are {string.Join(",", requiredFiles)}");
                                    logInfoList.Add($@"Required files are {string.Join(",", requiredFiles)}");

                                    ProcessDepedendentRecordsAndDetermineFurtherRequiredFilesForSouthPacific(ref dependentRecords, landingFileParts, ref requiredFiles, log, ref logInfoList);

                                    log.Info("Going to make entries in DB");
                                    logInfoList.Add("Going to make entries in DB");
                                    errorResponse = UpdateDbWorkflowEntriesForDependentRecords(req, landingFileParts, dependentRecords, requiredFiles, sourceSysId, string.Empty, lockTable, storageAccount, log, ref logInfoList);

                                    Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                                    if (errorResponse != null) return errorResponse;
                                }

                                catch (Exception ex)
                                {
                                    log.Info($@"Exception while dependency check - {ex.Message}");

                                    // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                    log.Info("Deleting lock table entry - error while dependecy check");
                                    LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);
                                }

                            }
                            else
                            {
                                log.Info("Either file is source file for south latin or file is not south latin, not south pacific and doesnt contain underscore in file name as well");
                                logInfoList.Add("Either file is source file for south latin or file is not south latin, not south pacific and doesnt contain underscore in file name as well");
                                // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                                LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);
                                log.Info("File Conversion Completed");
                                logInfoList.Add("File Conversion Completed");

                                Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                            }

                            //Load error rows into separate file, if error rows is available
                            if (partialProcessed == 2)
                            {
                                log.Info("Logged error AF-REJECTED");
                                logInfoList.Add("Logged error AF-REJECTED");

                                new NsrSqlClient().InsertForNSRFileReceived(sourceSysId, currentRecord, fileName, "AF-REJECTED");

                                var blobClient = storageAccount.CreateCloudBlobClient();

                                log.Info("Logged error text into file started");
                                logInfoList.Add("Logged error text into file started");

                                if (rejectedErrorRows.Count > 100000)
                                    rejectedErrorRows.RemoveRange(99999, rejectedErrorRows.Count - 100000);

                                LogErrorsToFile(blobClient, rejectedErrorRows, sourceSysId, landingFileParts, log, null, false);

                                var fileMovePath = $@"{landingFileParts.BottlerName}/invalid-data/rejects";
                                ArchiveFiles(log, storageAccount, landingFileParts, fileName, fileMovePath);

                                log.Info($@"Logged error sucessfully and moved file {fileName} to invalid-data/rejects");
                                logInfoList.Add($@"Logged error sucessfully and moved file {fileName} to invalid-data/rejects");

                                Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);
                            }


                        }
                        else
                        {
                            // Deleting this entry from lock table will allow re-processing of the same file name for SL file format 1,2,3 & 4 
                            log.Info("Deleting lock table entry as no confifurationg found");
                            logInfoList.Add("Deleting lock table entry as no confifurationg found");

                            LockTableEntity.DeleteWithWarning(tableEntryFileName, lockTable, log: log);

                            //Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{fileName}", logInfoList, $@"{landingFileParts.ContainerName}/{landingFileParts.BottlerName}", log);

                            return req.CreateResponse(HttpStatusCode.NoContent, $@"Configuration not found for source system id - {sourceSysId}", @"text/plain");
                        }
                    }
                }
                else
                {   //'nsr-files' file
                    log.Info($@"Checking for non-nsr files (nsr-files folder)...");
                    var nonNsrFileParts = NonNSRBlobfileAttributes.ParseNSRFile(eventGridItemUrl);

                    if (nonNsrFileParts != null)
                    {
                        var blobClient = storageAccount.CreateCloudBlobClient();
                        var directory = blobClient.GetContainerReference(nonNsrFileParts.ContainerName)
                            .GetDirectoryReference(nonNsrFileParts.FullPathToFolderWithinContainer);

                        var sourceBlob = directory.GetBlobReference(nonNsrFileParts.FullFilename);
                        log.Info($@"file storage uri: {sourceBlob.Name}");

                        var allBlobsInDirectory = directory.ListBlobs();
                        //int newFileCode = DetermineFileCode(nonNsrFileParts, allBlobsInDirectory, log);

                        // Upload double-quoted file with sequence number in filename. Put the ~ here because then the storage event for this upload won't trigger processing of this same file through this code again (due to regex behind nonNsrFileParts)
                        //var newFileName = $@"{newFileCode}~{nonNsrFileParts.Filename}.csv";

                        var newFileCode = nonNsrFileParts.Filename.Split('~').First();
                        var newFileName = $@"{newFileCode}@{nonNsrFileParts.Filename.Split('~').Last()}.csv";

                        // I'm scoping usage of the doubleQuotedFileContent so it can be GC'd ASAP
                        {
                            var doubleQuotedFileContent = ExtractFileContentAsDoubleQuotedCsvLines(sourceBlob, log, false);
                            Helpers.UploadBlobAsPutBlockList(blobClient, nonNsrFileParts.ContainerName, nonNsrFileParts.FullPathToFolderWithinContainer, newFileName, doubleQuotedFileContent);
                        }

                        if (!sourceBlob.DeleteIfExists())
                        {
                            log.Warning($@"Source blob for converted file ({sourceBlob.Name}) was not deleted after re-upload as CSV");
                        }

                        // Move All Files to valid-set-files if we have a full set
                        log.Info($@"Check if Entire Set is Available ");
                        var expectedFiles = GetExpectedFiles(nonNsrFileParts, log);

                        // Move entire set if all files are present                         
                        try
                        {
                            var filesToMove = allBlobsInDirectory
                                .Select(i => new { Item = i, LastSegment = i.Uri.Segments.Last() })
                                .Where(i => i.LastSegment.Split('@').First().Equals(newFileCode.ToString())
                                    && i.LastSegment.Contains(nonNsrFileParts.FactType))
                                .Select(i =>
                                {
                                    var fNameParts = i.LastSegment.Split('_');
                                    return new { i.Item, Filename = $@"{fNameParts[fNameParts.Length - 2]}_{fNameParts[fNameParts.Length - 1].Split('.').First()}.csv" };
                                })
                                .Where(i => expectedFiles.Contains(i.Filename))
                                .AsParallel()
                                .ToList();

                            // remove files we found from the expected files collection
                            expectedFiles = expectedFiles.Except(filesToMove.Select(f => f.Filename), StringComparer.OrdinalIgnoreCase);

                            //Check if all files available in storage
                            if (!expectedFiles.Any())
                            {
                                log.Info($@" All expected files received ");

                                var moveList = filesToMove.Select(f => f.Item);

                                string dateTimePart = DateTime.UtcNow.ToString("yyyyMMdd_hhmmss");
                                var fileMovePath = $@"{nonNsrFileParts.BottlerName}/valid-set-files";

                                //Apply Lock before making entry into database
                                var tableEntryName = $@"{nonNsrFileParts.ContainerName}_{nonNsrFileParts.FullPathToFolderWithinContainer}_{newFileCode}_{nonNsrFileParts.FactType}".Replace('/', '_');   // The value inserting in to the lock table can't contain '/' characters
                                (var lockSuccessful, var lockEntry, var lockErrorResponse) = LockTableEntity.TryLock(tableEntryName, () => req.CreateResponse(HttpStatusCode.NoContent), Helpers.GetLockTable());
                                if (!lockSuccessful) return lockErrorResponse;


                                // NOTE: FactType and BottlerName are OK to use from the payload in to the fxn even though we're looping through a set of files here; those values will be consistent across the entire set we're moving.
                                Helpers.MoveBlobs(blobClient, moveList, fileMovePath, (blobRefName) => $@"{nonNsrFileParts.BottlerName}_{dateTimePart}_{nonNsrFileParts.FactType}_{blobRefName.Split('_').Last()}", log: log);

                                // NSRP-3741: Make database entries for all files as INFA-READY
                                using (var sqlClient = new NsrSqlClient())
                                {
                                    int fileSetId = sqlClient.GetFileSetId();
                                    var sqlFactType = Helpers.GetFactType(nonNsrFileParts.FactType);

                                    string lastDimensionType = string.Empty;
                                    string lastFileName = string.Empty;

                                    // TODO: We hard-code the landing-files subfolder because of the way the database is structured to look up SrcSysIDs; this is not desirable & we should move toward a table that serves this purpose; look it up by bottler & container name
                                    var sourceSysId = sqlClient.GetSourceSysIdForNonNSRFile($@"{nonNsrFileParts.ContainerName}/{nonNsrFileParts.BottlerName}/landing-files");

                                    // Here's a clever way of making sure 'sales' and 'transaction' always occur last in the list; order the list by a custom key where sales & transaction always end up last
                                    var dbUpdateOrdered = moveList
                                        .Select(item =>
                                        {
                                            var dimensionType = item.Uri.Segments.Last().Split('_').Last();
                                            return new { dimensionType, fName = $@"{nonNsrFileParts.BottlerName}_{dateTimePart}_{nonNsrFileParts.FactType}_{dimensionType}" };
                                        })
                                        .OrderBy(i => i.dimensionType.Equals("sales.csv", StringComparison.OrdinalIgnoreCase) || i.dimensionType.Equals("transaction.csv", StringComparison.OrdinalIgnoreCase) ? 1 : 0);

                                    foreach (var fName in dbUpdateOrdered.Select(i => i.fName))
                                    {
                                        var sqlFileType = sqlClient.GetTypeForBottler(sourceSysId, sqlFactType, fName.Split('_').Last().ToLowerInvariant().Replace(".csv", ""));
                                        sqlClient.BottlerFileReceived(sourceSysId, fName, "INFA-READY", 2, fileSetId, sqlFactType, sqlFileType);
                                        log.Info($@"SourceSysId : {sourceSysId} and File : '{fName}'  set to INFA-READY ");
                                    }
                                }

                                log.Info($@" All files moved to valid-set-files ");
                            }
                            else
                            {
                                log.Info($@"Still waiting for files - {string.Join(", ", expectedFiles)} ");
                            }
                        }
                        catch (Exception ex)
                        {
                            log.Info($@"Exception for Check If Entire Set Present In Move All Files to Inbound - {ex.Message}");
                            return req.CreateResponse(HttpStatusCode.NoContent);
                        }
                    }
                    else
                    {


                        // NSRP-3800: splitting excel files into multiple csv files
                        log.Info($@"Checking for excel file in landing-files...");
                        logInfoList.Add($@"Checking for excel file in landing-files...");

                        var excelFileParts = NonNSRBlobfileAttributes.ParseLandingExcelFile(eventGridItemUrl);

                        if (excelFileParts == null)
                        {
                            log.Info($@"Request is not for excel file or folder is other than landing files ");
                            return req.CreateResponse(HttpStatusCode.NoContent);
                        }

                        //get required details for excel file
                        string excelFilesPath = $@"{excelFileParts.ContainerName}/{excelFileParts.FullPathToFolderWithinContainer}";
                        string sourceSysId = new NsrSqlClient().GetSourceSysIdForNonNSRFile(excelFilesPath);
                        NsrFileCnvrDataRow excelRecord = new NsrSqlClient().GetConfigurationRecord(sourceSysId);

                        try
                        {
                            if (excelFileParts != null)
                            {
                                var directory = storageAccount.CreateCloudBlobClient().GetContainerReference(excelFileParts.ContainerName).GetDirectoryReference(excelFileParts.FullPathToFolderWithinContainer);

                                logInfoList.Add("Going to split excel into csv");

                                ConvertExcelToCsv(excelFileParts.ContainerName, excelFileParts.FullPathToFolderWithinContainer, excelFileParts.FullFilename, ref logInfoList);

                                logInfoList.Add("Converting excel into csv is completed");

                                var blobClient = Helpers.GetCloudBlobClient();
                                var blobToMove = directory.GetBlobReference(excelFileParts.FullFilename);
                                var targetFolder = $@"{excelFileParts.BottlerName}/archive";
                                Helpers.MoveBlobs(blobClient, blobToMove, targetFolder, log: log);

                                logInfoList.Add($@"Excel moved to archive - {excelFileParts.FullFilename}");

                                //Helpers.LogMessage(storageAccount.CreateCloudBlobClient(), $@"log_{excelFileParts.FullFilename}", logInfoList, $@"{excelFileParts.ContainerName}/{excelFileParts.BottlerName}", log);
                            }
                            else
                            {
                                //log error in standard error file
                                LogStandardError(storageAccount, excelFileParts, sourceSysId,
                                    $@"Error in parsing regular expression for file {excelFileParts.FullFilename}",
                                    excelRecord, excelFileParts.FullFilename, log);

                                log.Verbose($@"Event Grid msg came in for {eventGridItemUrl}; unhandled by ProcessNonNSRFiles function");
                            }
                        }
                        catch (Exception ex)
                        {
                            log.Info($@"{ex.Message}");

                            //log error in standard error file
                            LogStandardError(storageAccount, excelFileParts, sourceSysId,
                                $@"Error in converting of excel to CSV for file {excelFileParts.FullFilename}",
                                excelRecord, excelFileParts.FullFilename, log);
                        }
                    }
                }
            }
            else
            {
                log.Info($@"Eventgrid api is not as expected ");
            }
            return req.CreateResponse(HttpStatusCode.NoContent);
        }

        /*
        static string ConvertToPipeFormattedString(string line)
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
                
                if (count==charArr.Count())
                {
                    lastchar = true;
                }

                if (isStartDoubleQuoteFound)
                {
                    if (c == '"' )
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
                            sbValue.Replace("\"","#q#").Replace(",","#c#");
                            sbNewLine.Append($@"{sbValue}");
                            sbNewLine.Append($@",");
                        }
                        else
                            isEndDoubleQuoteFound = false;
                    }
                }

                if (isStartDoubleQuoteFound && !isEndCommaAfterDoubleQuoteFound)
                    sbValue.Append(c.ToString());
                else if (isStartDoubleQuoteFound==false && c != '"')
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
        */

        /*static string ConvertToPipeFormattedString(string line)
        {
            StringBuilder sb = new StringBuilder(line.Trim());
            
            if (sb.ToString().StartsWith("\"") && sb.ToString().EndsWith("\"") && sb.Length > 1)
            {
                sb.Remove(0, 1);
                sb.Remove(sb.Length - 1, 1);
                sb.Replace("\",\"", "#pipe#");
                sb.Replace(",", "#c#");
                sb.Replace("\"", "#q#");
                sb.Replace("#pipe#", ",");
            }
            else
            {
                sb.Replace("\"", "#q#");
            }
            return sb.ToString();
        }*/



        private static void LogStandardError(CloudStorageAccount storageAccount, NonNSRBlobfileAttributes landingFileParts,
                        string sourceSysId, string errorMessage, NsrFileCnvrDataRow currentRecord, string fileName, TraceWriter log,
                        string auditStatus = "AF-REJECTED")
        {
            try
            {
                log.Info("Logged error " + auditStatus);
                new NsrSqlClient().InsertForNSRFileReceived(sourceSysId, currentRecord, fileName, auditStatus);
            }
            catch (Exception ex)
            {
                log.Info($@"Error inserting {auditStatus} flag {ex.Message}");
            }
            //Log error message into error file
            var blobClient = storageAccount.CreateCloudBlobClient();
            //Prepare error rows
            StringBuilder sbError = new StringBuilder();
            sbError.AppendLine("Line_Number,Error_Record,Error_Description");
            sbError.AppendLine("NA,NA," + errorMessage);
            string[] ErrorRows = sbError.ToString().Split('\n').ToArray<string>();
            //Pass prepared error to log error method
            LogErrorsToFile(blobClient, ErrorRows.ToList<string>(), sourceSysId, landingFileParts, log);

            var fileMovePath = $@"{landingFileParts.BottlerName}/invalid-data/rejects";
            ArchiveFiles(log, storageAccount, landingFileParts, fileName, fileMovePath);
        }
        private static (NsrFileCnvrDataRow currentRecord, string suffix, HttpResponseMessage errorResponse) GetCurrentRecordAndSuffix(HttpRequestMessage req, string fileName, IEnumerable<NsrFileCnvrDataRow> allRecords, TraceWriter log)
        {
            try
            {
                var r = GetRecordAndSuffix(allRecords, fileName, log);
                if (r != null)
                {
                    log.Info($@"Got the match in configuration table");
                    return (r.Value.Item1, r.Value.Item2, null);
                }
                else
                {
                    log.Info($@"No match found in configuration table for filename {fileName}");
                    return (new NsrFileCnvrDataRow(), string.Empty, null);
                }
            }
            catch (Exception ex)
            {
                log.Info($@"Exception for Set Current Record and Suffix - {ex.Message}");
                return (default(NsrFileCnvrDataRow), null, req.CreateResponse(HttpStatusCode.NoContent));
            }
        }

        private static HttpResponseMessage UpdateDbEntriesForLatinRecords(HttpRequestMessage req, NonNSRBlobfileAttributes fileParts,
            IEnumerable<NsrFileCnvrDataRow> dependentRecords, string sourceSysId, string suffix, CloudTable lockTable, CloudStorageAccount storageAccount, TraceWriter log, ref List<string> logInfoList)
        {
            var tableEntryName = $@"{fileParts.ContainerName}_{fileParts.FullPathToFolderWithinContainer}_{fileParts.Filename}".Replace('/', '_').Replace('~', '_');
            try
            {
                log.Info($@"All files received");
                logInfoList.Add($@"All files received");

                (var lockSuccessful, var lockEntry, var lockErrorResponse) = LockTableEntity.TryLock(tableEntryName, () => req.CreateResponse(HttpStatusCode.NoContent), Helpers.GetLockTable());
                if (!lockSuccessful) return null;

                log.Info($@"Lock applied - {tableEntryName}");
                logInfoList.Add($@"Lock applied - {tableEntryName}");

                using (var sqlClient = new NsrSqlClient())
                {
                    var fileNumId = sqlClient.GetFileNumIdForFileConversion();

                    foreach (var record in dependentRecords)
                    {
                        //get all dependent file details
                        var blobClient = storageAccount.CreateCloudBlobClient();

                        var blob = blobClient
                            .GetContainerReference(fileParts.ContainerName)
                            .GetDirectoryReference(fileParts.FullPathToFolderWithinContainer)
                            .GetBlobReference($@"{record.FileNamePattern.ToLowerInvariant()}{suffix}");

                        blob.FetchAttributes();


                        // the filename pattern was updated by ProcessDepedendentRecordsAndDetermineFurtherRequiredFiles to be the actual filenames associated with each record
                        //sqlClient.InsertForNSRFileReceived(sourceSysId, record, $@"{record.FileNamePattern.ToLowerInvariant()}{suffix}", "INFA-READY", fileNumId);

                        //NSRP-4962
                        //sqlClient.InsertForNSRFileReceivedWithCreateDateTime(sourceSysId, record, $@"{record.FileNamePattern.ToLowerInvariant()}{suffix}", "INFA-READY", blob.Properties.LastModified.Value.DateTime, fileNumId);

                        //NSRP-4962 
                        //Splitted SQL Query - First check if same file name record exist within 5 minutes time duration 
                        //then do not make entry else insert
                        log.Info($@"Check sql insert for file - {record.FileNamePattern.ToLowerInvariant()}{suffix}");
                        logInfoList.Add($@"Check sql insert for file - {record.FileNamePattern.ToLowerInvariant()}{suffix}");
                        DateTime? dtCreateDttm = sqlClient.GetNSRFileCreateDateTime(sourceSysId, record, $@"{record.FileNamePattern.ToLowerInvariant()}{suffix}");
                        bool isReqInsert = true;
                        if (dtCreateDttm != null)
                        {
                            log.Info($@"file - {record.FileNamePattern.ToLowerInvariant()}{suffix} - Last File Create Dttm - {dtCreateDttm} and Blob Create Dttm - {blob.Properties.LastModified.Value.DateTime} ");
                            logInfoList.Add($@"file - {record.FileNamePattern.ToLowerInvariant()}{suffix} - Last File Create Dttm - {dtCreateDttm} and Blob Create Dttm - {blob.Properties.LastModified.Value.DateTime} ");
                            DateTime dtSql = new DateTime(dtCreateDttm.Value.Year, dtCreateDttm.Value.Month, dtCreateDttm.Value.Day, dtCreateDttm.Value.Hour, dtCreateDttm.Value.Minute, dtCreateDttm.Value.Second);
                            DateTime dtBlob = new DateTime(blob.Properties.LastModified.Value.Year, blob.Properties.LastModified.Value.Month, blob.Properties.LastModified.Value.Day, blob.Properties.LastModified.Value.Hour, blob.Properties.LastModified.Value.Minute, blob.Properties.LastModified.Value.Second);
                            System.TimeSpan diffResult = dtBlob.Subtract(dtSql);

                            log.Info($@"file - {record.FileNamePattern.ToLowerInvariant()}{suffix} - Sql Dttm - {dtSql} and Blob Dttm - {dtBlob} and difference in seconds - {diffResult.TotalSeconds} ");
                            logInfoList.Add($@"file - {record.FileNamePattern.ToLowerInvariant()}{suffix} - Sql Dttm - {dtSql} and Blob Dttm - {dtBlob} and difference in seconds - {diffResult.TotalSeconds} ");
                            if (diffResult.TotalSeconds <= 301)
                            {
                                isReqInsert = false;
                                log.Info($@"Database contains the same file entry within last 5 minutes hence insert skipped this time, try again after 5 minutes ");
                                logInfoList.Add($@"Database contains the same file entry within last 5 minutes hence insert skipped this time, try again after 5 minutes ");
                            }
                        }

                        if (isReqInsert)
                        {
                            sqlClient.InsertForLatinFileReceivedWithBlobDateTime(sourceSysId, record, $@"{record.FileNamePattern.ToLowerInvariant()}{suffix}", "INFA-READY", blob.Properties.LastModified.Value.DateTime, fileNumId);

                            log.Info($@"Inserted INFA-READY for: {record.FileNamePattern.ToLowerInvariant()}{suffix} ");
                            logInfoList.Add($@"Inserted INFA-READY for: {record.FileNamePattern.ToLowerInvariant()}{suffix} ");
                        }
                    }
                }


                // update the "lock" record from the bottler filesets table; we're done processing the fileset
                // Delete entry form lock table so same file name can be reprocess
                LockTableEntity.DeleteWithWarning(tableEntryName, lockTable, log: log);

                log.Info("Deleting lock table entry to reprocess same name");
                logInfoList.Add("Deleting lock table entry entry to reprocess same name");
            }
            catch (Exception ex)
            {
                // Delete entry form lock table so same file name can be reprocess
                log.Info("Deleting lock table entry - exception while making entries into db for dependent files");
                logInfoList.Add("Deleting lock table entry - exception while making entries into db for dependent files");
                LockTableEntity.DeleteWithWarning(tableEntryName, lockTable, log: log);

                log.Info($@"Exception for If All Dependent Files Arrived - Make Entries into DB - {ex.Message}");
                logInfoList.Add($@"Exception for If All Dependent Files Arrived - Make Entries into DB - {ex.Message}");

                return req.CreateResponse(HttpStatusCode.NoContent);
            }
            return null;
        }

        private static HttpResponseMessage UpdateDbWorkflowEntriesForDependentRecords(HttpRequestMessage req, NonNSRBlobfileAttributes fileParts,
            IEnumerable<NsrFileCnvrDataRow> dependentRecords, IEnumerable<string> requiredFiles, string sourceSysId, string suffix,
            CloudTable lockTable, CloudStorageAccount storageAccount, TraceWriter log, ref List<string> logInfoList)
        {
            var tableEntryName = $@"{fileParts.ContainerName}_{fileParts.FullPathToFolderWithinContainer}_{suffix}".Replace('/', '_');   // The value inserting in to the lock table can't contain '/' characters
            try
            {
                if (!requiredFiles.Any())
                {
                    log.Info($@"All files received");
                    logInfoList.Add($@"All files received");
                    (var lockSuccessful, var lockEntry, var lockErrorResponse) = LockTableEntity.TryLock(tableEntryName, () => req.CreateResponse(HttpStatusCode.NoContent), Helpers.GetLockTable());
                    if (!lockSuccessful) return null;

                    log.Info($@"Lock applied - {tableEntryName}");
                    logInfoList.Add($@"Lock applied - {tableEntryName}");
                    using (var sqlClient = new NsrSqlClient())
                    {
                        // Fetch FileSetID for entire list

                        var fileNumId = sqlClient.GetFileNumIdForFileConversion();

                        // TODO: Don't we have to worry about file order here?
                        foreach (var record in dependentRecords)
                        {
                            //get all dependent file details
                            var blobClient = storageAccount.CreateCloudBlobClient();

                            var blob = blobClient
                                .GetContainerReference(fileParts.ContainerName)
                                .GetDirectoryReference(fileParts.FullPathToFolderWithinContainer)
                                .GetBlobReference(record.FileNamePattern.Replace("%20", " "));

                            blob.FetchAttributes();

                            //log.Info($@"Insertion started for : { record.FileNamePattern} having creation date {blob.Properties.LastModified.Value.DateTime.ToString()} ");
                            //logInfoList.Add($@"Insertion started for : { record.FileNamePattern} having creation date {blob.Properties.LastModified.Value.DateTime.ToString()} ");

                            //// the filename pattern was updated by ProcessDepedendentRecordsAndDetermineFurtherRequiredFiles to be the actual filenames associated with each record

                            //NSRP-4968                            
                            //sqlClient.InsertForNSRFileReceivedWithCreateDateTime(sourceSysId, record, record.FileNamePattern.Replace("%20", " "), "INFA-READY", blob.Properties.LastModified.Value.DateTime, fileNumId);
                            ////sqlClient.InsertForNSRFileReceived(sourceSysId, record, record.FileNamePattern, "INFA-READY");

                            //NSRP-4968
                            //Splitted SQL Query - First check if same file name record exist within 5 minutes time duration 
                            //then do not make entry else insert
                            log.Info($@"Check sql insert for file - {record.FileNamePattern.Replace("%20", " ")}");
                            logInfoList.Add($@"Check sql insert for file - {record.FileNamePattern.Replace("%20", " ")}");
                            DateTime? dtCreateDttm = sqlClient.GetNSRFileCreateDateTime(sourceSysId, record, $@"{record.FileNamePattern.Replace("%20", " ")}");
                            bool isReqInsert = true;
                            if (dtCreateDttm != null)
                            {
                                log.Info($@"file - {record.FileNamePattern.Replace("%20", " ")} - Last File Create Dttm - {dtCreateDttm} and Blob Create Dttm - {blob.Properties.LastModified.Value.DateTime} ");
                                logInfoList.Add($@"file - {record.FileNamePattern.Replace("%20", " ")} - Last File Create Dttm - {dtCreateDttm} and Blob Create Dttm - {blob.Properties.LastModified.Value.DateTime} ");
                                DateTime dtSql = new DateTime(dtCreateDttm.Value.Year, dtCreateDttm.Value.Month, dtCreateDttm.Value.Day, dtCreateDttm.Value.Hour, dtCreateDttm.Value.Minute, dtCreateDttm.Value.Second);
                                DateTime dtBlob = new DateTime(blob.Properties.LastModified.Value.Year, blob.Properties.LastModified.Value.Month, blob.Properties.LastModified.Value.Day, blob.Properties.LastModified.Value.Hour, blob.Properties.LastModified.Value.Minute, blob.Properties.LastModified.Value.Second);
                                System.TimeSpan diffResult = dtBlob.Subtract(dtSql);

                                log.Info($@"file - {record.FileNamePattern.Replace("%20", " ")} - Sql Dttm - {dtSql} and Blob Dttm - {dtBlob} and difference in seconds - {diffResult.TotalSeconds} ");
                                logInfoList.Add($@"file - {record.FileNamePattern.Replace("%20", " ")} - Sql Dttm - {dtSql} and Blob Dttm - {dtBlob} and difference in seconds - {diffResult.TotalSeconds} ");
                                if (diffResult.TotalSeconds <= 301)
                                {
                                    isReqInsert = false;
                                    log.Info($@"Database contains the same file entry within last 5 minutes hence insert skipped this time, try again after 5 minutes ");
                                    logInfoList.Add($@"Database contains the same file entry within last 5 minutes hence insert skipped this time, try again after 5 minutes ");
                                }
                            }

                            if (isReqInsert)
                            {
                                sqlClient.InsertForLatinFileReceivedWithBlobDateTime(sourceSysId, record, $@"{record.FileNamePattern.Replace("%20", " ")}", "INFA-READY", blob.Properties.LastModified.Value.DateTime, fileNumId);

                                log.Info($@"Inserted INFA-READY for: {record.FileNamePattern.Replace("%20", " ")}");
                                logInfoList.Add($@"Inserted INFA-READY for: {record.FileNamePattern.Replace("%20", " ")}");
                            }
                        }
                    }

                    // update the "lock" record from the bottler filesets table; we're done processing the fileset
                    // Delete entry form lock table so same file name can be reprocess
                    LockTableEntity.DeleteWithWarning(tableEntryName, lockTable, log: log);
                }
            }
            catch (Exception ex)
            {
                // Delete entry form lock table so same file name can be reprocess
                log.Info("Deleting lock table entry - exception while making entries into db for dependent files");
                logInfoList.Add("Deleting lock table entry - exception while making entries into db for dependent files");
                LockTableEntity.DeleteWithWarning(tableEntryName, lockTable, log: log);

                log.Info($@"Exception for If All Dependent Files Arrived - Make Entries into DB - {ex.Message}");
                logInfoList.Add($@"Exception for If All Dependent Files Arrived - Make Entries into DB - {ex.Message}");
                return req.CreateResponse(HttpStatusCode.NoContent);
            }
            return null;
        }
        //Processing dependent Records for Non South Latin & File name Without Underscore
        private static void ProcessDepedendentRecordsAndDetermineFurtherRequiredFilesForSouthPacific(ref List<NsrFileCnvrDataRow> dependentRecords, NonNSRBlobfileAttributes currentFileParts, ref IList<string> requiredFiles, TraceWriter log, ref List<string> logInfoList)
        {
            var lastSegmentOfUriForBlobsToProcessFromBottlerLandingFolder = Helpers.GetCloudBlobClient()
                                                    .GetContainerReference(currentFileParts.ContainerName)
                                                    .GetDirectoryReference(currentFileParts.FullPathToFolderWithinContainer)
                                                    .ListBlobs()
                                                    .Select(item => item.Uri.Segments.Last())
                                                    .ToList();  // to keep from re-executing query

            foreach (var lastSegmentOfBlob in lastSegmentOfUriForBlobsToProcessFromBottlerLandingFolder)
            {
                var removeName = requiredFiles.SingleOrDefault(fName => lastSegmentOfBlob.Replace("%20", " ").Equals($@"{fName}", StringComparison.OrdinalIgnoreCase));
                logInfoList.Add($@"Remove Name - {removeName} and blob name {lastSegmentOfBlob} and After Replace {lastSegmentOfBlob.Replace("%20", " ")}");
                string temp = "";

                if (removeName != null)
                {   // Update our dependent records with actual filenames from the blobs in storage; set these back in to our dependentRecords collection (passed by reference)
                    dependentRecords = dependentRecords.Select(m =>
                    {
                        if (m.FileNamePattern.Equals(removeName, StringComparison.OrdinalIgnoreCase))
                        {
                            // TODO: Why are we changing the content of a record in the dependent records collection?
                            log.Info($@"Dependent  file pattern {m.FileNamePattern} and changed to {lastSegmentOfBlob.Replace("%20", " ")}");
                            temp = $@"Dependent  file pattern {m.FileNamePattern} and changed to {lastSegmentOfBlob.Replace("%20", " ")}";
                            m.FileNamePattern = lastSegmentOfBlob.Replace("%20", " ");
                        }

                        return m;
                    }).ToList();
                    logInfoList.Add(temp);
                    requiredFiles.Remove(removeName.ToLowerInvariant());

                    log.Info($@"Latin Pattern Removed - {lastSegmentOfBlob} and required files count {requiredFiles.Count}");
                    logInfoList.Add($@"Latin Pattern Removed - {lastSegmentOfBlob} and required files count {requiredFiles.Count}");
                }
            }
        }

        private static void ProcessDepedendentRecordsAndDetermineFurtherRequiredFiles(ref List<NsrFileCnvrDataRow> dependentRecords, string suffix, bool isLatinFileSet, NonNSRBlobfileAttributes currentFileParts, ref IList<string> requiredFiles, TraceWriter log, ref List<string> logInfoList)
        {
            var lastSegmentOfUriForBlobsToProcessFromBottlerLandingFolder = Helpers.GetCloudBlobClient()
                                                    .GetContainerReference(currentFileParts.ContainerName)
                                                    .GetDirectoryReference(currentFileParts.FullPathToFolderWithinContainer)
                                                    .ListBlobs()
                                                    .Select(item => item.Uri.Segments.Last())
                                                    .Where(lastSegment => lastSegment.Contains('_') || lastSegment.Contains("~conv~"))
                                                    .ToList();  // to keep from re-executing query

            foreach (var lastSegmentOfBlob in lastSegmentOfUriForBlobsToProcessFromBottlerLandingFolder)
            {
                //logInfoList.Add($@"Check Blob Name in Required File List - {lastSegmentOfBlob}");
                //log.Info($@"lastsegmentofBlob '{lastSegmentOfBlob.Replace("%20", " ")}' and filename '{suffix}{requiredFiles.First()}'");
                string temp = "";
                var removeName = requiredFiles.SingleOrDefault(fName => isLatinFileSet
                    ? lastSegmentOfBlob.Equals($@"{fName}{suffix}", StringComparison.OrdinalIgnoreCase)
                    : lastSegmentOfBlob.Replace("%20", " ").Equals($@"{suffix}{fName}", StringComparison.OrdinalIgnoreCase));

                //logInfoList.Add($@"Remove Name - {removeName} and suffix - {suffix} ");

                if (removeName != null)
                {   // Update our dependent records with actual filenames from the blobs in storage; set these back in to our dependentRecords collection (passed by reference)
                    dependentRecords = dependentRecords.Select(m =>
                    {
                        if (isLatinFileSet
                            ? m.FileNamePattern.Equals(removeName, StringComparison.OrdinalIgnoreCase)
                            : m.FileNamePattern.Contains(removeName, StringComparison.OrdinalIgnoreCase))
                        {
                            // TODO: Why are we changing the content of a record in the dependent records collection?
                            log.Info($@"Dependent  file pattern {m.FileNamePattern} and changed to {lastSegmentOfBlob.Replace("%20", " ")}");
                            temp = $@"Dependent  file pattern {m.FileNamePattern} and changed to {lastSegmentOfBlob.Replace("%20", " ")}";
                            m.FileNamePattern = lastSegmentOfBlob.Replace("%20", " ");
                        }

                        return m;
                    }).ToList();
                    logInfoList.Add(temp);
                    requiredFiles.Remove(removeName.ToLowerInvariant());
                    log.Info($@"Latin Pattern Removed - {removeName}");
                    logInfoList.Add($@"Latin Pattern Removed - {removeName}");
                }
            }

            log.Info($@"Process Dependent Records Completed");

        }

        private static (IList<string>, bool islatinfileset, string newSuffix) GetRequiredFilesAndLatinFilesetIndicator(IEnumerable<NsrFileCnvrDataRow> dependentRecords, bool currentFileIsNonLatin, string fileName, TraceWriter log, ref List<string> logInfoList)
        {
            if (currentFileIsNonLatin)
            {
                bool isLatinFileset = false;
                var requiredFiles = new List<string>();
                int minCount = -1;
                int flag = 0;

                foreach (var record in dependentRecords)
                {
                    var splitVals = record.FileNamePattern.Split('_');

                    if ((minCount == -1) || (minCount > (splitVals.Length - 1)))
                    {
                        minCount = splitVals.Length - 1;
                        flag++;
                    }
                }

                log.Info($@"MinCount - {minCount}");
                logInfoList.Add($@"MinCount - {minCount} and flag {flag}");

                StringBuilder sbNewSuffix = new StringBuilder();
                foreach (var record in dependentRecords)
                {
                    var filenamePatternParts = record.FileNamePattern.Split('_');

                    var filePartsForSuffix = fileName.Split('_');
                    StringBuilder sbNewFileNamePattern = new StringBuilder();
                    if (flag > 1)
                    {
                        for (int i = minCount; i < filenamePatternParts.Length; i++)
                        {
                            if (string.IsNullOrEmpty(sbNewFileNamePattern.ToString()))
                                sbNewFileNamePattern.Append(filenamePatternParts[i]);
                            else
                                sbNewFileNamePattern.Append($@"_{filenamePatternParts[i]}");
                        }

                        if (string.IsNullOrEmpty(sbNewSuffix.ToString()))
                        {
                            for (int j = 0; j < minCount; j++)
                            {
                                sbNewSuffix.Append($@"{filePartsForSuffix[j]}_");
                            }
                        }

                        log.Info($@"New File Name Pattern - {sbNewFileNamePattern.ToString().ToLowerInvariant()} and new suffix - {sbNewSuffix.ToString()}");
                        logInfoList.Add($@"New File Name Pattern - {sbNewFileNamePattern.ToString().ToLowerInvariant()} and new suffix - {sbNewSuffix.ToString()}");

                        requiredFiles.Add(sbNewFileNamePattern.ToString().ToLowerInvariant());
                    }
                    else
                    {
                        log.Info($@"No change in suffix and file name pattern is {filenamePatternParts.Last().ToLowerInvariant()}");
                        logInfoList.Add($@"No change in suffix and file name pattern is {filenamePatternParts.Last().ToLowerInvariant()}");

                        requiredFiles.Add(filenamePatternParts.Last().ToLowerInvariant());
                    }

                    // we are working with latin files if any dependent records lack the '_' filename separator. This means the Split() above would have done nothing but return the whole string value
                    // once we set the value to 'true', can't unset it
                    isLatinFileset |= (filenamePatternParts.Length == 1);
                }

                return (requiredFiles, isLatinFileset, sbNewSuffix.ToString());
            }
            // if the current file IS latin, we can skip the loop and just do a LINQ query
            else
            {
                return (dependentRecords.Select(record => record.FileNamePattern.Split('_').Last().ToLowerInvariant()).AsParallel().ToList(), true, "");
            }

        }
        //Get Required files for Non South Latin with out Underscore and Datetime
        private static (IList<string>, bool islatinfileset) GetRequiredFilesForNonSouthFilesetWithOutDT(IEnumerable<NsrFileCnvrDataRow> dependentRecords, bool currentFileIsNonLatin)
        {

            // if the current file IS latin, we can skip the loop and just do a LINQ query

            return (dependentRecords.Select(record => record.FileNamePattern.ToLowerInvariant()).AsParallel().ToList(), true);


        }

        private static IEnumerable<string> GetExpectedFiles(NonNSRBlobfileAttributes nonNsrFileParts, TraceWriter log)
        {
            using (var sqlClient = new NsrSqlClient())
            {
                // TODO: We hard-code the landing-files subfolder because of the way the database is structured to look up SrcSysIDs; this is not desirable & we should move toward a table that serves this purpose; look it up by bottler & container name
                var dbLookupValue = $@"{nonNsrFileParts.ContainerName}/{nonNsrFileParts.BottlerName}/landing-files/";
                var sourceSysId = sqlClient.GetSourceSysIdForNonNSRFile(dbLookupValue);
                var expectedFiles = sqlClient.GetExpectedFilesForTarget(sourceSysId, nonNsrFileParts.FactType).ToList();

                log.Info($@" Files expected are {string.Join(", ", expectedFiles)}");
                return expectedFiles;
            }
        }

        private static int DetermineFileCode(NonNSRBlobfileAttributes nonNsrFileParts, IEnumerable<IListBlobItem> allfiles, TraceWriter log)
        {
            log.Verbose(@"Determining file code...");
            //NSRD-14541
            var fileType = $@"{nonNsrFileParts.FileType.ToLowerInvariant()}.csv";   //volume_sales.csv - volume_salestype.csv

            var highestFileCodeForFilesWithMatchingFiletype = allfiles
                .Select(fName => fName.Uri.Segments.Last())
                .Where(lastSegment => lastSegment.ToLowerInvariant().EndsWith(fileType) && lastSegment.Contains('~'))
                .AsParallel()
                // Use nullable int here so if Where() clause finds no matches, can still call Max and no throw exception; rather get 'null' back
                .Max(lastSegment => new int?(int.Parse(lastSegment.Split('~').First())));

            if (!highestFileCodeForFilesWithMatchingFiletype.HasValue)
            {
                highestFileCodeForFilesWithMatchingFiletype = allfiles
                    .Select(fName => fName.Uri.Segments.Last())
                    .Where(lastSegment => lastSegment.Contains(nonNsrFileParts.FactType, StringComparison.OrdinalIgnoreCase) && lastSegment.Contains('~'))
                    .AsParallel()
                    // Use nullable int here so if Where() clause finds no matches, can still call Max and no throw exception; rather get 'null' back
                    .Min(lastSegment => new int?(int.Parse(lastSegment.Split('~').First())));


                log.Info($@"Same type not present - new file code is {highestFileCodeForFilesWithMatchingFiletype} ");
            }
            else
            {
                highestFileCodeForFilesWithMatchingFiletype = highestFileCodeForFilesWithMatchingFiletype + 1;
                log.Info($@"Same type file is already present - new file code is {highestFileCodeForFilesWithMatchingFiletype} ");
            }

            if (!highestFileCodeForFilesWithMatchingFiletype.HasValue)
            {
                log.Info($@"Existing File Code is 0 ");
            }

            int newFileCode = highestFileCodeForFilesWithMatchingFiletype.GetValueOrDefault(1);
            return newFileCode;
        }
        private static IEnumerable<string> ExtractFileContentAsDoubleQuotedCsvLines(CloudBlob sourceBlob, TraceWriter log, bool shouldConvertToPipeFormatString,
            bool skipEmptyLines = true)
        {
            //detect encoding of input blob contents
            var blobEncoding = CustomEncodingCheck.DetectStreamFileEncoding(sourceBlob);

            log.Info($@"Detected encoding {blobEncoding.HeaderName}");
            //read blob with detected blob encoding
            using (var blobReader = new StreamReader(sourceBlob.OpenRead(), blobEncoding, true))
            {
                while (!blobReader.EndOfStream)
                {
                    //read each line of blob
                    var line = blobReader.ReadLine();

                    if (shouldConvertToPipeFormatString)
                        line = Helpers.ReplaceDoubleQuoteAndCommaInValues(line);

                    if (!skipEmptyLines || !string.IsNullOrWhiteSpace(line))
                    {
                        yield return line;
                    }
                }
            }

        }
        private static IEnumerable<string> GetFileContentAsDoubleQuotedCsvLines(CloudBlob sourceBlob, TraceWriter log, bool skipEmptyLines = true)
        {
            // TODO: This effectively makes an copy of the entire file in memory and carries it around - is this OK??
            //const string doubleQuote = "\"";
            //var blobEncoding = FileEncoding.DetectFileEncoding(sourceBlob.OpenRead(), Encoding.UTF8);

            log.Info("Checking File Encoding...");
            var blobEncoding = CustomEncodingCheck.DetectStreamFileEncoding(sourceBlob);
            //check if file has chiniese character
            if (blobEncoding.BodyName == "iso-8859-1")
            {
                using (var blobReader = new StreamReader(sourceBlob.OpenRead()))
                {
                    while (!blobReader.EndOfStream)
                    {
                        var line = blobReader.ReadLine();
                        if (!skipEmptyLines || !string.IsNullOrWhiteSpace(line))
                        {
                            // kill any double quotes that might be in the line so we can add them throughout without duplicating
                            //line = line.Replace(doubleQuote, string.Empty);
                            // re-create the line by adding double quotes where we now only have commas, and putting double quotes on the ends as well
                            //line = $@"{doubleQuote}{line.Replace(",", "\",\"")}{doubleQuote}";

                            yield return line;
                        }
                    }
                }
            }
            else
            {
                using (var blobReader = new StreamReader(sourceBlob.OpenRead(), blobEncoding, true))
                {
                    while (!blobReader.EndOfStream)
                    {
                        var line = ConvertBlobLineToUTF8(blobEncoding, blobReader.ReadLine());    // The ReadLine needs to happen before encoding is fetched or encoding isn't accurate
                        if (!skipEmptyLines || !string.IsNullOrWhiteSpace(line))
                        {
                            // kill any double quotes that might be in the line so we can add them throughout without duplicating
                            //line = line.Replace(doubleQuote, string.Empty);
                            // re-create the line by adding double quotes where we now only have commas, and putting double quotes on the ends as well
                            //line = $@"{doubleQuote}{line.Replace(",", "\",\"")}{doubleQuote}";

                            yield return line;
                        }
                    }
                }
            }
        }


        private static (NsrFileCnvrDataRow, string)? GetRecordAndSuffix(IEnumerable<NsrFileCnvrDataRow> allRecords, string fileName, TraceWriter log)
        {
            if (string.IsNullOrWhiteSpace(fileName)) throw new ArgumentNullException(nameof(fileName));

            foreach (var record in allRecords)
            {
                if (!fileName.Contains('_'))
                {
                    var subFileName = "";
                    if (fileName.Equals(record.FileNamePattern, StringComparison.OrdinalIgnoreCase))
                    {
                        subFileName = fileName;
                    }
                    else if (fileName.Length >= record.FileNamePattern.Length)
                    {
                        subFileName = fileName.Substring(0, record.FileNamePattern.Length);
                    }

                    // var subFileName = fileName.Substring(0, record.FileNamePattern.Length);
                    log.Info($@"Prefix for FileName {subFileName} and db record fileName {record.FileNamePattern}");
                    if (subFileName.Equals(record.FileNamePattern, StringComparison.OrdinalIgnoreCase))
                    {
                        log.Info($@"CnfgSrcSysInfaId-{record.CnfgSrcSysInfaId} MaxFileSetSubId-{record.MaxFileSetSubId} FileNamePtrn-{record.FileNamePattern}");
                        return (record, fileName.Substring(record.FileNamePattern.Length));
                    }
                }
                else
                {
                    var lastSplitValue = fileName.Split('_').Last();

                    if (record.FileNamePattern.Split('_').Last().Equals(lastSplitValue, StringComparison.OrdinalIgnoreCase))
                    {
                        log.Info($@"CnfgSrcSysInfaId-{record.CnfgSrcSysInfaId} MaxFileSetSubId-{record.MaxFileSetSubId} FileNamePtrn-{record.FileNamePattern}");
                        return (record, fileName.Replace(lastSplitValue, ""));
                    }
                }
            }

            return null;
        }

        private static void CopySourceFileToArchive(TraceWriter log, CloudStorageAccount storageAccount, NonNSRBlobfileAttributes blobParts, string fileName, string fileNamePrefix, string fileMovePath, ref List<string> logInforList)
        {
            try
            {
                var bClient = storageAccount.CreateCloudBlobClient();

                var archiveBlobRef = bClient.GetContainerReference(blobParts.ContainerName)
                    .GetDirectoryReference(blobParts.FullPathToFolderWithinContainer)
                    .GetBlobReference(fileName);

                Helpers.CopyBlobs(bClient, archiveBlobRef, fileNamePrefix, fileMovePath, log: log);

                log.Info($@"File {archiveBlobRef.Uri.Segments.Last()} Copied...");
                logInforList.Add($@"File {archiveBlobRef.Uri.Segments.Last()} Copied...");

                log.Info($@"Archive Path - {fileMovePath} ");
                logInforList.Add($@"Archive Path - {fileMovePath} ");
            }
            catch (Exception ex)
            {
                log.Info($@"Exception while Archiving Blob - {ex.Message.ToString()}");
                logInforList.Add($@"Exception while Archiving Blob - {ex.Message.ToString()}");
            }
        }

        private static void ArchiveFiles(TraceWriter log, CloudStorageAccount storageAccount, NonNSRBlobfileAttributes blobParts, string fileName, string fileMovePath)
        {
            try
            {
                var bClient = storageAccount.CreateCloudBlobClient();

                var archiveBlobRef = bClient.GetContainerReference(blobParts.ContainerName)
                    .GetDirectoryReference(blobParts.FullPathToFolderWithinContainer)
                    .GetBlobReference(fileName);

                Helpers.MoveBlobs(bClient, archiveBlobRef, fileMovePath, log: log);
                log.Info($@"File {archiveBlobRef.Uri.Segments.Last()} archived...");
                log.Info($@"Archive Path - {fileMovePath} ");
            }
            catch (Exception ex)
            {
                log.Info($@"Exception while Archiving Blob - {ex.Message.ToString()}");
            }
        }

        private static bool IsTimeFile(string fileName)
        {
            return fileName.Contains("time.csv");
        }

        private static (int, List<string>) ProcessNonNSRLatinFFFiles(CloudStorageAccount storageAccount, string fileNamePattern, string sourceSysId, IEnumerable<NsrFileCnvrDataRow> allRecords, NonNSRBlobfileAttributes landingFileParts, TraceWriter log, ref List<string> logInfoList)
        {
            var file = landingFileParts.FullFilename;
            //NSRD-13888 - create 0KB files only when CONT source file received
            if (file.StartsWith("CONT", StringComparison.OrdinalIgnoreCase) && !file.Contains("~conv~", StringComparison.OrdinalIgnoreCase))
            {
                //check all files are available if not generate missing files
                var fileSuffix = file.Substring(4);
                var expectedFiles = allRecords.Select(r => r.FileNamePattern);

                var directory = storageAccount.CreateCloudBlobClient().GetContainerReference(landingFileParts.ContainerName).GetDirectoryReference(landingFileParts.FullPathToFolderWithinContainer);

                var allBlobs = directory.ListBlobs();
                var missingFiles = allRecords.Where(record => !allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"{record.FileNamePattern}{fileSuffix}", StringComparison.OrdinalIgnoreCase))).AsParallel();

                log.Info($@"Step 8: FileName Validation Completed for file : {landingFileParts.Filename}...");
                logInfoList.Add($@"Step 8: FileName Validation Completed for file : {landingFileParts.Filename}...");

                logInfoList.Add($@" file suffix - {fileSuffix} Container Name - {landingFileParts.ContainerName} and Full Path - {landingFileParts.FullPathToFolderWithinContainer}");

                StringBuilder sb = new StringBuilder();


                Parallel.ForEach(missingFiles, missingFile =>
                {
                    var filewithSuffix = string.Concat(missingFile.FileNamePattern, fileSuffix);
                    var newFileName = CreateFileNameForConvertedFile(filewithSuffix);

                    //Check either converted file is present
                    var isPresent = allBlobs.Any(blob => blob.Uri.Segments.Last().Equals($@"{newFileName}", StringComparison.OrdinalIgnoreCase));
                    sb.AppendLine($@"Converted File {newFileName} Present? - {isPresent}");

                    if (!isPresent)
                    {
                        var blockBlob = directory.GetBlockBlobReference(newFileName);
                        blockBlob.Properties.ContentType = "text/csv; charset=utf-8";

                        log.Info($@"fileName: {filewithSuffix}");
                        sb.AppendLine($@"fileName: {filewithSuffix} and missing file pattern - {missingFile.FileNamePattern}");

                        // Create a 0-byte file w/ the required name
                        blockBlob.UploadFromByteArray(new byte[0], 0, 0);

                        log.Info($@"filename: {filewithSuffix} file created successfully...");
                        sb.AppendLine($@"filename: {filewithSuffix} file created successfully...");
                    }
                });

                logInfoList.Add(sb.ToString());

                log.Info($@"File conversion completed - suffix - {fileSuffix}");
                logInfoList.Add($@"File conversion completed - suffix - {fileSuffix}");

                return (1, null);
            }
            else if (!file.Contains("~conv~", StringComparison.OrdinalIgnoreCase) && !file.Contains('_') && !file.StartsWith("CONT", StringComparison.OrdinalIgnoreCase))
            {
                var latinConfigFileRows = new NsrSqlClient()
                    .GetPositionalElementsForLatinFileConversion(Int32.Parse(sourceSysId), fileNamePattern);

                if (latinConfigFileRows.Any())
                {
                    var blobClient = storageAccount.CreateCloudBlobClient();

                    var blob = blobClient.GetContainerReference(landingFileParts.ContainerName)
                        .GetDirectoryReference($@"{landingFileParts.BottlerName}/landing-files")
                        .GetBlobReference(file);

                    IEnumerable<string> pipeDelimitedFile;
                    List<string> rejectedErrorLines;
                    //receiving pipe delimited result rows and error rows
                    log.Info("receiving pipe delimited result rows and error rows");
                    logInfoList.Add("receiving pipe delimited result rows and error rows");

                    (pipeDelimitedFile, rejectedErrorLines) = GetPipeDelimitedLatinFileContents(blob, latinConfigFileRows, landingFileParts, sourceSysId, log, ref logInfoList);

                    log.Info("logging information of pipe delimited files");
                    logInfoList.Add("logging information of pipe delimited files");

                    //log.Info($@"Available blob rows: {GetBlobRows(blob)}");
                    //logInfoList.Add($@"Available blob rows: {GetBlobRows(blob)}");

                    log.Info($@"Pipe delimited file count: {pipeDelimitedFile.Count<string>()}");
                    logInfoList.Add($@"Pipe delimited file count: {pipeDelimitedFile.Count()}");

                    var directoryPath = $@"{landingFileParts.BottlerName}/landing-files";

                    Helpers.UploadBlobAsPutBlockList(blobClient, landingFileParts.ContainerName, directoryPath, CreateFileNameForConvertedFile(file),
                        pipeDelimitedFile, @"text/csv; charset=utf-8", false);

                    //Check if any row processing error occured                    
                    //if (pipeDelimitedFile.Count<string>() == GetBlobRows(blob))
                    if (rejectedErrorLines.Count == 1)
                    {
                        log.Info($@"File conversion completed - suffix - {file.Substring(fileNamePattern.Length)}");
                        logInfoList.Add($@"File conversion completed - suffix - {file.Substring(fileNamePattern.Length)}");

                        return (1, null); //Value 1 indicates all row processed
                    }
                    else
                    {
                        log.Info(@"File conversion completed partially.");
                        logInfoList.Add(@"File conversion completed partially.");

                        return (2, rejectedErrorLines); // Value 2 indicates row processing encounter error in rows
                    }

                }
                log.Info(@"Configuration for fixed width to pipe formatted file is not found.");
                logInfoList.Add(@"Configuration for fixed width to pipe formatted file is not found.");
            }

            return (0, null); // Value 0 indicates - file conversion not required as file is NonLatin File or already converted file
        }

        private static int GetBlobRows(CloudBlob blob)
        {
            int rownum = 0;
            using (var blobStream = new StreamReader(blob.OpenRead()))
            {
                while (!blobStream.EndOfStream)
                {
                    var line = blobStream.ReadLine();
                    //Do not count blank rows of input blob file
                    if (!string.IsNullOrWhiteSpace(line))
                        rownum++;
                }
            }
            //Return total number of rows in blob
            return rownum;
        }


        private static string CreateFileNameForConvertedFile(string originalFileName)
        {
            return originalFileName.ToLowerInvariant().Replace(".csv", "~conv~.csv").Replace(".txt", "~conv~.txt");
        }

        private static (IEnumerable<string>, List<string>) GetPipeDelimitedLatinFileContents(CloudBlob blob, IEnumerable<(int InitialCharacterPosition, int DataLength)> latinConfigFileRows, NonNSRBlobfileAttributes landingFileParts, string sourceSysId, TraceWriter log, ref List<string> logInfoList)
        {
            var latinConfigFileRowsList = latinConfigFileRows.ToList();
            var numberOfRows = latinConfigFileRowsList.Count;
            //var blobTotalRows = GetBlobRows(blob);

            //receive pipe delimited lines
            //var pipeDelimitedLine = new StringBuilder();

            //receive error lines
            string curErrorLines = string.Empty;
            //var errorLines = new StringBuilder();
            var errorRows = new List<string>();

            //adding header to error lines
            //errorLines.AppendLine("Line_Number,Error_Record,Error_Description");
            errorRows.Add("Line_Number,Error_Record,Error_Description");


            log.Info("Checking File Encoding...");
            logInfoList.Add("Checking File Encoding...");
            //var blobEncoding = FileEncoding.DetectFileEncoding(blob.OpenRead(), Encoding.UTF8);             //Not working for ANSI encoding properly

            var blobEncoding = CustomEncodingCheck.DetectStreamFileEncoding(blob);

            log.Info($@"Checking File Encoding {blobEncoding.ToString()} and {blobEncoding.EncodingName} and {blobEncoding.HeaderName} and {blobEncoding.WebName}...");
            logInfoList.Add($@"Checking File Encoding {blobEncoding.ToString()} and {blobEncoding.EncodingName} and {blobEncoding.HeaderName} and {blobEncoding.WebName}...");

            int currow = 1;
            //bool isAppendOnly = false;
            string lastCorrectLine = string.Empty;

            //get total character required in current file for each rows
            var (finalCharPos, dataColumnLength) = latinConfigFileRowsList[numberOfRows - 1];

            log.Info($@"finalCharPos {finalCharPos} and dataColumnLength {dataColumnLength}");
            logInfoList.Add($@"finalCharPos {finalCharPos} and dataColumnLength {dataColumnLength}");

            List<string> pipeDelimitedRows = new List<string>();            //New Code as array giving error for large filesize

            //log.Info("Checking File Encoding...");
            //var blobEncoding = CustomEncodingCheck.DetectStreamFileEncoding(blob);            
            using (var blobStream = new StreamReader(blob.OpenRead(), blobEncoding, true))
            {
                while (!blobStream.EndOfStream)
                {
                    var line = blobStream.ReadLine();
                    //var line = ConvertBlobLineToUTF8(blobEncoding, blobStream.ReadLine());    // The ReadLine needs to happen before encoding is fetched or encoding isn't accurate

                    // NSRP-3970: Don't add blank lines while converting to pipe formatted file 
                    if (!string.IsNullOrWhiteSpace(line))
                    {

                        var newLine = new StringBuilder();
                        try
                        {
                            //check if current line has required number of character available

                            if (line.Length == (finalCharPos + dataColumnLength) - 1)
                            {
                                for (int rowNum = 0; rowNum < numberOfRows; rowNum++)
                                {
                                    var isLastRow = (rowNum == (numberOfRows - 1));
                                    var (initialCharacterPosition, dataLength) = latinConfigFileRowsList[rowNum];
                                    int length = 0;

                                    length = isLastRow ? line.Length - (initialCharacterPosition - 1) : dataLength;

                                    if (isLastRow && (length < dataLength))
                                    {
                                        newLine
                                            .Append(line.Substring(initialCharacterPosition - 1, length - 1))
                                            .Append("|.|");
                                    }
                                    else
                                    {
                                        newLine
                                            .Append(line.Substring(initialCharacterPosition - 1, length))
                                            .Append("|");
                                    }
                                }

                                //add processed line to pipe delimited line
                                if (!string.IsNullOrEmpty(lastCorrectLine))
                                {
                                    //pipeDelimitedLine.AppendLine(lastCorrectLine);
                                    pipeDelimitedRows.Add(lastCorrectLine);             //New Code as array giving error for large filesize
                                }

                                lastCorrectLine = newLine.ToString().Substring(0, (newLine.Length - 1));


                            }
                            else
                            {
                                //log.Info($@"blob: {blob.Name} has occured processing error in {currow}.");
                                curErrorLines = currow.ToString() + "," + "\"" + line + "\"" + "," + "\"" +
                                    $@"Line does not have the {(finalCharPos + dataColumnLength) - 1} required number of character available" + "\"";
                                //errorLines.AppendLine(curErrorLines);
                                errorRows.Add(curErrorLines);

                                //logInfoList.Add($@"Error line - {curErrorLines}");
                            }
                        }
                        catch (Exception ex)
                        {
                            log.Info($@"error processing line: {currow} with exception: {ex.Message}");
                            logInfoList.Add($@"error processing line: {currow} with exception: {ex.Message}");
                        }
                    }
                    currow++;
                }
            }

            log.Info($@"processing completed ");
            logInfoList.Add($@"processing completed ");

            if (!string.IsNullOrEmpty(lastCorrectLine))
            {
                //pipeDelimitedLine.Append(lastCorrectLine);
                pipeDelimitedRows.Add(lastCorrectLine);         //New Code as array giving error for large filesize
            }

            /*
            
            //convert all successfully processed rows to ienumerable string
            string[] pipeRows = pipeDelimitedLine.ToString().Split('\n').ToArray<string>();

            IEnumerable<string> pipeDelimitedRows = pipeRows.Cast<string>();
            */
            /*
            //logInfoList.Add($@"pipeDelimitedLine - {pipeDelimitedLine.Length} and temp count {pipeDelimitedRows.Count<String>()}");
            logInfoList.Add($@"row count {pipeDelimitedRows.Count}");

            log.Info("log error rows to error file");
            logInfoList.Add("log error rows to error file");
            //collect rejected rows to ienumerable string

            string[] errorRows = errorLines.ToString().Split('\n').ToArray<string>();
                        
            logInfoList.Add($@"errorRows - {errorRows.Count()}");
            */
            //return pipe delimited successful processed and rejected rows
            log.Info($@"Converted Row Count {pipeDelimitedRows.Count} and Error Row Count - {errorRows.Count}");
            logInfoList.Add($@"Converted Row Count {pipeDelimitedRows.Count} and Error Row Count - {errorRows.Count}");
            return (pipeDelimitedRows, errorRows);
        }

        private static void LogErrorsToFile(CloudBlobClient blobClient, List<string> errors, string sourceSysId,
            NonNSRBlobfileAttributes blobParts, TraceWriter log, NsrSqlClient sqlClient = null, bool hasNewLine = true)
        {
            log.Info($@"calling method to receive row id");
            NsrSqlClient nsrfile = new NsrSqlClient();

            var filename = blobParts.Filename;
            //add extension when file name has blank extension
            if (!filename.Contains("."))
                filename = filename + ".txt";

            var idRow = 0;
            if (sourceSysId == string.Empty || sourceSysId == null)
                idRow = 0;
            else
                idRow = nsrfile.CheckRowId(sourceSysId, blobParts.FullFilename);

            log.Info($@"received Id row is : {idRow}");

            var errorFileName = $@"{idRow}_Error_{filename}";
            log.Info($@"error file name : {errorFileName}");
            Helpers.CreateErrorFile(blobClient, blobParts.ContainerName, blobParts.BottlerName, errorFileName,
                errors, log, hasNewLine);
        }


        /// <summary>
        /// Converts a line read from a Blob to UTF-8 format.
        /// </summary>
        /// <param name="blobStream">The BLOB stream. NOTE: At least one byte must have been read from the blob stream before calling this method or the stream's CurrentEncoding property hasn't been populated</param>
        /// <param name="line">The line.</param>
        /// <returns>A UTF-8 encoded version of the line from the blob</returns>
        private static string ConvertBlobLineToUTF8(StreamReader blobStream, string line) => ConvertBlobLineToUTF8(FileEncoding.DetectFileEncoding(blobStream.BaseStream), line);
        private static string ConvertBlobLineToUTF8(Encoding lineEncoding, string line)
        {
            if (lineEncoding != Encoding.UTF8)
            {
                var lineBytes = lineEncoding.GetBytes(line);
                var utf8Bytes = Encoding.Convert(lineEncoding, Encoding.UTF8, lineBytes);
                line = Encoding.UTF8.GetString(utf8Bytes);
            }

            return line;
        }

        /// <summary>
        /// Converts an Excel file to one or more CSV files based on the tables in each data set of the file
        /// </summary>
        /// <param name="containerName">Name of the container.</param>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="excelFileName">Name of the excel file.</param>
        private static void ConvertExcelToCsv(string containerName, string directoryPath, string excelFileName, ref List<string> logInfoList)
        {
            var blobClient = Helpers.GetCloudBlobClient();
            CloudBlockBlob blockBlobReference = blobClient
                .GetContainerReference(containerName)
                .GetDirectoryReference(directoryPath)
                .GetBlockBlobReference(excelFileName);

            // NSRP-3800: Fetching date from excel file name 
            // NSRD-14637: This is filename format with out datetime for South Pacific Bottler.
            string datePart = DateTime.UtcNow.ToString("yyyyMMdd_hhmmss");

            logInfoList.Add($@"Datepart for this batch is {datePart}");

            if (excelFileName.Contains('_'))
                datePart = excelFileName.Split('_').First();

            DataSet ds;
            using (var excelReader = ExcelReaderFactory.CreateOpenXmlReader(blockBlobReference.OpenRead()))
            {
                ds = excelReader.AsDataSet();
            }
            List<string> temp = new List<string>();
            // We can process the file in parallel & do simultaneous uploads - fire away!
            Parallel.ForEach(ds.Tables.OfType<DataTable>(), table =>
            {
                //string doubleQuote = "\"";
                List<string> fileContent = new List<string>();

                foreach (DataRow dr in table.Rows)
                {
                    var val = "";
                    int colCount = dr.ItemArray.Length;

                    for (int i = 0; i < colCount; i++)
                    {
                        //val = $@"{val},{doubleQuote}{dr[i].ToString()}{doubleQuote}";
                        val = $@"{val},{dr[i].ToString()}";
                    }

                    var checkBlankRow = val.Replace(",", "");
                    //checkBlankRow = checkBlankRow.Replace(doubleQuote, "");

                    if (!string.IsNullOrEmpty(checkBlankRow))
                    {
                        val = val.Remove(0, 1);
                        fileContent.Add(val);
                    }
                }

                // NSRP-3800: Add date to file name so each time file name will be unique
                var newBlobName = $@"{datePart}_{table.TableName}.csv";
                Helpers.UploadBlobAsPutBlockList(blobClient, containerName, directoryPath, newBlobName, fileContent);
                temp.Add($@"CSV file generated with name - {newBlobName}");
            });

            foreach (var t in temp)
            {
                logInfoList.Add(t);
            }
        }
    }

    public struct NsrFileCnvrDataRow
    {
        public int CnfgSrcSysInfaId { get; set; }
        public int FileSeqId { get; set; }
        public int FileSetId { get; set; }
        public int FileSetSubId { get; set; }
        public int MaxFileSetSubId { get; set; }
        public string FileMask { get; set; }
        public string FileNamePattern { get; set; }
        public bool IsSouthLatinFile { get; set; }
        public bool HasDatetimeFormat { get; set; }
    }
}
