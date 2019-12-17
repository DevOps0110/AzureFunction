using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace NsrFunctions
{
    public static class Validate
    {
        private const string DEFAULT_SUBFOLDER_NAME = @"valid-set-files";

        public class ValidationRequest
        {
            [JsonRequired, JsonProperty(@"type")]
            public ValidationRequestType Type { get; set; }

            [JsonProperty(@"prefix")]
            public string Prefix { get; set; }
            [JsonProperty(@"fileTypes")]
            public IList<string> Filetypes { get; set; }

            [JsonProperty(@"filename")]
            public string Filename { get; set; }

            public enum ValidationRequestType { Batch, UIFile }

            /// <summary>
            /// Determines if the structure of this Validation Request is valid. To be valid, for Batch types only Prefix and Filetypes must be specified. For UIFile types, only Filename must be specified.
            /// </summary>
            /// <returns>
            ///   <c>true</c> if this instance is valid; otherwise, <c>false</c>.
            /// </returns>
            public bool IsValid() => (this.Type == ValidationRequestType.Batch && !string.IsNullOrWhiteSpace(this.Prefix) && this.Filetypes?.Any() == true && string.IsNullOrWhiteSpace(this.Filename))
                || (this.Type == ValidationRequestType.UIFile && !string.IsNullOrWhiteSpace(this.Filename) && string.IsNullOrWhiteSpace(this.Prefix) && !(this.Filetypes?.Any()).GetValueOrDefault(false));
        }

        [FunctionName(@"Validate")]
        public static void Run([ServiceBusTrigger(@"ready-for-validation", Connection = @"ServiceBus_Trigger")]ValidationRequest payload, TraceWriter log)
        {
            if (!payload.IsValid()) throw new ArgumentException($@"The payload is invalid. For Batch validation requests, only Prefix & Filetypes must be specified. For UIFile validation requests, only Filename must be specified");

            IList<string> errors = null;

            if (payload.Type == ValidationRequest.ValidationRequestType.Batch && payload.Prefix.Contains("auto-curr-ntrl/valid-set-files"))
            {
                log.Info("The request is for auto-curr-ntrl/valid-set-files)");
                var fileName = payload.Filetypes[0];
                errors = ValidateAutoCurrencyFiles(fileName, log);
                log.Info($@"file url - {fileName}");
            }
            else if (payload.Type == ValidationRequest.ValidationRequestType.Batch)
            {
                errors = ValidateBatch(payload.Prefix, payload.Filetypes, log);
            }
            else
            {
                errors = ValidateSingleFile(payload.Filename, log);
            }

            LogErrors(payload, errors, log);
        }

        private static bool ValidateAutoCurrencyFiles(CloudBlobClient blobClient, List<string> errors, IListBlobItem blobDetails, BottlerBlobfileAttributes blobParts, TraceWriter log, string sourceSysId, string fileType, ref bool isAfRejected, bool isBatchUpload = false)
        {
            var blobRef = blobClient.GetBlobReferenceFromServer(blobDetails.StorageUri);
            using (var sqlClient = new NsrSqlClient())
            {                
                var fileSetId = sqlClient.GetFileSetId(blobParts.Filename);
                log.Info($@"fileSetId - {fileSetId} and filename - {blobParts.Filename}");

                // 2nd level validation
                log.Info($@"Validating {fileType} file having name {blobParts.Filename}...");
                var err = ValidateCsvStructureForAutoCurrencyFiles(blobRef, sourceSysId, log, blobParts.Filename, fileType, fileSetId, isBatchUpload, blobClient, blobParts, ref isAfRejected);

                if (err.Any())
                {
                    errors.AddRange(err);
                }

                return !err.Any();
            }
        }


        private static IList<string> ValidateAutoCurrencyFiles(string filename, TraceWriter log)
        {
            try
            {
                log.Info($@"Auto Currency File Upload - Filename : {filename} ");

                var blobClient = Helpers.GetCloudStorageAccount().CreateCloudBlobClient();
                Uri blobUri = new Uri(filename);
                var blobDetails = blobClient.GetBlobReferenceFromServer(blobUri);

                BottlerBlobfileAttributes blobParts = new BottlerBlobfileAttributes();
                blobParts = UISetBlobfileAttributes.ParseExchangeRateFile(filename);
                string fileType = null;

                log.Info($@"Check source sys id for auto file type {blobParts.FiletypePrefix}  ");
                var srcSysId = new NsrSqlClient().GetSourceSysIdForFileType(blobParts.FiletypePrefix, blobParts.ContainerName);

                fileType = blobParts.GetDbFiletype();
                log.Info($@"Source Id {srcSysId} and file type {fileType} and fact type {blobParts.FactType}");
                
                var errors = new List<string>();
                var isAfRejected = false;
                bool ifError = false;

                if (!ValidateAutoCurrencyFiles(blobClient, errors, blobDetails, blobParts, log, srcSysId, fileType, ref isAfRejected))
                {
                    LogErrorsToFile(blobClient, errors, srcSysId, blobParts, log);
                    ifError = true;
                }

                if (ifError)
                {
                    log.Info($@"moving blob to reject...");
                    Helpers.MoveBlobs(blobClient, blobDetails, $@"{blobParts.BottlerName}/invalid-data/rejects", log: log);
                }

                return errors;
            }
            catch (Exception ex)
            {
                var excpList = new List<string>();
                log.Info($@"exception in Auto Currency File Validation - {ex.Message}");
                excpList.Add($@"exception in Auto Currency File Validation - {ex.Message}");
                return excpList;
            }
        }

        private static IEnumerable<string> ValidateCsvStructureForAutoCurrencyFiles(ICloudBlob blob, string sourceSysId, TraceWriter log, string filename, string fileType, string fileSetId, bool isBatchUpload, CloudBlobClient blobClient, BottlerBlobfileAttributes blobParts, ref bool isAfRejected)
        {
            var errs = new List<string>();
            
            List<string> newFileContent = new List<string>();

            try
            {
                using (var blobReader = new StreamReader(blob.OpenRead()))
                {
                    log.Info($@"Fetching field specs for {fileType} - {sourceSysId} - {blobParts.FactType} - {fileSetId}");
                    var fieldSpecs = new NsrSqlClient().GetFieldSpecsForFile(fileType, sourceSysId, blobParts.FactType).ToList();
                                        
                    int lineNumberForLogging = 0;

                    for (int lineNumber = 1; !blobReader.EndOfStream; lineNumber++)
                    {
                        var line = blobReader.ReadLine();
                        log.Info($@"Replacing double quote for line no {lineNumber} - {line}");
                        newFileContent.Add(Helpers.ReplaceDoubleQuoteAndCommaInValues(line));

                        if (lineNumberForLogging == 0)
                            lineNumberForLogging = 1;
                        else
                            lineNumberForLogging = lineNumberForLogging + 1;

                        //if file has only blank row
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            log.Info($@"Error - file contains white spaces");
                            errs.Add($@"{lineNumberForLogging},NA,File contains blank row");
                            continue;
                        }

                        // When we log error messages, we pipe-separate the value of the line instead of its comma-separated "truth"
                        // instead of adding this replacement everywhere in the code; just do it here and use it that way
                        // NSRD-13413                        

                        
                        var fields = GetFieldCountForUIFile(line.Trim());

                        //Check fields counts are matching in file
                        ////NSRP - 4949 - LCF File UI upload will have one extra column then its in configuration in t_cnfg_file table
                        //if (fileType.ToLowerInvariant().Equals("local conversion factor") && fields != fieldSpecs.Count + 1)
                        //{
                        //    errs.Add($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count + 1} values");
                        //}
                        //else 
                        if (fields != fieldSpecs.Count)
                        {
                            log.Info($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count} values");
                            errs.Add($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count} values");
                        }
                       
                        
                    }

                    // Validate file is UTF-8 encoded
                    if (blobReader.CurrentEncoding != System.Text.Encoding.UTF8)
                    {
                        log.Info($@"NA,NA,{blob.Name} is not UTF-8 encoded");
                        errs.Add($@"NA,NA,{blob.Name} is not UTF-8 encoded");
                    }
                    else
                    {
                        var header = new byte[3];

                        // We've done some reading in the blob; reset back to the front because now we need to check the first 3 bytes
                        blobReader.BaseStream.Position = 0;
                        blobReader.BaseStream.Read(header, 0, header.Length);

                        if (header[0] == 0xef && header[1] == 0xbb && header[2] == 0xbf)
                        {
                            log.Info($@"NA,NA,{blob.Name} is not UTF-8 encoded; instead it's UTF-8 BOM encoded");
                            errs.Add($@"NA,NA,{blob.Name} is not UTF-8 encoded; instead it's UTF-8 BOM encoded");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                log.Info($@"NA,NA,Invalid File Content: {ex.Message}");
                errs.Add($@"NA,NA,Invalid File Content: {ex.Message}");
            }

            using (var sqlClient = new NsrSqlClient())
            {
                //Check if Product or Shipfrom AF-Rejected for batch upload
                if (!errs.Any())
                {
                    //NSRD-18915         
                    //Copy source blob to archive with prefix 'SRC~' for reference
                    var fileCopyPath = $@"{blobParts.Subfolder}/archive";
                    Helpers.CopySourceFileToArchive(log, Helpers.GetCloudStorageAccount(), blobParts.ContainerName, $@"{blobParts.Subfolder}/valid-set-files", filename, "SRC~", fileCopyPath);

                    Helpers.UploadBlobAsPutBlockList(blobClient, blobParts.ContainerName, $@"{blobParts.Subfolder}/valid-set-files", filename, newFileContent);

                    log.Info($@"INFA READY...");

                    sqlClient.UpdateStatusForBottlerFileReceived(sourceSysId, filename, "INFA-READY", fileSetId);
                }
                else
                {
                    errs.Insert(0, $@"Line_Number,Error_Record,Error_Description");

                    log.Info($@"AF-REJECTED - {sourceSysId} {filename} {fileSetId}...");

                    sqlClient.InsertIntoErrorTracker(sourceSysId, filename, "ERROR", "AF-REJECTED");
                    sqlClient.UpdateStatusForBottlerFileForAutoCurrencyFiles(sourceSysId, filename, "AF-REJECTED", fileSetId);                    
                }
            }

            return errs;
        }


        private static IList<string> ValidateBatch(string fullPathPrefix, IList<string> fileTypes, TraceWriter log)
        {
            var fullPathPrefixParts = fullPathPrefix.Split('/');
            var filePrefix = fullPathPrefixParts.Last();
            var filePrefixParts = filePrefix.Split('_');

            // Check to see if lock record says this set is in progress
            var lockTable = Helpers.GetLockTable();
            if (!ShouldProceed(lockTable, fullPathPrefix, filePrefix, log))
            {
                return null;
            }

            log.Info($@"FilePrefix : {filePrefix} and Prefix : {fullPathPrefix}...");
            var blobClient = Helpers.GetCloudStorageAccount().CreateCloudBlobClient();
            var targetBlobs = blobClient.ListBlobs(WebUtility.UrlDecode(fullPathPrefix));

            var containerName = fullPathPrefixParts.First();
            var bottlerName = filePrefixParts.First();        //.Split('-').Last();
            var sourceSysId = new NsrSqlClient().GetSourceSysIdForBottlerName(bottlerName, containerName);
            log.Info($@"bottlerName: {bottlerName} and sourceSysId: {sourceSysId} and targetBlobs.count: {targetBlobs.Count().ToString()}...");

            var fileSetErrors = new List<string>();
            List<IListBlobItem> moveList = new List<IListBlobItem>();

            var targetBlobsAndParts = targetBlobs.Select(i => (Blob: i, Parts: BottlerBlobfileAttributes.Parse(i.Uri.OriginalString, DEFAULT_SUBFOLDER_NAME)));
            var newTargetBlobList = GetFilesInOrder(targetBlobsAndParts);

            var isAfRejected = false;
            log.Info($@"Checking if LCF file present in the uploaded set");
            newTargetBlobList = CheckIfLCFFilePresent(newTargetBlobList, log);

            log.Info($@"Going to validate all the files");
            foreach (var (Blob, Parts) in newTargetBlobList)
            {
                // Check if fileName is in the list before running validation
                if (FileNeedsToBeValidated(bottlerName, sourceSysId, fileTypes, Blob, Parts, log))
                {
                    var fileType = Parts.GetDbFiletype();
                    var fileErrors = new List<string>();
                    if (!ValidateFile(blobClient, fileErrors, Blob, Parts, log, sourceSysId, fileType, ref isAfRejected, true))
                    {
                        LogErrorsToFile(blobClient, fileErrors, sourceSysId, Parts, log);
                        moveList.Add(Blob);
                        fileSetErrors.AddRange(fileErrors);
                    }
                }
                else
                {
                    log.Verbose($@"{Parts.FilenameWithoutExtension} skipped. Isn't in the list of file types to process ({string.Join(", ", fileTypes)}) for bottler '{bottlerName}' ({sourceSysId})");
                }
            }

            // update the "lock" record from the bottler filesets table; we're done processing the fileset
            LockTableEntity.DeleteWithWarning(filePrefix, lockTable, log: log);

            if (fileSetErrors?.Any() == true)
            {
                var bottlerFolderName = new NsrSqlClient().GetBottlerFolderName(bottlerName, containerName);
                Helpers.MoveBlobs(blobClient, moveList, $@"{bottlerFolderName}/invalid-data/rejects", log: log);
                log.Info($@"Rejected files moved to {bottlerFolderName}/invalid-data/rejects");
            }

            return fileSetErrors;
        }

        private static IList<string> ValidateSingleFile(string filename, TraceWriter log)
        {
            try
            {
                log.Info($@"Single File Upload - Filename : {filename} ");

                var blobClient = Helpers.GetCloudStorageAccount().CreateCloudBlobClient();
                Uri blobUri = new Uri(filename);
                var blobDetails = blobClient.GetBlobReferenceFromServer(blobUri);

                BottlerBlobfileAttributes blobParts = new BottlerBlobfileAttributes();
                bool isGlobalFile = false;
                List<string> srcSysIdList = null;
                string fileType = null;

                // UiSetBlobfileAttributes is a less-strict regex; use it to figure out which one we actually need.
                if (Helpers.IsManualOrAutoSubmissionFile(filename))
                {
                    log.Info($@"File submitted thru Manual or Auto submission ");

                    // if we have a volume, revenue or discount file, go for the big regex (contains bottler name, facttype, etc)
                    blobParts = BottlerBlobfileAttributes.Parse(blobUri.OriginalString, DEFAULT_SUBFOLDER_NAME);
                }
                else
                {
                    log.Info($@"Other files submitted ");
                    blobParts = UISetBlobfileAttributes.Parse(filename);

                    // TODO: Do we need to do this logic for bulk uploads?
                    log.Info($@"Check if file {blobParts.Filename} is currency neutral file ");
                    if (Helpers.IsCurrencyNeutralFile(filename))
                    {
                        (isGlobalFile, srcSysIdList, fileType) = MakeDbEntriesIfGlobalFile(blobParts.Filename, log, ref blobParts);
                    }
                }

                if (!isGlobalFile && srcSysIdList == null)
                {
                    log.Info($@"Check source sys id for bottler {blobParts.BottlerName} and container {blobParts.ContainerName} ");
                    var srcSysId = new NsrSqlClient().GetSourceSysIdForBottlerName(blobParts.BottlerName, blobParts.ContainerName);
                    srcSysIdList = new List<string>() { srcSysId };
                    fileType = blobParts.GetDbFiletype();
                    log.Info($@"Source Id {srcSysId} and file type {fileType} and fact type {blobParts.FactType}");
                }

                if (string.IsNullOrEmpty(blobParts.FactType))
                {
                    blobParts.GetDbFactType();
                }

                var errors = new List<string>();
                var isAfRejected = false;
                bool ifError = false;
                foreach (var srcId in srcSysIdList)
                {
                    if (!ValidateFile(blobClient, errors, blobDetails, blobParts, log, srcId, fileType, ref isAfRejected))
                    {
                        LogErrorsToFile(blobClient, errors, srcId, blobParts, log);
                        ifError = true;
                    }
                }

                if (ifError)
                {
                    log.Info($@"moving blob to reject...");
                    Helpers.MoveBlobs(blobClient, blobDetails, $@"{blobParts.BottlerName}/invalid-data/rejects", log: log);
                }

                return errors;
            }
            catch (Exception ex)
            {
                var excpList = new List<string>();
                log.Info($@"exception in validation - {ex.Message}");
                excpList.Add($@"exception in validation - {ex.Message}");
                return excpList;
            }
        }

        private static (bool, List<string>, string) MakeDbEntriesIfGlobalFile(string filename, TraceWriter log, ref BottlerBlobfileAttributes blobParts)
        {
            log.Info($@"Inside make db entries file name - {filename}");
            // TODO: Could these statements better key off a container name == 'global'?
            if (filename.Contains("actual_exchange_rates", StringComparison.OrdinalIgnoreCase))
            {
                var srcSysIdList = SetGlobalFileAFReady(filename, "AC-Currency Neutral", "AC-Currency Neutral", @"AC-CN", 28, log);
                blobParts.GetDbFactType(@"AC-CN");
                log.Info($@"Data Inserted into Audit Table for actual_exchange_rates");
                return (true, srcSysIdList, "AC-Currency Neutral");
            }
            else if (filename.Contains("rolling_estimate_exchange_rates", StringComparison.OrdinalIgnoreCase))
            {
                var srcSysIdList = SetGlobalFileAFReady(filename, "RE-Currency Neutral", "RE-Currency Neutral", @"RE-CN", 29, log);
                blobParts.GetDbFactType(@"RE-CN");
                log.Info($@"Data Inserted into Audit Table for rolling_estimate_exchange_rates");
                return (true, srcSysIdList, "RE-Currency Neutral");
            }
            else if (filename.Contains("business_plan_exchange_rates", StringComparison.OrdinalIgnoreCase))
            {
                var srcSysIdList = SetGlobalFileAFReady(filename, "BP-Currency Neutral", "BP-Currency Neutral", @"BP-CN", 44, log);
                blobParts.GetDbFactType(@"BP-CN");
                log.Info($@"Data Inserted into Audit Table for business_plan_exchange_rates");
                return (true, srcSysIdList, "BP-Currency Neutral");
            }


            return (false, null, null);
        }

        private static List<string> SetGlobalFileAFReady(string filename, string bottlerName, string fileMask, string factType, int moduleId, TraceWriter log)
        {
            //Giving open command error for one of the query below so using new eachtime
            using (var sqlClient = new NsrSqlClient())
            {
                var sourceSysIdList = sqlClient.GetSourceSysIdListForFileType(bottlerName).ToList();
                var fileType = sqlClient.GetTypeForBottler(fileMask);

                foreach (var sourceSysId in sourceSysIdList)
                {
                    int fileSetId = sqlClient.GetFileSetId();
                    log.Info($@"Insert for source id {sourceSysId} for file {fileMask} and filesetid {fileSetId} in audit table as af-ready");
                    sqlClient.BottlerFileReceived(sourceSysId, $@"{filename}", "AF-READY", moduleId, fileSetId, factType, fileType);
                }

                return sourceSysIdList;
            }
        }

        private static bool FileNeedsToBeValidated(string bottlerName, string sourceSysId, IList<string> filesToProcess, IListBlobItem blobDetails, BottlerBlobfileAttributes blobParts, TraceWriter log)
        {
            log.Info($@"blobParts.Filetype: {blobParts.Filetype} and blobParts.Filename: {blobParts.FilenameWithoutExtension} ");
            return filesToProcess.Contains(blobParts.Filetype, StringComparer.OrdinalIgnoreCase);
        }

        private static bool ValidateFile(CloudBlobClient blobClient, List<string> errors, IListBlobItem blobDetails, BottlerBlobfileAttributes blobParts, TraceWriter log, string sourceSysId, string fileType, ref bool isAfRejected, bool isBatchUpload = false)
        {
            var blobRef = blobClient.GetBlobReferenceFromServer(blobDetails.StorageUri);
            using (var sqlClient = new NsrSqlClient())
            {
                var headersRequired = sqlClient.HeadersRequiredForBottlerFile(sourceSysId, fileType, blobParts.FactType);
                log.Info($@"Header required - {headersRequired} for fact type - {blobParts.FactType} file type - {fileType} and source id - {sourceSysId}");
                var fileSetId = sqlClient.GetFileSetId(blobParts.Filename);
                log.Info($@"fileSetId - {fileSetId}");

                // 2nd level validation
                log.Info($@"Validating {fileType} file having name {blobParts.Filename}...");
                var err = ValidateCsvStructure(blobRef, headersRequired, sourceSysId, log, blobParts.Filename, fileType, fileSetId, isBatchUpload, blobClient, blobParts, ref isAfRejected);

                if (err.Any())
                {
                    errors.AddRange(err);
                }

                return !err.Any();
            }
        }

        private static void LogErrorsToFile(CloudBlobClient blobClient, List<string> errors, string sourceSysId, BottlerBlobfileAttributes blobParts, TraceWriter log, NsrSqlClient sqlClient = null)
        {
            try
            {
                var idColumns = (sqlClient ?? new NsrSqlClient()).GetFileId(sourceSysId, blobParts.Filename);
                log.Info($@"Error File Creation - {idColumns.Value.FileSetId}_Error_{blobParts.Filename}");
                var errorFileName = $@"{idColumns.Value.FileSetId}_Error_{blobParts.Filename}";
                var bottlerName = new NsrSqlClient().GetBottlerFolderName(blobParts.BottlerName, blobParts.ContainerName);

                log.Info($@"Bottler folder name is {bottlerName} for Error File Creation - {blobParts.BottlerName},{blobParts.ContainerName}");

                Helpers.CreateErrorFile(blobClient, blobParts.ContainerName, bottlerName, errorFileName, errors, log);

                log.Info($@"Error File Creation Completed");
            }
            catch (Exception ex)
            {
                log.Info($@"Exception occurred while creating error file - {ex.Message}");
            }
        }

        private static IEnumerable<string> ValidateCsvStructure(ICloudBlob blob, bool shouldHaveHeaders, string sourceSysId, TraceWriter log, string filename, string fileType, string fileSetId, bool isBatchUpload, CloudBlobClient blobClient, BottlerBlobfileAttributes blobParts, ref bool isAfRejected)
        {
            var errs = new List<string>();
            using (var sqlClient = new NsrSqlClient())
            {
                //NSRV2O-188 Check if Product or Shipfrom AF-Rejected for batch upload
                if (isBatchUpload && fileType.ToLowerInvariant().Equals("local conversion factor") && isAfRejected)
                {
                    errs.Insert(0, $@"Line_Number,Error_Record,Error_Description");
                    errs.Add($@"NA,NA,Either Product or ShipFrom file has been rejected for the same set");

                    log.Info($@"AF-REJECTED...");

                    sqlClient.InsertIntoErrorTracker(sourceSysId, filename, "ERROR", "AF-REJECTED");
                    sqlClient.UpdateStatusForBottlerFileReceived(sourceSysId, filename, "AF-REJECTED", fileSetId);
                }
            }

            if (errs.Any())
                return errs;

            List<string> newFileContent = new List<string>();

            try
            {
                using (var blobReader = new StreamReader(blob.OpenRead()))
                {
                    var fieldSpecs = new NsrSqlClient().GetFieldSpecsForFile(fileType, sourceSysId, blobParts.FactType).ToList();

                    //NSRV2O - 187 - LCF File UI upload will have one extra column then its in configuration in t_cnfg_file table
                    if (isBatchUpload && fileType.Equals("local conversion factor", StringComparison.OrdinalIgnoreCase))
                    {
                        log.Info($@"LCF file found and fieldspec count {fieldSpecs.Count} ");

                        fieldSpecs.Remove(fieldSpecs.Last());

                        log.Info($@"Frozen Flag column removed and fieldspec count {fieldSpecs.Count} ");
                    }

                    int lineNumberForLogging = 0;

                    if (shouldHaveHeaders && isBatchUpload)
                    {
                        var headerLine = blobReader.ReadLine();
                        newFileContent.Add(Helpers.ReplaceDoubleQuoteAndCommaInValues(headerLine));

                        lineNumberForLogging = 1;
                        //If file has only blank row
                        if (string.IsNullOrWhiteSpace(headerLine))
                        {
                            errs.Add($@"1,NA,File contains blank header row");
                            log.Info($@"Header validation error - blank header row");
                        }

                        //Check all header validations
                        var headerErrors = EnforceHeaderLine(headerLine, fieldSpecs);
                        if (headerErrors.Any())
                        {
                            errs.AddRange(headerErrors.Select(e => $@"1,{headerLine.Replace(',', '|')},{e}"));
                            log.Info($@"Header validation error");
                        }
                    }

                    for (int lineNumber = 1; !blobReader.EndOfStream; lineNumber++)
                    {
                        var line = blobReader.ReadLine();
                        newFileContent.Add(Helpers.ReplaceDoubleQuoteAndCommaInValues(line));

                        if (lineNumberForLogging == 0)
                            lineNumberForLogging = 1;
                        else
                            lineNumberForLogging = lineNumberForLogging + 1;

                        //if file has only blank row
                        if (string.IsNullOrWhiteSpace(line))
                        {
                            errs.Add($@"{lineNumberForLogging},NA,File contains blank row");
                            continue;
                        }

                        // When we log error messages, we pipe-separate the value of the line instead of its comma-separated "truth"
                        // instead of adding this replacement everywhere in the code; just do it here and use it that way
                        // NSRD-13413                        

                        if (!isBatchUpload)
                        {
                            var fields = GetFieldCountForUIFile(line.Trim());

                            //Check fields counts are matching in file
                            ////NSRP - 4949 - LCF File UI upload will have one extra column then its in configuration in t_cnfg_file table
                            //if (fileType.ToLowerInvariant().Equals("local conversion factor") && fields != fieldSpecs.Count + 1)
                            //{
                            //    errs.Add($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count + 1} values");
                            //}
                            //else 
                            if (fields != fieldSpecs.Count)
                            {
                                errs.Add($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count} values");
                            }
                        }
                        else
                        {
                            line = line.Replace("\",\"", "\"|\"");
                            var fields = line.Split('|');

                            //Check fields counts are matching in file
                            if (fields.Length != fieldSpecs.Count)
                            {
                                errs.Add($@"{lineNumberForLogging},{line},Field's count doesn't match required count; Should have {fieldSpecs.Count} values");
                            }
                            else
                            {
                                for (int i = 0; i < fields.Length; i++)
                                {
                                    var field = fields[i];
                                    int fieldIndexForLogging = i + 1;
                                    if (field.Length > 1)
                                    {
                                        // each field must be enclosed in double quotes
                                        if (field[0] != '"' || field.Last() != '"')
                                        {
                                            errs.Add($@"{lineNumberForLogging},{line},Field {fieldIndexForLogging}'s value ({field}) not enclosed in double quotes ("")");
                                        }
                                        else
                                        {
                                            // since they're double quoted and the values from the database aren't, trim off the double quotes now for comparison purposes
                                            field = field.Substring(1, field.Length - 2);

                                            // if header aren't required, on the first line make sure field values aren't header values
                                            if (!shouldHaveHeaders
                                                && lineNumber == 1
                                                && field.Equals(fieldSpecs[i].Description, StringComparison.OrdinalIgnoreCase))
                                            {
                                                errs.Add($@"{lineNumberForLogging},{line},Header's not required though Field {fieldIndexForLogging}'s value added as header value");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        errs.Add($@"{lineNumberForLogging},{line},Field {fieldIndexForLogging}'s value is missing, should have at least double quotes ("")");
                                    }
                                }
                            }
                        }
                    }

                    // Validate file is UTF-8 encoded
                    if (blobReader.CurrentEncoding != System.Text.Encoding.UTF8)
                    {
                        errs.Add($@"NA,NA,{blob.Name} is not UTF-8 encoded");
                    }
                    else
                    {
                        var header = new byte[3];

                        // We've done some reading in the blob; reset back to the front because now we need to check the first 3 bytes
                        blobReader.BaseStream.Position = 0;
                        blobReader.BaseStream.Read(header, 0, header.Length);

                        if (header[0] == 0xef && header[1] == 0xbb && header[2] == 0xbf)
                        {
                            errs.Add($@"NA,NA,{blob.Name} is not UTF-8 encoded; instead it's UTF-8 BOM encoded");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errs.Add($@"NA,NA,Invalid File Content: {ex.Message}");
            }

            using (var sqlClient = new NsrSqlClient())
            {
                //Check if Product or Shipfrom AF-Rejected for batch upload
                if (!errs.Any())
                {
                    //NSRD-18915         
                    //Copy source blob to archive with prefix 'SRC~' for reference
                    var fileCopyPath = $@"{blobParts.Subfolder}/archive";
                    Helpers.CopySourceFileToArchive(log, Helpers.GetCloudStorageAccount(), blobParts.ContainerName, $@"{blobParts.Subfolder}/valid-set-files", filename, "SRC~", fileCopyPath);

                    Helpers.UploadBlobAsPutBlockList(blobClient, blobParts.ContainerName, $@"{blobParts.Subfolder}/valid-set-files", filename, newFileContent);

                    log.Info($@"INFA READY...");

                    sqlClient.UpdateStatusForBottlerFileReceived(sourceSysId, filename, "INFA-READY", fileSetId);
                }
                else
                {
                    errs.Insert(0, $@"Line_Number,Error_Record,Error_Description");

                    log.Info($@"AF-REJECTED...");

                    sqlClient.InsertIntoErrorTracker(sourceSysId, filename, "ERROR", "AF-REJECTED");
                    sqlClient.UpdateStatusForBottlerFileReceived(sourceSysId, filename, "AF-REJECTED", fileSetId);

                    if (fileType.Equals("product", StringComparison.OrdinalIgnoreCase) || fileType.Equals("shipfrom", StringComparison.OrdinalIgnoreCase))
                    {
                        isAfRejected = true;
                    }
                }
            }

            return errs;
        }

        public static int GetFieldCountForUIFile(string line)
        {
            string doubleQuote = "\"";

            var splitValues = line.Split(',');
            int fieldCount = 0;
            bool isDoubleQuoteValue = false;

            foreach (var val in splitValues)
            {
                if ((val.StartsWith(doubleQuote) && !val.EndsWith(doubleQuote)) || (val.StartsWith(doubleQuote) && val.Length == 1 && !isDoubleQuoteValue))
                {
                    isDoubleQuoteValue = true;
                }
                else if (isDoubleQuoteValue && val.EndsWith(doubleQuote))
                {
                    isDoubleQuoteValue = false;
                }

                if (!isDoubleQuoteValue)
                {
                    fieldCount++;
                }
            }

            return fieldCount;
        }

        private static IEnumerable<string> EnforceHeaderLine(string headerLine, IEnumerable<FieldSpec> fieldSpecs)
        {
            // header line is CSV
            var vals = headerLine.Split(',');
            var numFieldSpecs = fieldSpecs.Count();
            // We better have same number of header values as we do field specs
            if (numFieldSpecs != vals.Length)
            {
                return new[] { $@"Incorrect number of headers. Expecting {numFieldSpecs}, have {vals.Length}" };
            }

            // each header value must be surrounded in double-quotes (")
            var errList = vals.Where(v => v[0] != '"' || v.Last() != '"')
                .Select((h, i) => $@"Each header description must be surrounded in double-quotes ('""'). [{i}: {h}] is not")
                .ToList();  // to avoid multi-eval
            //if (!errList.Any())
            //{
            //    // Get all the values from the file where their content doesn't match what's specified in the database
            //    errList = vals
            //        // since they're double quoted and the values from the database aren't, trim off the double quotes now for comparison purposes
            //        .Select(v => v.Substring(1, v.Length - 2))
            //        // Put the value of the header & its spec side-by-side in a list
            //        .Zip(fieldSpecs, (v, s) => new { Value = v, Spec = s })
            //        // Pick out those whose value doesn't match the spec's description
            //        .Where(i => i.Value != i.Spec.Description)
            //        // Choose the right error message if empty vs full
            //        .Select(i => string.IsNullOrEmpty(i.Value)
            //            ? $@"Header name is missing. Should be {i.Spec.Description}"
            //            : $@"{i.Value} is misnamed. Should be {i.Spec.Description}")
            //        // Do this quick!
            //        .AsParallel()
            //        .ToList();  // to avoid multi-eval
            //}

            return errList;
        }

        private static bool ShouldProceed(CloudTable bottlerFilesTable, string prefix, string filePrefix, TraceWriter log)
        {
            try
            {
                var lockRecord = LockTableEntity.GetLockRecord(filePrefix, bottlerFilesTable);
                if (lockRecord?.DbState.Equals(@"waiting", StringComparison.OrdinalIgnoreCase) == true)
                {
                    // Update the lock record to mark it as in progress
                    lockRecord.DbState = @"InProgress";
                    bottlerFilesTable.Execute(TableOperation.Replace(lockRecord));
                    return true;
                }
                else
                {
                    log.Info($@"Validate for {prefix} skipped. State was {lockRecord?.DbState ?? @"[null]"}.");
                }
            }
            catch (StorageException)
            {
                log.Info($@"Validate for {prefix} skipped (StorageException). Somebody else picked it up already.");
            }

            return false;
        }

        private static void LogErrors(ValidationRequest payload, IEnumerable<string> errors, TraceWriter tracer = null)
        {
            if (errors?.Any() == true)
            {
                if (payload.Type == ValidationRequest.ValidationRequestType.Batch)
                {
                    tracer?.Error($@"Errors found in batch {payload.Prefix}: {string.Join(@", ", errors)}");
                }
                else if (payload.Type == ValidationRequest.ValidationRequestType.UIFile)
                {
                    tracer?.Error($@"Errors found in file {payload.Filename}: {string.Join(@", ", errors)}");
                }

                // TODO: Want to do other things with error messages? Put that here
            }
        }

        private static IEnumerable<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> GetFilesInOrder(IEnumerable<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> targetBlobsAndParts) =>
            // Order the values by assigning a sort weight of '1' to sales/transaction files and a sort weight of '0' to all others
            targetBlobsAndParts.OrderBy(i => i.Parts.Filetype.Equals(@"sales", StringComparison.OrdinalIgnoreCase) || i.Parts.Filetype.Equals(@"transaction", StringComparison.OrdinalIgnoreCase) ? 1 : 0);

        private static IEnumerable<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> CheckIfLCFFilePresent(IEnumerable<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> targetBlobsAndParts, TraceWriter log)
        {
            log.Info($@"Checking if LCF File Present");
            bool isLCFPresent = false;
            List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> newList = new List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)>();
            List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> lcfList = new List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)>();
            List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)> salesList = new List<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)>();

            foreach (var (Blob, Parts) in targetBlobsAndParts)
            {
                if (Parts.Filetype.Equals(@"lcf", StringComparison.OrdinalIgnoreCase))
                {
                    isLCFPresent = true;
                    lcfList.Add((Blob, Parts));
                }
                else if (Parts.Filetype.Equals(@"sales", StringComparison.OrdinalIgnoreCase) || Parts.Filetype.Equals(@"transaction", StringComparison.OrdinalIgnoreCase))
                {
                    salesList.Add((Blob, Parts));
                }
                else
                {
                    newList.Add((Blob, Parts));
                }
            }

            if (isLCFPresent)
            {
                newList.Add(lcfList.First());
                newList.Add(salesList.First());

                return newList.AsEnumerable<(IListBlobItem Blob, BottlerBlobfileAttributes Parts)>();
            }
            else
                return targetBlobsAndParts;

        }

    }
}

