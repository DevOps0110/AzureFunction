using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace NsrFunctions
{
    public static class MoveBlobs
    {
        [FunctionName("MoveBlobs")]
        public static void Run([ServiceBusTrigger(@"move-blobs", Connection = @"ServiceBus_Trigger")]MoveRequest req, TraceWriter log)
        {
            if ((req.RelativeFilePaths?.Count()).GetValueOrDefault(0) == 0 && string.IsNullOrEmpty(req.FilePrefix))
            {
                throw new ArgumentNullException(nameof(req), @"One of file_prefix or file_paths must be populated");
            }
            else if ((req.RelativeFilePaths?.Count()).GetValueOrDefault(0) != 0 && !string.IsNullOrEmpty(req.FilePrefix))
            {
                throw new ArgumentException(@"Only one of file_prefix or file_paths must be populated", nameof(req));
            }

            log.Info($@"Move Process Started...");
            log.Info(req.ToString());

            IEnumerable<IListBlobItem> moveList = Enumerable.Empty<IListBlobItem>();
            if (!string.IsNullOrEmpty(req.FilePrefix))
            {
                moveList = Helpers.GetCloudBlobClient().ListBlobs($@"{req.ContainerName}/{req.FilePrefix}");
            }
            else
            {
                moveList = req.RelativeFilePaths.Select(f => Helpers.GetCloudBlobClient().GetContainerReference(req.ContainerName).GetBlobReference(f));
            }

            if (moveList.Any())
            {
                log.Info($@"# of files to move: {moveList.Count()}");
                log.Info(@"Moving files...");

                Helpers.MoveBlobs(Helpers.GetCloudBlobClient(), moveList, req.TargetFolder, log: log);
            }
            else
            {
                log.Info(@"No files to move.");
            }

            log.Info($@"Move completed.");
        }

        public sealed class MoveRequest
        {
            [JsonProperty(@"containername")]
            public string ContainerName { get; set; }
            [JsonProperty(@"target_folder")]
            public string TargetFolder { get; set; }
            [JsonProperty(@"file_paths")]
            public IList<string> RelativeFilePaths { get; set; }
            [JsonProperty(@"file_prefix")]
            public string FilePrefix { get; set; }

            public override string ToString() => $@"{nameof(this.ContainerName)}: {this.ContainerName}, {nameof(this.TargetFolder)}: {this.TargetFolder ?? @"[null]"}, {nameof(this.RelativeFilePaths)}: [{(this.RelativeFilePaths?.Any() == true ? string.Join(", ", this.RelativeFilePaths) : @"null")}], {nameof(this.FilePrefix)}: {this.FilePrefix ?? @"[null]"}";
        }
    }
}
