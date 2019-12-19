using System;
using System.Linq;
using System.Net.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace NsrFunctions
{
    class LockTableEntity : TableEntity
    {
        public LockTableEntity() : base() { }

        public LockTableEntity(string prefix) : base(prefix, prefix) { }

        [IgnoreProperty]
        public string Prefix
        {
            get { return this.PartitionKey; }
            set
            {
                this.PartitionKey = value;
                this.RowKey = value;
            }
        }

        public string DbState { get; set; }

        public static LockTableEntity GetLockRecord(string filePrefix, CloudTable bottlerFilesTable = null, CloudStorageAccount bottlerFilesTableStorageAccount = null)
        {
            bottlerFilesTable = bottlerFilesTable ?? Helpers.GetLockTable(bottlerFilesTableStorageAccount);

            return bottlerFilesTable.ExecuteQuery(
                new TableQuery<LockTableEntity>()
                    .Where(TableQuery.GenerateFilterCondition(@"PartitionKey", QueryComparisons.Equal, filePrefix)))
                .SingleOrDefault();
        }

        public static void Update(string filePrefix, string state, CloudTable bottlerFilesTable = null, CloudStorageAccount bottlerFilesTableStorageAccount = null)
        {
            var entity = GetLockRecord(filePrefix, bottlerFilesTable);
            entity.DbState = state;

            bottlerFilesTable = bottlerFilesTable ?? Helpers.GetLockTable(bottlerFilesTableStorageAccount);

            bottlerFilesTable.Execute(TableOperation.Replace(entity));
        }

        public static void Delete(string filePrefix, CloudTable bottlerFilesTable = null, CloudStorageAccount bottlerFilesTableStorageAccount = null)
        {
            var entity = GetLockRecord(filePrefix, bottlerFilesTable);

            bottlerFilesTable = bottlerFilesTable ?? Helpers.GetLockTable(bottlerFilesTableStorageAccount);

            bottlerFilesTable.Execute(TableOperation.Delete(entity));
        }

        public static void DeleteWithWarning(string filePrefix, CloudTable bottlerFilesTable = null, CloudStorageAccount bottlerFilesTableStorageAccount = null, TraceWriter log = null)
        {
            try
            {
                Delete(filePrefix, bottlerFilesTable);
                log?.Info($@"Lock Table Entry Deleted for filePrefix - {filePrefix}");
            }
            catch (StorageException)
            {
                log?.Warning($@"That's weird. The lock for prefix {filePrefix} wasn't there. Shouldn't happen!");
            }
        }

        internal static (bool Successful, LockTableEntity currentLock, HttpResponseMessage ErrorResponse) TryLock(string lockValue, Func<HttpResponseMessage> createErrorResponse, CloudTable lockTable, string status = null, TraceWriter log = null)
        {
            var entryMatchingPrefix = GetLockRecord(lockValue, lockTable);
            if (entryMatchingPrefix != null)
            {
                log?.Info($@"Lock denied. A lock already exists for '{lockValue}'");
                return (false, entryMatchingPrefix, createErrorResponse());
            }

            try
            {
                lockTable.Execute(TableOperation.Insert(new LockTableEntity(lockValue) { DbState = status }));
            }
            catch (StorageException ex)
            {
                log?.Warning($@"Lock denied lock due to exception ({ex.Message}). Assuming we've already locked on value '{lockValue}'");
                return (false, null, createErrorResponse());
            }

            return (true, null, null);
        }
    }
}
