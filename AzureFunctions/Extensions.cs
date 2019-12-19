using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage.Blob;

namespace NsrFunctions
{
    static class EnumExtensions
    {
        public static IEnumerable<Enum> GetFlags(this Enum value)
        {
            return GetFlags(value, Enum.GetValues(value.GetType()).Cast<Enum>().ToArray());
        }

        public static IEnumerable<Enum> GetIndividualFlags(this Enum value)
        {
            return GetFlags(value, GetFlagValues(value.GetType()).ToArray());
        }

        private static IEnumerable<Enum> GetFlags(Enum value, Enum[] values)
        {
            ulong bits = Convert.ToUInt64(value);
            List<Enum> results = new List<Enum>();
            for (int i = values.Length - 1; i >= 0; i--)
            {
                ulong mask = Convert.ToUInt64(values[i]);
                if (i == 0 && mask == 0L)
                    break;
                if ((bits & mask) == mask)
                {
                    results.Add(values[i]);
                    bits -= mask;
                }
            }
            if (bits != 0L)
                return Enumerable.Empty<Enum>();
            if (Convert.ToUInt64(value) != 0L)
                return results.Reverse<Enum>();
            if (bits == Convert.ToUInt64(value) && values.Length > 0 && Convert.ToUInt64(values[0]) == 0L)
                return values.Take(1);
            return Enumerable.Empty<Enum>();
        }

        private static IEnumerable<Enum> GetFlagValues(Type enumType)
        {
            ulong flag = 0x1;
            foreach (var value in Enum.GetValues(enumType).Cast<Enum>())
            {
                ulong bits = Convert.ToUInt64(value);
                if (bits == 0L)
                    //yield return value;
                    continue; // skip the zero value
                while (flag < bits) flag <<= 1;
                if (flag == bits)
                    yield return value;
            }
        }
    }

    static class SqlExtensions
    {
        public static SqlCommand OpenAndCreateCommand(this SqlConnection connection)
        {
            if (connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }

            return connection.CreateCommand();
        }
    }

    static class CloudBlobClientExtensions
    {
        public static IEnumerable<IListBlobItem> ListBlobs(this CloudBlobClient blobClient, string containerName, string directoryPath, string matchPrefix, bool caseInsensitive = true, TraceWriter log = null)
        {
            //log.Info($@"Extension method called");
            var allBlobs = blobClient.GetContainerReference(containerName).GetDirectoryReference(directoryPath).ListBlobs();
            //log.Info($@"Blobs Count - {allBlobs.Count()}");
            var matches = allBlobs.Where(match => match.Uri.Segments.Last().StartsWith(matchPrefix,
                caseInsensitive ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal));
            //log.Info($@"Matches Count - {matches.Count()}");
            return matches;
        }
    }

    static class StringExtensions
    {
        public static bool Contains(this string @string, string substring, StringComparison comparar) => @string.IndexOf(substring, comparar) != -1;
    }
}
