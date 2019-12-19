using System.Collections.Generic;

namespace NsrFunctions
{
    class BlobFilenameVsDatabaseFileMaskComparer : IEqualityComparer<string>
    {
        public bool Equals(string x, string y) => y.Contains(x);

        public int GetHashCode(string obj) => obj.GetHashCode();
    }
}
