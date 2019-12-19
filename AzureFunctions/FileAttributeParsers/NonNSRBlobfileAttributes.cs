using System.Linq;
using System.Text.RegularExpressions;

namespace NsrFunctions
{
    class NonNSRBlobfileAttributes
    {        
        static readonly Regex landingFilesRegex = new Regex(@"^\S*/(?<container>[^/]+)/(?<pathInContainer>(?<bottlerName>[^/]+)/landing-files)/(?<fullFilename>(?<filenameWithoutExtension>[^/]+)\.(csv|txt))$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        // we disallow ~ in the filename here because in nsr-files handling of ProcessNonNSRFiles, we re-upload the file prefixed by [filecode]~ - this regex prevents that upload from being handled by the same nsr-files code again (since it's uploaded to the same place as a new file and will trigger a new storage event)
        static readonly Regex nonNSRFilesRegex = new Regex(@"^\S*/(?<container>[^/]+)/(?<pathInContainer>(?<bottlerName>[^/]+)/nsr-files)/(?<fullFilename>(?<filenameWithoutExtension>[^/@]+)\.csv)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        static readonly Regex landingExcelFilesRegex = new Regex(@"^\S*/(?<container>[^/]+)/(?<pathInContainer>(?<bottlerName>[^/]+)/landing-files)/(?<fullFilename>(?<filenameWithoutExtension>[^/]+)\.xlsx)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);
        static readonly Regex landingZipFilesRegex = new Regex(@"^\S*/(?<container>[^/]+)/(?<pathInContainer>(?<bottlerName>[^/]+)/landing-files)/(?<fullFilename>(?<filenameWithoutExtension>[^/]+)\.zip)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        public string FullUrl { get; private set; }
        public string Filename { get; private set; }
        public string ContainerName { get; private set; }
        public string BottlerName { get; private set; }
        public string FullFilename { get; private set; }
        public string FullPathToFolderWithinContainer { get; private set; }
        public string FactType { get; private set; }
        public string FileType { get; private set; }
                
        public static NonNSRBlobfileAttributes ParseLandingFile(string fullUri)
        {
            var regexMatch = landingFilesRegex.Match(fullUri);
            return regexMatch.Success ? GroupsToObject(regexMatch.Groups) : null;
        }

        public static NonNSRBlobfileAttributes ParseNSRFile(string fullUri)
        {
            var regexMatch = nonNSRFilesRegex.Match(fullUri);

            NonNSRBlobfileAttributes retVal = null;
            if (regexMatch.Success)
            {
                retVal = GroupsToObject(regexMatch.Groups);

                var splitFilename = retVal.Filename.Split('_');

                if (splitFilename.Length > 1)
                {   // If we can't split & pull out fact & filetype, not a valid NSR file
                    retVal.FactType = splitFilename[splitFilename.Length - 2];
                    retVal.FileType = $@"{splitFilename[splitFilename.Length - 2]}_{splitFilename.Last()}";
                }
            }

            return retVal;
        }

        public static NonNSRBlobfileAttributes ParseLandingZipFile(string fullUri)
        {
            var regexMatch = landingZipFilesRegex.Match(fullUri);
            return regexMatch.Success ? GroupsToObject(regexMatch.Groups) : null;
        }

        public static NonNSRBlobfileAttributes ParseLandingExcelFile(string fullUri)
        {
            var regexMatch = landingExcelFilesRegex.Match(fullUri);
            return regexMatch.Success ? GroupsToObject(regexMatch.Groups) : null;
        }

        private static NonNSRBlobfileAttributes GroupsToObject(GroupCollection groups)
        {
            return new NonNSRBlobfileAttributes
            {
                FullUrl = groups[0].Value,
                FullPathToFolderWithinContainer = groups[@"pathInContainer"].Value,
                ContainerName = groups[@"container"].Value,
                BottlerName = groups[@"bottlerName"].Value,
                FullFilename = groups[@"fullFilename"].Value,
                Filename = groups[@"filenameWithoutExtension"].Value,
            };
        }

    }
}
