using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace NsrFunctions
{
    class UISetBlobfileAttributes : BottlerBlobfileAttributes
    {
        static readonly Regex blobUrlRegexExtractForOtherType = new Regex(@"^\S*/(?<container>[^/]+)/(?<bottler>[^/]+)/valid-set-files/(?<filename>(?<filenamewithoutextension>(\w+(?=_))?_?(?<datetime>[\d]+_[\d]+)_(?<filetype>[^\.]+))\.csv)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        static readonly Regex blobUrlRegexExtractForBottlerExchangeFiles = new Regex(@"^\S*/(?<container>[^/]+)/auto-curr-ntrl/valid-set-files/(?<filename>(?<filenamewithoutextension>([\w\-]+(?=_))?_?(?<datetime>[\d]+_[\d]+)_(?<filetype>[^\.]+))\.csv)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        public static UISetBlobfileAttributes Parse(string fullUri)
        {
            UISetBlobfileAttributes retVal = null;
            var regexMatch = blobUrlRegexExtractForOtherType.Match(fullUri);
            if (regexMatch.Success)
            {
                retVal = new UISetBlobfileAttributes
                {
                    FullUrl = regexMatch.Groups[0].Value,
                    ContainerName = regexMatch.Groups[@"container"].Value,
                    BottlerName = regexMatch.Groups[@"bottler"].Value,
                    Subfolder = regexMatch.Groups[@"bottler"].Value,
                    Filename = regexMatch.Groups[@"filename"].Value,
                    FilenameWithoutExtension = regexMatch.Groups[@"filenamewithoutextension"].Value,
                    BatchDateTime = DateTime.ParseExact(regexMatch.Groups[@"datetime"].Value, @"yyyyMMdd_HHmmss", CultureInfo.InvariantCulture),
                };

                if (!Helpers.IsCurrencyNeutralFile(retVal.Filename))                      
                {
                    using (var sqlClient = new NsrSqlClient())
                    {
                        var srcSysId = retVal.GetSrcSysId(sqlClient);
                        retVal.Filetype = sqlClient.GetFileTypeForUiFile(srcSysId);
                    }
                }
            }

            return retVal;
        }

        public static UISetBlobfileAttributes ParseExchangeRateFile(string fullUri)
        {
            UISetBlobfileAttributes retVal = null;
            var regexMatch = blobUrlRegexExtractForBottlerExchangeFiles.Match(fullUri);
            if (regexMatch.Success)
            {
                retVal = new UISetBlobfileAttributes
                {
                    FullUrl = regexMatch.Groups[0].Value,
                    ContainerName = regexMatch.Groups[@"container"].Value,
                    BottlerName = "auto-curr-ntrl",
                    Subfolder = "auto-curr-ntrl",
                    Filename = regexMatch.Groups[@"filename"].Value,
                    FilenameWithoutExtension = regexMatch.Groups[@"filenamewithoutextension"].Value,
                    BatchDateTime = DateTime.ParseExact(regexMatch.Groups[@"datetime"].Value, @"yyyyMMdd_HHmmss", CultureInfo.InvariantCulture),
                    FiletypePrefix = regexMatch.Groups[@"filetype"].Value
                };

                using (var sqlClient = new NsrSqlClient())
                {
                    var srcSysId = retVal.GetSrcSysIdForFileType(sqlClient);
                    retVal.FactType = sqlClient.GetFactTypeForSrcId(srcSysId);
                    retVal.Filetype = sqlClient.GetFileTypeForUiFile(srcSysId);
                }                
            }

            return retVal;
        }
    }
}
