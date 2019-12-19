using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace NsrFunctions
{
    class BottlerBlobfileAttributes
    {
        static readonly Regex inboundBlobUrlRegex = new Regex(@"^\S*/([^/]+)/([^/]+)/inbound/(((([^_]+)_([\d]+_[\d]+)_([\w]+))_([\w]+))\.csv)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        static readonly Regex validSetBlobUrlRegex = new Regex(@"^\S*/([^/]+)/([^/]+)/valid-set-files/(((([^_]+)_([\d]+_[\d]+)_([\w]+))_([\w]+))\.csv)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

        private string _dbFiletype;
        private string _srcSysId;
        private int? _moduleId;

        public string FullUrl { get; protected set; }
        public string FilenameWithoutExtension { get; protected set; }
        public string BatchPrefix { get; protected set; }
        public DateTime BatchDateTime { get; protected set; }
        /// <summary>
        /// Gets the filetype. Eg: 'channel', 'product', 'header', 'transaction', etc
        /// </summary>
        public string Filetype { get; protected set; }
        /// <summary>
        /// Gets the bottler name.
        /// </summary>
        public string BottlerName { get; protected set; }
        public string ContainerName { get; protected set; }
        public string Subfolder { get; protected set; }
        /// <summary>
        /// Gets the filetype prefix. Eg: 'offdisc', 'volume', etc
        /// </summary>
        public string FiletypePrefix { get; protected set; }
        /// <summary>
        /// Gets the type of the fact. Eg: 'offdisc', 'volume', etc
        /// </summary>
        public string FactType { get; protected set; }
        public string Filename { get; protected set; }

        public string GetSrcSysIdForFileType(NsrSqlClient client = null)
        {
            if (string.IsNullOrEmpty(_srcSysId))
            {
                var disposeClient = client == null;
                client = client ?? new NsrSqlClient();

                _srcSysId = client.GetSourceSysIdForFileType(this.FiletypePrefix, this.ContainerName);

                if (disposeClient)
                {
                    client.Dispose();
                    client = null;
                }
            }

            return _srcSysId;
        }

        public string GetSrcSysId(NsrSqlClient client = null)
        {
            if (string.IsNullOrEmpty(_srcSysId))
            {
                var disposeClient = client == null;
                client = client ?? new NsrSqlClient();

                _srcSysId = client.GetSourceSysIdForBottlerName(this.BottlerName, this.ContainerName);

                if (disposeClient)
                {
                    client.Dispose();
                    client = null;
                }
            }

            return _srcSysId;
        }

        public void GetDbFactType(string factType = null, NsrSqlClient client = null)
        {
            if (string.IsNullOrEmpty(factType))
            {
                if (string.IsNullOrEmpty(this.FactType))
                {
                    var disposeClient = client == null;
                    client = client ?? new NsrSqlClient();

                    var srcSysId = GetSrcSysId(client);

                    var moduleId = GetModuleId(client);
                    if (moduleId > 2)
                    {   // volume, revenue, discount files have all this information
                        this.FactType = client.GetFactTypeForFile(srcSysId, this.Filename);
                    }


                    if (disposeClient)
                    {
                        client.Dispose();
                        client = null;
                    }
                }
            }
            else
            {
                this.FactType = factType;
            }
        }

        public string GetDbFiletype(NsrSqlClient client = null)
        {
            if (string.IsNullOrEmpty(_dbFiletype))
            {
                var disposeClient = client == null;
                client = client ?? new NsrSqlClient();

                var srcSysId = GetSrcSysId(client);

                var moduleId = GetModuleId(client);
                if (moduleId == 1 || moduleId == 2)
                {   // volume, revenue, discount files have all this information
                    _dbFiletype = client.GetTypeForBottler(srcSysId, this.FactType, this.Filetype);
                }
                else
                {
                    _dbFiletype = client.GetFileTypeForUiFile(srcSysId);

                }

                if (disposeClient)
                {
                    client.Dispose();
                    client = null;
                }
            }

            return _dbFiletype;
        }

        public int GetModuleId(NsrSqlClient client = null)
        {
            if (!_moduleId.HasValue)
            {
                var disposeClient = client == null;
                client = client ?? new NsrSqlClient();

                var srcSysId = GetSrcSysId(client);

                _moduleId = client.GetModuleIdForFile(srcSysId, this.Filename);

                if (disposeClient)
                {
                    client.Dispose();
                    client = null;
                }
            }

            return _moduleId.Value;
        }

        /// <summary>Parses the specified full URI.</summary>
        /// <param name="fullUri">The full URI.</param>
        /// <param name="subfolderTarget">The subfolder target.</param>
        /// <param name="requiredExtension">The required extension without the leading '.'</param>
        /// <returns></returns>
        public static BottlerBlobfileAttributes Parse(string fullUri, string subfolderTarget, string requiredExtension = @"csv")
        {
            Match regexMatch;

            // Use the precompiled common regexes whenever possible for performance
            if (subfolderTarget.Equals(@"inbound", StringComparison.OrdinalIgnoreCase) && requiredExtension.Equals(@"csv"))
            {
                regexMatch = inboundBlobUrlRegex.Match(fullUri);
            }
            else if (subfolderTarget.Equals(@"valid-set-files", StringComparison.OrdinalIgnoreCase) && requiredExtension.Equals(@"csv"))
            {
                regexMatch = validSetBlobUrlRegex.Match(fullUri);
            }
            else if (subfolderTarget.Equals(@"auto-curr-ntrl", StringComparison.OrdinalIgnoreCase) && requiredExtension.Equals(@"csv"))
            {
                try
                {
                    return UISetBlobfileAttributes.ParseExchangeRateFile(fullUri);
                }
                catch (Exception ex)
                {
                    return null;
                }
            }

            // otherwise fab one up based on the parameters to this method.
            else
            {
                regexMatch = Regex.Match(fullUri, $@"^\S*/([^/]+)/([^/]+)/{subfolderTarget}/(((([^_]+)_([\d]+_[\d]+)_([\w]+))_([\w]+))\.{requiredExtension})$", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            }

            return regexMatch.Success ? GroupsToObject(regexMatch.Groups) : null;
        }

        private static BottlerBlobfileAttributes GroupsToObject(GroupCollection groups)
        {
            return new BottlerBlobfileAttributes
            {
                FullUrl = groups[0].Value,
                ContainerName = groups[1].Value,
                Subfolder = groups[2].Value,
                Filename = groups[3].Value,
                FilenameWithoutExtension = groups[4].Value,
                BatchPrefix = groups[5].Value,
                BottlerName = groups[6].Value,
                BatchDateTime = DateTime.ParseExact(groups[7].Value, @"yyyyMMdd_HHmmss", CultureInfo.InvariantCulture),
                FiletypePrefix = groups[8].Value,
                Filetype = groups[9].Value,
                FactType = Helpers.GetFactType(groups[8].Value)
            };
        }
    }
}
