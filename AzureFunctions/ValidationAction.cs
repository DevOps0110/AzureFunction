using Microsoft.WindowsAzure.Storage.Blob;

namespace NsrFunctions
{
    internal class ValidationAction
    {
        public string Message { get; internal set; }
        public bool Failed { get; internal set; }
        public ICloudBlob Blob { get; internal set; }
    }
}