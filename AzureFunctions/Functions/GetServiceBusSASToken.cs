using System;
using System.Net;
using System.Net.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus;
using Newtonsoft.Json;

namespace NsrFunctions
{
    public static class GetServiceBusSASToken
    {
        public class SasTokenRequest
        {
            [JsonProperty(@"namespace")]
            public string Namespace { get; set; }

            [JsonProperty(@"path")]
            public string Path { get; set; }

            [JsonProperty(@"keyname")]
            public string KeyName { get; set; }

            [JsonProperty(@"key")]
            public string Key { get; set; }
        }

        [FunctionName("GetServiceBusSASToken")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]SasTokenRequest req, TraceWriter log)
        {
            var uri = ServiceBusEnvironment.CreateServiceUri(@"https", req.Namespace, req.Path).ToString().Trim('/');
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(SharedAccessSignatureTokenProvider.GetSharedAccessSignature(req.KeyName, req.Key, uri, TimeSpan.FromDays(365 * 10))) };
        }
    }
}
