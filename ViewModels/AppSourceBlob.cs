using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSourceBlob
    {
        public string checksum { get; set; }
        public string url { get; set; }
        public string version { get; set; }
        [JsonIgnore]
        public string version_description { get; set; }
    }
}
