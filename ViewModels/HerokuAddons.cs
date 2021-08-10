using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct HerokuAddon
    {
        public string id { get; set; }

        public string name { get; set; }

        public Dictionary<string,string> addon_service { get; set; }

        public Dictionary<string, string> plan { get; set; }

        public Dictionary<string, string> app { get; set; }

        public string provider_id { get; set; }

        public string state { get; set; }

        public string web_url { get; set; }
    }
}
