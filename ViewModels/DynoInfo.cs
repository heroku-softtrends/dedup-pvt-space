using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct DynoInfo
    {
        public string attach_url { get; set; }
        public string command { get; set; }
        public DateTime created_at { get; set; }
        public string id { get; set; }
        public string name { get; set; }
        public AppRelease release { get; set; }
        public AddonApp app { get; set; }
        public string size { get; set; }
        public string state { get; set; }
        public string type { get; set; }
        public DateTime updated_at { get; set; }
    }
}
