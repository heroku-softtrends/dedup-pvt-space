using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct VendorAppInfo
    {
        public string id { get; set; }
        public string callback_url { get; set; }
        public dynamic config { get; set; }
        public List<string> domains { get; set; }
        public string log_input_url { get; set; }
        public string name { get; set; }
        public string plan { get; set; }
        public string owner_email { get; set; }
        public AppOwner owner { get; set; }
        public string region { get; set; }
        public AddonResource resource { get; set; }
    }
}
