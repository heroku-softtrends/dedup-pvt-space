using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AddonInfo
    {
        public List<AddonAction> actions { get; set; }
        public AddonService addon_service { get; set; }
        public AddonApp app { get; set; }
        public AddonBillingPrice billed_price { get; set; }
        public List<string> config_vars { get; set; }
        public DateTime created_at { get; set; }
        public string id { get; set; }
        public string name { get; set; }
        public AddonPlan plan { get; set; }
        public DateTime updated_at { get; set; }
        public string provider_id { get; set; }
        public string state { get; set; }
        public string web_url { get; set; }
    }
}
