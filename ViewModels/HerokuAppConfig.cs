using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct HerokuAppConfig
    {
        public ResourceViewModel resource { get; set; }

        public List<AddonInfo> addons { get; set; }

        public Dictionary<string,string> config_vars { get; set; }
    }
}
