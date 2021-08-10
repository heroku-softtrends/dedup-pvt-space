using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AddonAction
    {
        public string id { get; set; }
        public string label { get; set; }
        public string action { get; set; }
        public string url { get; set; }
        public bool requires_owner { get; set; }
    }
}
