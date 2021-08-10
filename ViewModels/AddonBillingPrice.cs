using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AddonBillingPrice
    {
        public int cents { get; set; }
        public string unit { get; set; }
    }
}
