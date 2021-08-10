using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSetupAppInfo
    {
        public bool locked { get; set; }
        public string name { get; set; }
        public string organization { get; set; }
        public bool personal { get; set; }
        public string region { get; set; }
        public string space { get; set; }
        public string stack { get; set; }
    }
}
