using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSetups
    {
        public AppSetupAppInfo? app { get; set; }
        public AppSetupOverrides? overrides { get; set; }
        public AppSourceBlob source_blob { get; set; }
    }
}
