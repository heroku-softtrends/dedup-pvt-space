using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSetupBuild
    {
        public string id { get; set; }
        public string status { get; set; }
        public string output_stream_url { get; set; }
    }
}
