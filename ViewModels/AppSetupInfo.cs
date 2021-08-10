using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSetupInfo
    {
        public string id { get; set; }
        public DateTime created_at { get; set; }
        public DateTime updated_at { get; set; }
        public string status { get; set; }
        public string failure_message { get; set; }
        public HerokuApp app { get; set; }
        public AppSetupBuild build { get; set; }
        public List<string> manifest_errors { get; set; }
        public IDictionary<string, object> postdeploy { get; set; }
        public string resolved_success_url { get; set; }

    }
}
