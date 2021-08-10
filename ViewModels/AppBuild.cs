using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppBuild
    {
        public HerokuApp app { get; set; }
        public List<AppBuildpack> buildpacks { get; set; }
        public DateTime created_at { get; set; }
        public string id { get; set; }
        public string output_stream_url { get; set; }
        public AppRelease release { get; set; }
        public AppSlug slug { get; set; }
        public AppSourceBlob source_blob { get; set; }
        public string stack { get; set; }
        public string status { get; set; }
        public string updated_at { get; set; }
        public AppOwner user { get; set; }
    }
}
