using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct CreateAppBuild
    {
        public List<AppBuildpack> buildpacks { get; set; }
        public AppSourceBlob source_blob { get; set; }
    }
}
