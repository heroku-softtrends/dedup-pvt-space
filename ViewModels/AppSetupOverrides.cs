using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSetupOverrides
    {
        public List<AppBuildpack> buildpacks { get; set; }
        public AppConfigvars env { get; set; }
    }
}
