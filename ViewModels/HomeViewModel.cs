using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct HomeViewModel
    {
        public List<ConnectorConfig> ConnectorConfigs { get; set; }

        public PlanInfos CurrentPlan { get; set; }
    }
}
