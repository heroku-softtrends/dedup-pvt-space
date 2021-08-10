using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct AppSpace
    {
        public Guid id { get; set; }
        public string name { get; set; }
        public DateTime created_at { get; set; }
        public AppOrganization? organization { get; set; }
        public AppTeam? team { get; set; }
        public AppRegion region { get; set; }
        public bool shield { get; set; }
        public string state { get; set; }
        public DateTime updated_at { get; set; }
    }
}
