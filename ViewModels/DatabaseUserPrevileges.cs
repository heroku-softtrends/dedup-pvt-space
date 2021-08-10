using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct DatabaseUserPrevileges
    {
        public string table_catalog { get; set; }
        public string table_schema { get; set; }
        public string table_name { get; set; }
        public string privilege_type { get; set; }
    }
}
