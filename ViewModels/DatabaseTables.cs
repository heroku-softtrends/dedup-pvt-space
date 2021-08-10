using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct DatabaseTables
    {
        public string dbSchema { get; set; }
        public string tableName { get; set; }
        public string customerKey { get; set; }
    }
}
