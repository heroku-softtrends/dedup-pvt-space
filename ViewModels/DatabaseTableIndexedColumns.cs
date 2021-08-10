using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct DatabaseTableIndexedColumns
    {
        public string table_name { get; set; }
        public string index_name { get; set; }
        public string column_names { get; set; }

        public bool is_index_on_multiple_columns
        {
            get
            {
                return column_names.Contains(",") ? true : false;
            }
        }
    }
}
