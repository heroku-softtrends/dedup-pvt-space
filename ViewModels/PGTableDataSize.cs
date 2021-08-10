using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class PGTableDataSize
    {
        public long total_rows { set; get; }
        public long row_data_size { set; get; }
    }
}
