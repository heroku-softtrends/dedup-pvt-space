using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class CompareMultipleTables
    {
        public IList<IDictionary<string, object>> FinalResultSet { get; set; }
        public IList<IDictionary<string, object>> CompareResultSet { get; set; }
        public int TotalRecordCount { get; set; }
    }
}
