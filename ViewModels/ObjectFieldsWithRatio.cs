using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class ObjectFieldsWithRatio
    {
        public string Similarity_Value { get; set; }
        public string Similarity_Type { get; set; }
        public double? Similarity_Percent { get; set; }
    }
}
