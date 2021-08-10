using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Extensions
{
    public static class StructExtensions
    {
        public static bool IsNull<T>(this T source) where T : struct
        {
            return source.Equals(default(T));
        }
    }
}
