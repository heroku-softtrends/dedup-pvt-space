using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Extensions
{
    public static class TempDataExtensions
    {
        public static void PutObject<T>(this ITempDataDictionary tempData, string key, T value) where T : class
        {
            tempData[key] = JsonConvert.SerializeObject(value);
        }

        public static void PutValue<T>(this ITempDataDictionary tempData, string key, T value) where T : struct
        {
            tempData[key] = JsonConvert.SerializeObject(value);
        }

        public static T GetObject<T>(this ITempDataDictionary tempData, string key) where T : class
        {
            object o;
            tempData.TryGetValue(key, out o);
            return o == null ? null : JsonConvert.DeserializeObject<T>((string)o);
        }

        public static T GetValue<T>(this ITempDataDictionary tempData, string key) where T : struct
        {
            object o;
            tempData.TryGetValue(key, out o);
            return o == null ? default(T) : JsonConvert.DeserializeObject<T>((string)o);
        }
    }
}
