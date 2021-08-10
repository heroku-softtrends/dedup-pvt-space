using Dedup.Common;
using Dedup.Repositories;
using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Runtime;
using System.Threading.Tasks;
using System.Web;

namespace Dedup.Extensions
{
    public static class DictionaryExtensions
    {
        public static T ToConvert<T>(this Dictionary<string, string> objDict) where T : class
        {
            if (objDict == null)
                return null;

            if (typeof(T) == typeof(ExpandoObject))
            {
                var eObj = new ExpandoObject();
                var eObjColl = (ICollection<KeyValuePair<string, object>>)eObj;
                foreach (var kvp in objDict)
                {
                    eObjColl.Add(new KeyValuePair<string, object>(kvp.Key, kvp.Value));
                }
                return eObjColl as T;
            }

            return null;
        }

        public static object With(this IDictionary<string, string> objDic, List<SyncObjectColumn> dataExtnCols)
        {
            SyncObjectColumn dataExnCol = null;
            IDictionary<string, object> eo = new ExpandoObject() as IDictionary<string, object>;

            foreach (var name in objDic.Keys)
            {
                dataExnCol = dataExtnCols.Where(p => p.name.ToLower() == name.ToLower()).FirstOrDefault();
                if (dataExnCol != null)
                {
                    switch (dataExnCol.fieldType.ToLower())
                    {
                        case "date":
                        case "datetime":
                            eo.Add(name, (string.IsNullOrEmpty(objDic[name]) ? default(DateTime) : Convert.ToDateTime(objDic[name])));
                            break;
                        case "number":
                            eo.Add(name, (string.IsNullOrEmpty(objDic[name]) ? default(int) : Convert.ToInt32(objDic[name])));
                            break;
                        case "long":
                            eo.Add(name, (string.IsNullOrEmpty(objDic[name]) ? default(long) : Convert.ToInt64(objDic[name])));
                            break;
                        case "float":
                        case "double":
                        case "decimal":
                            eo.Add(name, (string.IsNullOrEmpty(objDic[name]) ? default(double) : Convert.ToDouble(objDic[name])));
                            break;
                        case "boolean":
                            eo.Add(name, (string.IsNullOrEmpty(objDic[name]) ? default(bool) : Convert.ToBoolean(objDic[name])));
                            break;
                        default:
                            eo.Add(name, string.IsNullOrEmpty(objDic[name]) ? null : objDic[name]);
                            break;
                    }
                }
                else
                {
                    eo.Add(name, string.IsNullOrEmpty(objDic[name]) ? null : objDic[name]);
                }
            }

            return eo;
        }

        public static void AddMultiple(this List<IDictionary<string, object>> objDicLst, List<IDictionary<string, object>> items)
        {
            if (objDicLst != null && objDicLst.Count() > 0)
            {
                objDicLst.ClearMemory();
            }
            if (items != null && items.Count() > 0)
            {
                if (objDicLst == null)
                {
                    objDicLst = new List<IDictionary<string, object>>(items);
                }
                objDicLst.AddRange(items);
            }
        }

        public static void ClearMemory<T>(this List<T> objLst)
        {
            int identificador = GC.GetGeneration(objLst);
            objLst.Clear();

            GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
            GC.Collect(identificador, GCCollectionMode.Forced);
            GC.WaitForPendingFinalizers();
        }

        public static IDictionary<K, V> ResetByDefaultValues<K, V>(this IDictionary<K, V> dic)
        {
            dic.Keys.ToList().ForEach(x => dic[x] = default(V));
            return dic;
        }

        public static IDictionary<string, object> ResetValues(this IDictionary<string, object> dic)
        {
            dic.Keys.ToList().ForEach(x => dic[x] = null);
            return dic;
        }

        public static void ClearMemory(this List<IDictionary<string, object>> objLst, int currentCycle)
        {
            if (objLst != null)
            {
                objLst = objLst.Select(dic => dic.ResetValues()).ToList();
                objLst.Clear();
            }
            if (currentCycle % 10 == 0 && GC.GetTotalMemory(false) >= 67108864)
            {
                Console.WriteLine($"max allocated memory: {GC.GetTotalMemory(false)}");
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
            }
            if (currentCycle % 10 == 1 && GC.GetTotalMemory(false) >= 33554432)
            {
                Console.WriteLine($"max allocated memory: {GC.GetTotalMemory(false)}");
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
            }
            if (GC.GetTotalMemory(false) >= 20971520)
            {
                Console.WriteLine($"max allocated memory: {GC.GetTotalMemory(false)}");
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                Console.WriteLine($"Max allocated memory after GC.Collect: {GC.GetTotalMemory(false)}");
            }
        }

        //public static void AddMultiple(this List<IDictionary<string, object>> objDicLst, List<IDictionary<string, object>> items, int pageSize)
        //{
        //    if (objDicLst != null && objDicLst.Count() > 0)
        //    {
        //        objDicLst.Clear();
        //        //objDicLst.ClearMemory();
        //    }
        //    if (items != null && items.Count() > 0)
        //    {
        //        if (objDicLst == null)
        //        {
        //            objDicLst = new List<IDictionary<string, object>>(pageSize);
        //        }
        //        objDicLst.AddRange(items);
        //    }
        //}
        public static IEnumerable<Dictionary<string, object>> QueryDictionary(this IDbConnection connection, string query)
        {
            var data = Dapper.SqlMapper.Query(connection, query) as IEnumerable<Dictionary<string, object>>;
            return data;
            //return data.Select(r => r.ToDictionary(d => d.Key, d => d.Value));
        }

        public static dynamic GetDynamicObject(this Dictionary<string, object> properties)
        {
            var dynamicObject = new ExpandoObject() as IDictionary<string, Object>;
            foreach (var property in properties)
            {
                dynamicObject.Add(property.Key, property.Value);
            }
            return dynamicObject;
        }

        public static IDictionary<string, object> GetDynamicObject(this IEnumerable<KeyValuePair<string, object>> properties, List<string> NonEncodeDataTypes, List<SyncObjectColumn> DataExtnCols, bool isEncode = true)
        {
            var dicObj = new Dictionary<string, object>();
            for (int j = 0; j < properties.Count(); j++)
            {
                if (DataExtnCols.Where(c => c.name.ToLower().Trim() == properties.ElementAt(j).Key.ToLower().Trim() && NonEncodeDataTypes.Contains(c.fieldType.ToLower())).Count() > 0)
                {
                    dicObj.Add(properties.ElementAt(j).Key, properties.ElementAt(j).Value);
                }
                else if (DataExtnCols.Where(c => c.name.ToLower().Trim() == properties.ElementAt(j).Key.ToLower().Trim()).Count() > 0
                    && properties.ElementAt(j).Value != null)
                {
                    if (properties.ElementAt(j).Value == null)
                        dicObj.Add(properties.ElementAt(j).Key, null);
                    else
                        dicObj.Add(properties.ElementAt(j).Key, (isEncode ? HttpUtility.UrlEncode(properties.ElementAt(j).Value.ToString()) : HttpUtility.UrlDecode(properties.ElementAt(j).Value.ToString())));
                }
            }
            return dicObj;
        }
    }
}
