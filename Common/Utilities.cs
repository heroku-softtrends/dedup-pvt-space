using Dapper;
using Dedup.Extensions;
using Dedup.ViewModels;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

namespace Dedup.Common
{
    public static class Utilities
    {
        public static IServiceProvider AppServiceProvider { get; set; }

        public static ILoggerFactory AppLoggerFactory { get; set; }

        public static IWebHostEnvironment HostingEnvironment { get; set; }

        private static Random random = new Random();

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public static T GetGuid<T>() where T : class
        {
            if (typeof(T) == typeof(string))
            {
                return Guid.NewGuid().ToString() as T;
            }
            else
            {
                return Guid.NewGuid() as T;
            }
        }

        public static DynamicParameters DictionaryToDynamicParameters(Dictionary<string, string> objDict)
        {
            var dbArgs = new DynamicParameters();
            foreach (var pair in objDict) dbArgs.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? (object)DBNull.Value : (object)pair.Value));
            return dbArgs;
        }
       
        public static List<ExpandoObject> ToDynamicObjects(List<Dictionary<string, string>> listDict, List<SyncObjectColumn> dataExtnCols)
        {
            SyncObjectColumn dataExnCol = null;
            var eoLst = new List<ExpandoObject>();

            foreach (var dic in listDict)
            {
                IDictionary<string, object> eo = new ExpandoObject() as IDictionary<string, object>;
                foreach (var pair in dic)
                {
                    dataExnCol = dataExtnCols.Where(p => p.name.ToLower() == pair.Key.ToLower()).FirstOrDefault();
                    if (dataExnCol != null)
                    {
                        switch (dataExnCol.fieldType.ToLower())
                        {
                            case "date":
                            case "datetime":
                                eo.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? default(DateTime) : Convert.ToDateTime(pair.Value)));
                                break;
                            case "number":
                                eo.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? default(int) : Convert.ToInt32(pair.Value)));
                                break;
                            case "long":
                                eo.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? default(long) : Convert.ToInt64(pair.Value)));
                                break;
                            case "float":
                            case "double":
                            case "decimal":
                                eo.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? default(double) : Convert.ToDouble(pair.Value)));
                                break;
                            case "boolean":
                                eo.Add(pair.Key, (string.IsNullOrEmpty(pair.Value) ? default(bool) : Convert.ToBoolean(pair.Value)));
                                break;
                            default:
                                eo.Add(pair.Key, string.IsNullOrEmpty(pair.Value) ? null : pair.Value);
                                break;
                        }
                    }
                    else
                    {
                        eo.Add(pair.Key, string.IsNullOrEmpty(pair.Value) ? null : pair.Value);
                    }
                }
                eoLst.Add((dynamic)eo);
            }
            return eoLst;
        }

        public static string GetEnvVarVal(string pKey)
        {
            return Environment.GetEnvironmentVariable(pKey);
        }

        public static string SHA1HashStringForUTF8String(string pStr)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(pStr);

            var sha1 = SHA1.Create();
            byte[] hashBytes = sha1.ComputeHash(bytes);

            return HexStringFromBytes(hashBytes);
        }

        public static string HexStringFromBytes(byte[] bytes)
        {
            var sb = new StringBuilder();
            foreach (byte b in bytes)
            {
                var hex = b.ToString("x2");
                sb.Append(hex);
            }
            return sb.ToString();
        }

        public static long ConvertToUnixTime(DateTime datetime)
        {
            DateTime sTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            return (long)(datetime - sTime).TotalSeconds;
        }

        public static string RemoveSpecialChars(string strText)
        {
            return Regex.Replace(strText, @"[^0-9a-zA-Z\._-]", string.Empty);
        }
		
		public static string RemoveSpecialCharsIncludeDash(string strText)
        {
            return Regex.Replace(strText, @"[^0-9a-zA-Z\._]", string.Empty);
        }

        public static string Base64Encode(string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(plainText);
            return System.Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String(base64EncodedData);
            return System.Text.Encoding.UTF8.GetString(base64EncodedBytes);
        }
        public static string EncryptText(string stringToEncrypt)
        {
            string encryptedValue = string.Empty;
            try
            {
                using (var aesAlg = Aes.Create())
                {
                    Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(Constants.AES_KEY, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                    aesAlg.Key = pdb.GetBytes(32);
                    aesAlg.IV = pdb.GetBytes(16);
                    byte[] src = Encoding.Unicode.GetBytes(stringToEncrypt);
                    using (ICryptoTransform encrypt = aesAlg.CreateEncryptor())
                    {
                        byte[] dest = encrypt.TransformFinalBlock(src, 0, src.Length);
                        encryptedValue = Convert.ToBase64String(dest);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return encryptedValue;
        }

        public static string DecryptText(string encryptedText)
        {
            string decryptedValue = string.Empty;
            try
            {
                using (var aesAlg = Aes.Create())
                {
                    Rfc2898DeriveBytes pdb = new Rfc2898DeriveBytes(Constants.AES_KEY, new byte[] { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 });
                    aesAlg.Key = pdb.GetBytes(32);
                    aesAlg.IV = pdb.GetBytes(16);
                    var src = Convert.FromBase64String(encryptedText);
                    using (ICryptoTransform decrypt = aesAlg.CreateDecryptor())
                    {
                        byte[] dest = decrypt.TransformFinalBlock(src, 0, src.Length);
                        decryptedValue = Encoding.Unicode.GetString(dest);
                    }
                }
            }
            catch (CryptographicException e)
            {
                Console.WriteLine(e.Message);
            }
            return decryptedValue;
        }

        public static bool IsPrivatePlanByLevel(string planLevel)
        {
            var addonPlanInfo = GetAddonPlanByLevel(planLevel);
            if (!addonPlanInfo.IsNull() && addonPlanInfo.is_private_space)
                return true;

            return false;
        }

        public static bool IsAddonPrivatePlan(string planLevel)
        {
            bool isPrivate = false;
            if (!string.IsNullOrEmpty(planLevel))
            {
                try
                {
                    if (ConfigVars.Instance.addonPlans != null && ConfigVars.Instance.addonPlans.FirstOrDefault(p => p.level.ToString() == planLevel).IsNull() == false)
                    {
                        isPrivate = ConfigVars.Instance.addonPlans.FirstOrDefault(p => p.level.ToString() == planLevel).is_private_space;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
            return isPrivate;
        }

        public static PlanInfos GetAddonPlanByLevel(string planLevel)
        {
            PlanInfos planInfos = default(PlanInfos);
            if (!string.IsNullOrEmpty(planLevel))
            {
                try
                {
                    if (ConfigVars.Instance.addonPlans != null && ConfigVars.Instance.addonPlans.Where(p => p.level.ToString() == planLevel).Count() > 0)
                    {
                        planInfos = ConfigVars.Instance.addonPlans.FirstOrDefault(p => p.level.ToString() == planLevel);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
            return planInfos;
        }

        public static PlanInfos GetAddonPlanByPlan(string plan)
        {
            PlanInfos planInfos = default(PlanInfos);
            if (!string.IsNullOrEmpty(plan))
            {
                try
                {
                    if (ConfigVars.Instance.addonPlans != null && ConfigVars.Instance.addonPlans.Where(p => p.name.ToLower().Trim() == plan.ToLower().Trim()).Count() > 0)
                    {
                        planInfos = ConfigVars.Instance.addonPlans.Where(p => p.name.ToLower().Trim() == plan).FirstOrDefault();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            return planInfos;
        }

        public static T GetJsonPropertyValueByKeyPath<T>(string jsonString, string keyPath) where T : class
        {
            try
            {
                if (string.IsNullOrEmpty(jsonString))
                {
                    return null;
                }

                if (string.IsNullOrEmpty(keyPath))
                {
                    if (typeof(T) == typeof(List<string>))
                    {
                        List<string> strLst = JsonConvert.DeserializeObject<T>(jsonString) as List<string>;
                        strLst = strLst.Select(s => HttpUtility.UrlDecode(s)).ToList();
                        return strLst as T;
                    }
                }
                else
                {
                    if (typeof(T) == typeof(string))
                    {
                        return ((string)JObject.Parse(jsonString).SelectToken(keyPath)) as T;
                    }
                    else if (typeof(T) == typeof(List<string>))
                    {
                        return JObject.Parse(jsonString).SelectToken(keyPath).ToList() as T;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            return null;
        }

        static public String GenerateGuid(String name)
        {
            byte[] buf = Encoding.UTF8.GetBytes(name);
            byte[] guid = new byte[16];
            if (buf.Length < 16)
            {
                Array.Copy(buf, guid, buf.Length);
            }
            else
            {
                using (SHA1 sha1 = SHA1.Create())
                {
                    byte[] hash = sha1.ComputeHash(buf);
                    // Hash is 20 bytes, but we need 16. We loose some of "uniqueness", but I doubt it will be fatal
                    Array.Copy(hash, guid, 16);
                }
            }

            // Don't use Guid constructor, it tends to swap bytes. We want to preserve original string as hex dump.
            String guidS = "{" + String.Format("{0:X2}{1:X2}{2:X2}{3:X2}-{4:X2}{5:X2}-{6:X2}{7:X2}-{8:X2}{9:X2}-{10:X2}{11:X2}{12:X2}{13:X2}{14:X2}{15:X2}",
                guid[0], guid[1], guid[2], guid[3], guid[4], guid[5], guid[6], guid[7], guid[8], guid[9], guid[10], guid[11], guid[12], guid[13], guid[14], guid[15]) + "}";

            return guidS;
        }

        public static Guid ConvertToMd5HashGUID(string value)
        {
            // convert null to empty string - null can not be hashed
            if (value == null)
                value = string.Empty;

            // get the byte representation
            var bytes = Encoding.UTF8.GetBytes(value);

            // create the md5 hash
            MD5 md5Hasher = MD5.Create();
            byte[] data = md5Hasher.ComputeHash(bytes);

            // convert the hash to a Guid
            return new Guid(data);
        }

        public static Guid ToGuid(int value)
        {
            byte[] bytes = new byte[16];
            BitConverter.GetBytes(value).CopyTo(bytes, 0);
            return new Guid(bytes);
        }

        public static int ToInt(this Guid value)
        {
            byte[] bytes = value.ToByteArray();
            int iGuid = ((int)bytes[0]) | ((int)bytes[1] << 8) | ((int)bytes[2] << 16) | ((int)bytes[3] << 24);
            return iGuid;
        }

        public static string GetHerokuStackVersion(this HerokuStackVersion HerokuStackVersion)
        {
            // get the field 
            var field = HerokuStackVersion.GetType().GetField(HerokuStackVersion.ToString());
            var customAttributes = field.GetCustomAttributes(typeof(DescriptionAttribute), false);
            if (customAttributes.Count() > 0)
            {
                return (customAttributes.ElementAt(0) as DescriptionAttribute).Description;
            }
            else
            {
                return HerokuStackVersion.ToString();
            }
        }

        public static string ToBase64Encoding(this string pData)
        {
            try
            {
                byte[] bytes = Encoding.UTF8.GetBytes(pData);
                return Convert.ToBase64String(bytes);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }
            return null;
        }

        public static string ToBase64Decoding(this string pData)
        {
            try
            {
                byte[] bytes = Convert.FromBase64String(pData);
                return Encoding.UTF8.GetString(bytes);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }
            return null;
        }

        public static IQueryable<T> GetPagingResult<T, TResult>(this IQueryable<T> pQuery, int pPageNum, int pPageSize,
        Expression<Func<T, TResult>> pOrderByProperty, bool pIsAscendingOrder, out int pRowsCount)
        {
            if (pPageSize <= 0) pPageSize = 1;

            //Total result count
            pRowsCount = pQuery.Count();

            //If page number should be > 0 else set to first page
            if (pRowsCount <= pPageSize || pPageNum <= 0) pPageNum = 1;

            //Calculate number of rows to skip on pagesize
            int excludedRows = (pPageNum - 1) * pPageSize;

            pQuery = pIsAscendingOrder ? pQuery.OrderBy(pOrderByProperty) : pQuery.OrderByDescending(pOrderByProperty);

            //Skip the required rows and take the next records of pagesize count
            return pQuery.Skip(excludedRows).Take(pPageSize);
        }

        public static DateTime ConvertTimeBySystemTimeZoneId(DateTime date, string sourceTimeZoneId = "", string destTimeZoneId = "UTC")
        {
            try
            {
                if (!string.IsNullOrEmpty(sourceTimeZoneId) && !string.IsNullOrEmpty(destTimeZoneId))
                    return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date, sourceTimeZoneId, destTimeZoneId);
                else if (string.IsNullOrEmpty(destTimeZoneId))
                    return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(date, destTimeZoneId);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }

            return date;
        }
    }
}
