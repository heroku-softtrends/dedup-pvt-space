using Dedup.Common;
using Dedup.Models;
using Dedup.ViewModels;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Dedup.Extensions
{
    public static class DeDupSettingExtensions
    {
        public static T ToModel<T>(this DeDupSettings dedupSetting, string ccid = "", DatabaseType databaseType = DatabaseType.None) where T : class
        {
            if (dedupSetting != null && string.IsNullOrEmpty(ccid))
            {
                ccid = dedupSetting.ccid;
            }

            if (typeof(T) == typeof(DeDupSettings))
            {
                return dedupSetting as T;
            }
            if (typeof(T) == typeof(DatabaseConfig))
            {
                DatabaseConfig databaseSetting = default(DatabaseConfig);
                if (dedupSetting != null && !string.IsNullOrEmpty(dedupSetting.database_config_json))
                {
                    try
                    {
                        databaseSetting = JsonConvert.DeserializeObject<List<DatabaseConfig>>(dedupSetting.database_config_json).FirstOrDefault(p => p.databaseType == databaseType);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("ERROR: {0}", ex.Message);
                        if (dedupSetting.database_config_json.Contains("databaseType") || databaseType == DatabaseType.Heroku_Postgres)
                        {
                            databaseSetting = JsonConvert.DeserializeObject<DatabaseConfig>(dedupSetting.database_config_json);
                        }
                    }
                }
                if (databaseSetting == null)
                {
                    databaseSetting = new DatabaseConfig();
                }
                databaseSetting.ccid = ccid;
                databaseSetting.databaseType = databaseType;
                if (databaseSetting.databaseType == DatabaseType.None)
                {
                    databaseSetting.databaseType = DatabaseType.Heroku_Postgres;
                }
                return databaseSetting as T;
            }
            if (typeof(T) == typeof(List<DatabaseConfig>))
            {
                List<DatabaseConfig> databaseSettings = default(List<DatabaseConfig>);
                if (dedupSetting != null && !string.IsNullOrEmpty(dedupSetting.database_config_json))
                {
                    try
                    {
                        databaseSettings = JsonConvert.DeserializeObject<List<DatabaseConfig>>(dedupSetting.database_config_json);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("ERROR: {0}", ex.Message);
                        databaseSettings = new List<DatabaseConfig>() { JsonConvert.DeserializeObject<DatabaseConfig>(dedupSetting.database_config_json) };
                    }

                    if (databaseSettings != null)
                    {
                        databaseSettings = databaseSettings.Select(p =>
                        {
                            p.ccid = ccid;
                            if (p.databaseType == DatabaseType.None)
                            {
                                p.databaseType = DatabaseType.Heroku_Postgres;
                            }
                            return p;
                        }).ToList();
                    }
                }

                return databaseSettings as T;
            }
           
            return null;
        }
    }
}
