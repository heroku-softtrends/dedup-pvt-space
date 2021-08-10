using System;
using System.Linq;
using Dedup.Models;
using Dedup.Data;
using Microsoft.EntityFrameworkCore;
using Dedup.Extensions;
using Dedup.ViewModels;
using Newtonsoft.Json;
using System.Reflection;
using System.Collections.Generic;
using Dedup.Common;

namespace Dedup.Repositories
{
    public class DeDupSettingsRepository : IDeDupSettingsRepository
    {
        private DeDupContext _context;

        public DeDupSettingsRepository(DeDupContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Method: Reload
        /// Description: It is used to reload DeDupSettings entity
        /// </summary>
        /// <param name="entity"></param>
        public void Reload(DeDupSettings entity)
        {
            _context.Entry<DeDupSettings>(entity).GetDatabaseValues();
        }

        /// <summary>
        /// Method: Add
        /// Description: It is used to add new DeDupSettings to DeDupSettings table
        /// </summary>
        /// <param name="item"></param>
        public void Add(DeDupSettings item)
        {
            _context.DeDupSettings.Add(item);
            _context.SaveChanges();
        }

        /// <summary>
        /// Method: Find
        /// Description: It is used to get DeDupSettings by ccid
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public DeDupSettings Find(string key)
        {
            if (_context.DeDupSettings.Where(r => r.ccid == key).Count() > 0)
            {
                var entity = _context.DeDupSettings.Where(r => r.ccid == key).FirstOrDefault();
                //get latest database value
                Reload(entity);
                return entity;
            }
            else
                return null;
        }

        /// <summary>
        /// Method: Get
        /// Description: It is used to get DeDupSettings/DatabaseConfig by ccid
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="databaseType"></param>
        /// <returns>DeDupSettings/DatabaseConfig Instance</returns>
        public T Get<T>(string key, DatabaseType databaseType = DatabaseType.None) where T : class
        {
            //Get DeDupSettings by ccid
            var entity = Find(key);
            //Return requested model instance
            return entity.ToModel<T>(key, databaseType);
        }

        /// <summary>
        /// Method: Remove
        /// Description: It is used to delete DeDupSettings by ccid from DeDupSettings table
        /// </summary>
        /// <param name="id"></param>
        public void Remove(string id)
        {
            //Get DeDupSettings by ccid
            var entity = Find(id);
            if (entity != null)
            {
                _context.Entry(entity).State = EntityState.Deleted;
                _context.SaveChanges();
            }
        }

        /// <summary>
        /// Method: Update
        /// Description: It is used to update DeDupSettings by ccid in DeDupSettings table
        /// </summary>
        /// <param name="item"></param>
        public void Update(DeDupSettings item)
        {
            //Get DeDupSettings by ccid
            var entity = Find(item.ccid);
            if (entity != null)
            {
                entity.database_config_json = item.database_config_json;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
        }

        /// <summary>
        /// Method: AddEditDatabaseConfig
        /// Description: It is used to insert/update database config by ccid
        /// </summary>
        /// <param name="databaseSetting"></param>
        public void AddEditDatabaseConfig(DatabaseConfig databaseSetting)
        {
            //Get DeDupSettings by ccid
            var entity = Find(databaseSetting.ccid);
            var isNew = false;
            if (entity == null)
            {
                isNew = true;
                //Create new DeDupSettings and assign ccid
                entity = new DeDupSettings() { ccid = databaseSetting.ccid };
            }

            List<DatabaseConfig> dbConfigs = entity.ToModel<List<DatabaseConfig>>();
            if (dbConfigs != null && dbConfigs.FirstOrDefault(p => p.databaseType == databaseSetting.databaseType) != null)
            {
                dbConfigs.FirstOrDefault(p => p.databaseType == databaseSetting.databaseType).syncDefaultDatabaseUrl = databaseSetting.syncDefaultDatabaseUrl;
               // dbConfigs.FirstOrDefault(p => p.databaseType == databaseSetting.databaseType).syncFollowerDatabaseUrl = databaseSetting.syncFollowerDatabaseUrl;
            }
            else
            {
                if (dbConfigs == null)
                {
                    dbConfigs = new List<DatabaseConfig>();
                }
                dbConfigs.Add(databaseSetting);
            }

            //Assign database config as json
            entity.database_config_json = JsonConvert.SerializeObject(dbConfigs);

            //Add/Update DeDupSettings entity
            if (isNew)
                _context.DeDupSettings.Add(entity);
            else
                _context.Entry(entity).State = EntityState.Modified;

            _context.SaveChanges();
        }

        /// <summary>
        /// Method: DeleteDatabaseConfig
        /// Description: It is used to delete database config by ccid
        /// </summary>
        /// <param name="databaseSetting"></param>
        public void DeleteDatabaseConfig(DatabaseType databaseType, string key)
        {
            //Get DeDupSettings by ccid
            var entity = Find(key);
            if (entity!= null)
            {
                List<DatabaseConfig> dbConfigs = entity.ToModel<List<DatabaseConfig>>();
                if (dbConfigs != null)
                {
                    //Get all db-config except deleting one
                    dbConfigs = dbConfigs.Where(p => p.databaseType != databaseType).ToList();
                    
                    //Assign database config as json
                    entity.database_config_json = JsonConvert.SerializeObject(dbConfigs);

                    //Update DeDupSettings entity
                    _context.Entry(entity).State = EntityState.Modified;
                    _context.SaveChanges();
                }
            }
        }

        /// <summary>
        /// Method: AddEditDatabaseConfig
        /// Description: It is used to insert/update database config property by ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="fieldName"></param>
        /// <param name="fieldValue"></param>
        public void AddEditDatabaseConfig(string ccid, string fieldName, string fieldValue)
        {
            var isNew = false;
            //Get DeDupSettings by ccid
            var entity = Find(ccid);

            //Get DatabaseConfig by ccid
            var dbConfig = entity.ToModel<DatabaseConfig>(ccid);
            if (entity == null)
            {
                isNew = true;
                //Create new DeDupSettings and assign ccid
                entity = new DeDupSettings() { ccid = ccid };
            }

            //Update DatabaseConfig property value by name
            if (dbConfig != null && dbConfig.GetType().GetProperty(fieldName) != null)
            {
                var propertyInfo = dbConfig.GetType().GetProperty(fieldName);
                propertyInfo.SetValue(dbConfig, fieldValue, null);
            }
            //Assign database config as json
            entity.database_config_json = JsonConvert.SerializeObject(dbConfig);

            //Add/Update DeDupSettings entity
            if (isNew)
                _context.DeDupSettings.Add(entity);
            else
                _context.Entry(entity).State = EntityState.Modified;
            _context.SaveChanges();
        }

        /// <summary>
        /// Method: GetHerokuAppName
        /// Description: It is used to get herokuId of main app by resourceId from resources table
        /// </summary>
        /// <param name="ccid"></param>
        /// <returns></returns>
        public string GetHerokuAppName(string ccid)
        {
            if (_context.Resources.Where(r => r.uuid == ccid).Count() > 0)
            {
                var entity = _context.Resources.FirstOrDefault(r => r.uuid == ccid);
                return string.IsNullOrEmpty(entity.app_name) ? entity.heroku_id : entity.app_name;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Method: GetResource
        /// Description: It is used to get resource by resourceId from resources table
        /// </summary>
        /// <param name="uuid"></param>
        /// <returns>Resources</returns>
        public Resources GetResource(string uuid)
        {
            if (_context.Resources.Where(r => r.uuid == uuid).Count() > 0)
                return _context.Resources.Where(r => r.uuid == uuid).FirstOrDefault();
            else
                return null;
        }

        private bool _disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _context.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~DeDupSettingsRepository()
        {
            Dispose(false);
        }
    }
}
