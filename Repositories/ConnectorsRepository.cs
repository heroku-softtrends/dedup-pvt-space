using System;
using Dedup.ViewModels;
using Dedup.Data;
using Dedup.Models;
using System.Linq;
using System.Collections.Generic;
using Dedup.Extensions;
using Newtonsoft.Json;
using Microsoft.EntityFrameworkCore;
using System.Threading.Tasks;
using Dedup.Services;
using System.Reflection;
using Dedup.Common;
using System.Text;
using Dapper;

namespace Dedup.Repositories
{
    public class ConnectorsRepository : IConnectorsRepository
    {
        public DeDupContext _context;

        public ConnectorsRepository(DeDupContext context)
        {
            _context = context;
        }

        //public ConnectorsRepository()
        //{
        //    _context = (DeDupContext)Utilities.AppServiceProvider.GetService(typeof(DeDupContext));
        //}

        /// <summary>
        /// Method: GetMaxID
        /// Description: It is used to get max of connector id in connectors table by ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="dedupSettings"></param>
        /// <returns>max connector id as int</returns>
        private int GetMaxID(string ccid, ref DeDupSettings dedupSettings)
        {
            int connectorId = 0;
            if (_context.Connectors.Where(r => r.ccid == ccid).Count() > 0)
            {
                var entity = _context.Connectors.Where(r => r.ccid == ccid).OrderByDescending(r => r.connector_id).FirstOrDefault();
                _context.Entry(entity).Reference(e => e.DeDupSetting).Load();
                dedupSettings = entity.DeDupSetting;
                connectorId = entity.connector_id;
            }
            else if (_context.DeDupSettings.FirstOrDefault(r => r.ccid == ccid) != null)
            {
                dedupSettings = _context.DeDupSettings.FirstOrDefault(r => r.ccid == ccid);
            }
            return connectorId;
        }

        /// <summary>
        /// Method: Reload
        /// Description: It is used to reload entity and its reference entity
        /// </summary>
        /// <param name="entity"></param>
        private void Reload(Connectors entity)
        {
            if (entity != null)
            {
                _context.Entry(entity).Reload();
                _context.Entry(entity).Reference(e => e.DeDupSetting).Load();
            }
        }

        /// <summary>
        /// Method: Find
        /// Description: It is used to get connector entity by id and ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="id"></param>
        /// <returns>Connectors</returns>
        private Connectors Find(string ccid, int? id)
        {
            if (_context.Connectors.Where(r => r.ccid == ccid && r.connector_id == id).Count() > 0)
            {
                var entity = _context.Connectors.FirstOrDefault(r => r.ccid == ccid && r.connector_id == id);
                Reload(entity);
                return entity;
            }
            else
                return null;
        }

        /// <summary>
        /// Method: GetConnectorById
        /// Description: It is used to get connector entity by id and ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="id"></param>
        /// <param name="consumedProcessCount"></param>
        /// <returns></returns>
        public ConnectorConfig GetConnectorById(string ccid, int id, ref int consumedProcessCount)
        {
            if (_context.Connectors.Where(r => r.ccid == ccid).Count() > 0)
            {
                //get consumed process count
                consumedProcessCount = _context.Connectors.Where(r => r.ccid == ccid).Count();

                //get current connector
                var entity = _context.Connectors.Where(p => p.ccid == ccid).FirstOrDefault(p => id > 0 && p.connector_id == id);
                Reload(entity);
                if (entity != null)
                {
                    return entity.ToModel<ConnectorConfig>();
                }
            }

            return null;
        }

        /// <summary>
        /// Description: It is used to get connectors count by ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <returns></returns>
        public async Task<int> GetConnectorsCount(string ccid)
        {
            //get consumed process count
            int consumedProcessCount = _context.Connectors.Where(r => r.ccid == ccid).Count();

            return await Task.FromResult(consumedProcessCount);
        }

        /// <summary>
        /// Method: Get
        /// Description: It is used to get requested viewmodel instance by id and ccid
        /// <typeparam name="T"></typeparam>
        /// <param name="ccid"></param>
        /// <param name="id"></param>
        /// <returns>T</returns>
        public T Get<T>(string ccid, int? id, DatabaseType? databaseType, bool IsSetConfig = false) where T : class
        {
            if (id.HasValue && id > 0)
            {
                if (_context.Connectors.FirstOrDefault(r => r.ccid == ccid && r.connector_id == id) != null)
                {
                    var entity = _context.Connectors.FirstOrDefault(r => r.ccid == ccid && r.connector_id == id);
                    Reload(entity);

                    if (typeof(T) == typeof(ConnectorConfig))
                    {
                        return entity.ToModel<T>(IsSetConfig) as T;
                    }
                    else if (typeof(T) == typeof(List<ConnectorConfig>))
                    {
                        return entity.ToModel<T>(IsSetConfig) as T;
                    }
                    else
                    {
                        return entity as T;
                    }
                }
            }
            else if (typeof(T) == typeof(List<ConnectorConfig>))
            {
                if (_context.Connectors.Where(r => r.ccid == ccid).Count() > 0)
                {
                    var entity = _context.DeDupSettings.Where(r => r.ccid == ccid).Include(r => r.Connectors).FirstOrDefault();
                    if (entity != null && entity.Connectors != null)
                    {
                        if (databaseType.HasValue)
                        {
                            if (databaseType == DatabaseType.Heroku_Postgres
                                || databaseType == DatabaseType.AWS_Postgres
                                || databaseType == DatabaseType.Azure_Postgres)
                            {
                                if (entity.Connectors.Where(c => JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.Heroku_Postgres
                                || JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.Azure_Postgres
                                || JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.AWS_Postgres).Count() > 0)
                                    return entity.Connectors.Where(c => JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.Heroku_Postgres
                                || JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.Azure_Postgres
                                || JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == DatabaseType.AWS_Postgres).ToList().ToModel<List<ConnectorConfig>>(IsSetConfig) as T;
                            }
                            else
                            {
                                if (entity.Connectors.Where(c => JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == databaseType).Count() > 0)
                                    return entity.Connectors.Where(c => JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json).databaseType == databaseType).ToList().ToModel<List<ConnectorConfig>>(IsSetConfig) as T;
                            }
                        }
                        else
                        {
                            return entity.Connectors.ToList().ToModel<List<ConnectorConfig>>(IsSetConfig) as T;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(DeDupSettings))
            {
                if (_context.DeDupSettings.Where(r => r.ccid == ccid).Count() > 0)
                {
                    var entity = _context.DeDupSettings.Where(r => r.ccid == ccid).FirstOrDefault();
                    return entity as T;
                }
            }

            else if (typeof(T) == typeof(DatabaseConfig))
            {
                if (_context.DeDupSettings.Where(r => r.ccid == ccid).Count() > 0)
                {
                    var entity = _context.DeDupSettings.Where(r => r.ccid == ccid).FirstOrDefault();
                    return entity.ToModel<DatabaseConfig>() as T;
                }
            }
            else if (typeof(T) == typeof(List<ConnectorLogs>))
            {
                List<ConnectorLogs> connectorLogs = (from c in _context.Connectors
                                                     where c.ccid == ccid
                                                     select new ConnectorLogs()
                                                     {
                                                         deduped_table_name = (JsonConvert.DeserializeObject<DatabaseConfig>(c.dest_config_json)).object_name,
                                                         sync_updated_count = c.sync_updated_count,
                                                         sync_connector_name = c.connector_name,
                                                         sync_started_at = c.sync_started_at,
                                                         sync_ended_at = c.sync_ended_at,
                                                         sync_status = c.sync_status,
                                                         last_sync_at = c.last_sync_at,
                                                         last_sync_status = c.last_sync_status,
                                                         sync_count = c.sync_count,
                                                         dedup_type = c.dedup_type,
                                                         source_type = c.dedup_source_type,
                                                         deduped_count = c.deduped_count,
                                                         sync_connector_type = c.dedup_type.ToString(),
                                                         unique_records_count = c.unique_records_count,
                                                         total_records_count = c.total_records_count,
                                                         sync_logs = Utilities.GetJsonPropertyValueByKeyPath<List<string>>(c.sync_log_json, "")
                                                     }).ToList();
                return connectorLogs as T;
            }

            return null;
        }

        /// <summary>
        /// Method: AddEditExtensionsConfig
        /// Description: It is used to add/update connector in connectors table
        /// </summary>
        /// <param name="connectorConfig"></param>
        public async Task AddEditExtensionsConfig(ConnectorConfig connectorConfig)
        {
            using (var transaction = _context.Database.BeginTransaction())
            {
                Connectors entity;
                DeDupSettings dedupSettings = null;
                bool isNew = false;
                List<SyncObjectColumn> syncObjectColumn = new List<SyncObjectColumn>();
                List<DatabaseConfig> _databaseConfig = new List<DatabaseConfig>();
                if (connectorConfig.connectorId.HasValue)
                {
                    entity = Find(connectorConfig.ccid, connectorConfig.connectorId);
                    if (entity != null)
                    {
                        //delete sync process if it is in progress while updating connector
                        if (!string.IsNullOrEmpty(entity.job_id)
                            || entity.schedule_type != ScheduleType.MANUAL_SYNC)
                        {
                            //delete old jobs
                            JobScheduler.Instance.DeleteJob(ccid: entity.ccid, connectorId: entity.connector_id, jobId: entity.job_id, scheduleType: entity.schedule_type);

                            //reset job id once deleted
                            entity.job_id = string.Empty;
                            if (entity.sync_status == 1)
                            {
                                //set status interrupted
                                entity.sync_status = 10;
                            }
                            else if (entity.sync_status.HasValue)
                            {
                                //reset sync status
                                entity.sync_status = 0;
                            }
                        }
                        else if (entity.sync_status.HasValue)
                        {
                            //reset sync status
                            entity.sync_status = 0;
                        }

                        if (entity.sync_status != 2 && entity.sync_status != 10)
                        {
                            entity.sync_count = null;
                            entity.unique_records_count = null;
                            entity.sync_updated_count = null;
                            entity.total_records_count = null;
                            entity.sync_log_json = null;
                            entity.sync_started_at = null;
                            entity.sync_ended_at = null;
                            entity.last_sync_at = null;
                            entity.last_sync_status = null;
                        }

                        dedupSettings = entity.DeDupSetting;
                        //check table exist or not. if not then create table and sync
                        //isSyncTableExist = SyncRepository.SyncTableIsExist(connectorConfig);
                    }
                }
                else
                {
                    isNew = true;
                    entity = new Connectors() { ccid = connectorConfig.ccid };
                    //Set next connector id
                    entity.connector_id = GetMaxID(entity.ccid, ref dedupSettings) + 1;
                }
                // connectorConfig.destDBConfig.db_schema = connectorConfig.destDBSchema;
                entity.connector_name = connectorConfig.connectorName;
                //var matchingColumnDatatype = new List<DatabaseTableColumns>();
                if (isNew)
                {
                    //entity.sync_src = connectorConfig.dataSource;
                    //entity.src_object_name = connectorConfig.sourceObjectName;
                    //commented by Kathir on 12-Aug-2020
                    // entity.src_object_fields_json = JsonConvert.SerializeObject(connectorConfig.sourceObjectFields);
                    //if (entity.sync_src == DataSource.Heroku_Postgres
                    //   || entity.sync_src == DataSource.Azure_Postgres
                    //   || entity.sync_src == DataSource.AWS_Postgres
                    //   || entity.sync_src == DataSource.Azure_SQL)
                    //{
                    //    entity.src_config_json = JsonConvert.SerializeObject(connectorConfig.dbConfig);
                    //}
                    //if (connectorConfig.dedup_type != DedupType.Full_Dedup)
                    //{
                    //    entity.connector_type = (ConnectorType)connectorConfig.syncDestination;
                    //    // entity.dest_object_name = connectorConfig.destObjectName;
                    //   // connectorConfig.destDBConfig.object_name = connectorConfig.destObjectName;
                    //}
                    //else
                    //{
                    //    if (connectorConfig.dataSource == DataSource.Heroku_Postgres)
                    //        entity.connector_type = ConnectorType.Heroku_Postgres;
                    //    if (connectorConfig.dataSource == DataSource.AWS_Postgres)
                    //        entity.connector_type = ConnectorType.AWS_Postgres;
                    //    if (connectorConfig.dataSource == DataSource.Azure_Postgres)
                    //        entity.connector_type = ConnectorType.Azure_Postgres;
                    //    // entity.dest_object_name = string.Empty;
                    //    connectorConfig.destDBConfig.object_name = string.Empty;
                    //}
                    //entity.src_schema = connectorConfig.dbSchema;
                    entity.dedup_type = connectorConfig.dedup_type;
                    entity.dedup_source_type = connectorConfig.dedupSourceType;
                    //if ((connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination ||
                    if (connectorConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                    {
                        // entity.compare_object_fields = JsonConvert.SerializeObject(connectorConfig.compareObjectFieldsMapping);
                        _databaseConfig = new List<DatabaseConfig>();
                        _databaseConfig.Add(connectorConfig.dbConfig_compare);
                        _databaseConfig.Add(connectorConfig.dbConfig);
                        entity.compare_config_json = JsonConvert.SerializeObject(_databaseConfig);
                    }

                    //if (IsMultipleConfigSupported)
                    //{
                    //    if (entity.connector_type == ConnectorType.Heroku_Postgres
                    //       || entity.connector_type == ConnectorType.Azure_Postgres
                    //       || entity.connector_type == ConnectorType.AWS_Postgres
                    //       || entity.connector_type == ConnectorType.Azure_SQL)
                    //    {
                    //        entity.dest_config_json = JsonConvert.SerializeObject(connectorConfig.destDBConfig);
                    //    }
                    //}

                    //set global db setting
                    if (dedupSettings != null)
                    {
                        //save sync database url
                        if (connectorConfig.dbConfig != null &&
                            !string.IsNullOrEmpty(connectorConfig.dbConfig.syncDefaultDatabaseUrl))
                        {
                            if (string.IsNullOrEmpty(dedupSettings.database_config_json))
                            {
                                dedupSettings.database_config_json = JsonConvert.SerializeObject((new List<DatabaseConfig>() { connectorConfig.dbConfig }));
                                _context.Entry(dedupSettings).State = EntityState.Modified;
                            }
                            else
                            {
                                var dbConfigs = dedupSettings.ToModel<List<DatabaseConfig>>();
                                if (dbConfigs != null && dbConfigs.FirstOrDefault(p => p.databaseType == connectorConfig.dbConfig.databaseType) == null)
                                {
                                    dbConfigs.Add(connectorConfig.dbConfig);
                                    dedupSettings.database_config_json = JsonConvert.SerializeObject(dbConfigs);
                                    _context.Entry(dedupSettings).State = EntityState.Modified;
                                }
                            }
                        }
                    }
                    else
                    {
                        dedupSettings = new DeDupSettings() { ccid = connectorConfig.ccid };
                        //save sync database url
                        if (connectorConfig.dbConfig != null && !string.IsNullOrEmpty(connectorConfig.dbConfig.syncDefaultDatabaseUrl))
                        {
                            dedupSettings.database_config_json = JsonConvert.SerializeObject((new List<DatabaseConfig>() { connectorConfig.dbConfig }));
                        }
                        //add dedupsetting
                        _context.Entry(dedupSettings).State = EntityState.Added;
                    }
                    entity.unique_records_count = null;
                    entity.sync_updated_count = null;
                    entity.sync_count = null;
                    entity.total_records_count = null;
                }
                connectorConfig.dbConfig.new_record_filter = connectorConfig.srcNewRecordFilter;
                //connectorConfig.dbConfig.update_record_filter = connectorConfig.srcUpdateRecordFilter;
                if (connectorConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                {
                    connectorConfig.dbConfig_compare.new_record_filter = connectorConfig.srcNewRecordFilter2;
                    //connectorConfig.dbConfig_compare.update_record_filter = connectorConfig.srcUpdateRecordFilter2;
                }

                entity.schedule_type = connectorConfig.scheduleType;
                //entity.src_new_record_filter = connectorConfig.srcNewRecordFilter;
                //entity.src_update_record_filter = connectorConfig.srcUpdateRecordFilter;
                entity.two_way_sync_priority = connectorConfig.twoWaySyncPriority;

                entity.custom_schedule_in_minutes = connectorConfig.customScheduleInMinutes;
                //Added by Kathir on 20-8-2020
                entity.dedup_method = connectorConfig.dedup_method;

                entity.fuzzy_ratio = connectorConfig.fuzzy_ratio;
                entity.dedup_match_type = connectorConfig.dedupMatchType;
                entity.review_before_delete = connectorConfig.review_before_delete;
                entity.backup_before_delete = connectorConfig.backup_before_delete;
                entity.simulation_count = connectorConfig.simulation_count;

                //if ((connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination ||

                if (connectorConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                {
                    _databaseConfig = new List<DatabaseConfig>();
                    _databaseConfig.Add(connectorConfig.dbConfig_compare);
                    _databaseConfig.Add(connectorConfig.dbConfig);

                    entity.compare_config_json = JsonConvert.SerializeObject(_databaseConfig);
                }
                else
                {
                    _databaseConfig = new List<DatabaseConfig>();
                    _databaseConfig.Add(connectorConfig.dbConfig);
                    entity.compare_config_json = JsonConvert.SerializeObject(_databaseConfig);
                }
                int count = 0;
                foreach (DatabaseConfig dbc in _databaseConfig)
                {
                    var srcTbl = SyncRepository.GetPGDatabaseTableColumns(dbc, dbc.object_name, dbc.db_schema);
                    if (count == 0)
                    {
                        syncObjectColumn = (from c in srcTbl
                                            select new SyncObjectColumn() { name = c.name, fieldType = c.fieldType.ToString() }).ToList();
                    }
                    else
                    {
                        var notmatchingColumn = srcTbl.Where(a => !syncObjectColumn.Any(b => a.name == b.name)).ToList();

                        var notmatchingDatatype = srcTbl.Where(a => syncObjectColumn.Any(b => a.name == b.name && a.fieldType != b.fieldType)).ToList();

                        List<SyncObjectColumn> notmatchingColumn1 = (from c in notmatchingColumn
                                                                     select new SyncObjectColumn() { name = c.name, fieldType = c.fieldType.ToString() }).ToList();
                        List<SyncObjectColumn> matchingColumn1 = (from c in notmatchingDatatype
                                                                  select new SyncObjectColumn() { name = c.name, fieldType = c.fieldType.ToString() }).ToList();
                        syncObjectColumn.AddRange(notmatchingColumn1);
                        syncObjectColumn.RemoveAll(x => matchingColumn1.Any(a => x.name == a.name));
                    }
                    count++;
                }

                //Assigned source object fields to compare object field json
                List<ObjectFieldsWithRatio> _ObjectFieldsWithRatio = new List<ObjectFieldsWithRatio>();
                foreach (string colName in connectorConfig.sourceObjectFields)
                {
                    ObjectFieldsWithRatio ofr = new ObjectFieldsWithRatio();
                    ofr.Similarity_Value = colName;
                    ofr.Similarity_Percent = connectorConfig.fuzzy_ratio;
                    _ObjectFieldsWithRatio.Add(ofr);
                }
                entity.compare_object_fields = JsonConvert.SerializeObject(_ObjectFieldsWithRatio);

                DestDatabaseConfig _DestDatabaseConfig = new DestDatabaseConfig();
                _DestDatabaseConfig.serialNo = connectorConfig.destDBConfig.serialNo;
                _DestDatabaseConfig.syncDefaultDatabaseUrl = connectorConfig.destDBConfig.syncDefaultDatabaseUrl;
                _DestDatabaseConfig.object_name = connectorConfig.destDBConfig.object_name;
                _DestDatabaseConfig.db_schema = connectorConfig.destDBConfig.db_schema;
                _DestDatabaseConfig.databaseType = connectorConfig.destDBConfig.databaseType;
                //Adding all source column together
                _DestDatabaseConfig.dest_object_fields = syncObjectColumn;

                entity.dest_config_json = JsonConvert.SerializeObject(_DestDatabaseConfig);
                //save extension setting
                _context.Entry(entity).State = isNew ? EntityState.Added : EntityState.Modified;
                _context.SaveChanges();

                transaction.Commit();

                //Schedule job if scheduletype is not manual
                if (entity.schedule_type != ScheduleType.MANUAL_SYNC)
                {
                    if (entity.schedule_type == ScheduleType.CUSTOM)
                       await JobScheduler.Instance.ScheduleJob(entity.ccid, entity.connector_id, JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType, entity.schedule_type, (entity.custom_schedule_in_minutes.HasValue ? entity.custom_schedule_in_minutes.Value : 1200));
                    else
                      await  JobScheduler.Instance.ScheduleJob(entity.ccid, entity.connector_id, JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType, entity.schedule_type);
                }
                //});
            }
        }


        /// <summary>
        /// Method: AddEditDatabaseConfig
        /// Description: It is used to insert/update database config property by ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="fieldName"></param>
        /// <param name="fieldValue"></param>
        public void AddEditDatabaseConfig(DatabaseConfig dbConfig, int? id)
        {
            DeDupSettings dedupSettings = null;
            if (dbConfig.databaseType == DatabaseType.None)
            {
                dbConfig.databaseType = DatabaseType.Heroku_Postgres;
            }
            if (id.HasValue)
            {
                //get connector by id
                Connectors entity = Find(dbConfig.ccid, id);

                //assign DeDupsettings
                dedupSettings = entity.DeDupSetting;

                //save sync db config
                if (dedupSettings != null && string.IsNullOrEmpty(dedupSettings.database_config_json))
                {
                    dedupSettings.database_config_json = JsonConvert.SerializeObject(dbConfig);
                    _context.Entry(dedupSettings).State = EntityState.Modified;
                }

                //if (JsonConvert.DeserializeObject<DatabaseConfig>(entity.comp).databaseType == DatabaseType.Heroku_Postgres
                //    || entity.sync_src == DataSource.Azure_Postgres
                //    || entity.sync_src == DataSource.AWS_Postgres
                //    || entity.sync_src == DataSource.Azure_SQL)
                //{
                //    entity.src_config_json = JsonConvert.SerializeObject((new List<DatabaseConfig>() { dbConfig }));
                //}
                //if (JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType == DatabaseType.Heroku_Postgres
                //    || JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType == DatabaseType.Azure_Postgres
                //    || JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType == DatabaseType.AWS_Postgres
                //    || JsonConvert.DeserializeObject<DatabaseConfig>(entity.dest_config_json).databaseType == DatabaseType.Azure_SQL)
                //{
                //    entity.dest_config_json = JsonConvert.SerializeObject((new List<DatabaseConfig>() { dbConfig }));
                //}

                //_context.Entry(entity).State = EntityState.Modified;
            }
            else
            {
                if (_context.DeDupSettings.Where(r => r.ccid == dbConfig.ccid).Count() > 0)
                {
                    dedupSettings = _context.DeDupSettings.Where(r => r.ccid == dbConfig.ccid).FirstOrDefault();
                    //get latest database value
                    _context.Entry<DeDupSettings>(dedupSettings).GetDatabaseValues();

                    dedupSettings.database_config_json = JsonConvert.SerializeObject(dbConfig);

                    _context.Entry(dedupSettings).State = EntityState.Modified;
                }
                else
                {
                    //Create new DeDupSettings and assign ccid
                    dedupSettings = new DeDupSettings() { ccid = dbConfig.ccid };

                    dedupSettings.database_config_json = JsonConvert.SerializeObject((new List<DatabaseConfig>() { dbConfig }));

                    _context.DeDupSettings.Add(dedupSettings);
                }
            }

            _context.SaveChanges();
        }


        /// <summary>
        /// Method: DeleteExtensionConfigById
        /// Description: It is used to delete connector from connectors table by ccid and connectorId
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="connectorId"></param>
        /// <returns>true/false</returns>
        public bool DeleteExtensionConfigById(string ccid, int connectorId)
        {
            if (connectorId > 0 && !string.IsNullOrEmpty(ccid))
            {
                var entity = Find(ccid, connectorId);
                if (entity != null)
                {
                    DeletectindexConfigById(ccid, connectorId, true);
                    //Set entity state to delete
                    _context.Entry(entity).State = EntityState.Deleted;
                    _context.SaveChanges();

                    //delete job if it is in progress
                    JobScheduler.Instance.DeleteJob(ccid, Convert.ToInt32(connectorId), entity.job_id, entity.schedule_type);
                    return true;
                }
            }

            return false;
        }
        /// <summary>
        /// Delete ct-index table query execution
        /// </summary>
        /// <param name="connectorConfig"></param>
        /// <returns></returns>
        private bool deleteCtIndextable(ConnectorConfig connectorConfig)
        {
            StringBuilder sb = new StringBuilder();
            if (connectorConfig != null)
            {
                sb.Append($"DROP TABLE IF EXISTS \"{connectorConfig.destDBSchema}\".\"{connectorConfig.destObjectName}_ctindex\";");
                using (ConnectionFactory connectionFactory = new ConnectionFactory(connectorConfig.destDBConfig.syncDefaultDatabaseUrl))
                {
                    Console.WriteLine("Delete ctindex by query{0} ", sb.ToString());
                    connectionFactory.DbConnection.QueryAsync<dynamic>(sb.ToString());
                    sb.Clear();
                    sb = null;
                }
            }
            return true;
        }
        /// <summary>
        /// This method is used to delete the CTindex table from destination database
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="connectorId"></param>
        /// <param name="IsSingle"></param>
        /// <returns></returns>
        public bool DeletectindexConfigById(string ccid, int connectorId, bool IsSingle)
        {
            if (IsSingle)
            {
                if (connectorId > 0 && !string.IsNullOrEmpty(ccid))
                {
                    Console.WriteLine("Delete ctindex by connectorId-{0} and Resource Id-{1} ", connectorId, ccid);
                    ConnectorConfig connectorConfig = Get<ConnectorConfig>(ccid, connectorId, null, true);
                    if (connectorConfig != null)
                    {
                        return deleteCtIndextable(connectorConfig);
                    }
                }
            }
            else
            {
                if (connectorId == 0 && !string.IsNullOrEmpty(ccid))
                {
                    Console.WriteLine("Delete ctindex while user deprovision using Resource Id-{0} ", ccid);
                    List<ConnectorConfig> connectorConfigs = Get<List<ConnectorConfig>>(ccid, 0, null, true);
                    if (connectorConfigs != null && connectorConfigs.Count > 0)
                    {
                        for (int i = 0; i < connectorConfigs.Count; i++)
                        {
                            Console.WriteLine("Delete ctindex by connectorId-{0} and Resource Id-{1} ", connectorConfigs[i].connectorId, ccid);
                            deleteCtIndextable(connectorConfigs[i]);
                            if (i == connectorConfigs.Count - 1)
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }
        /// <summary>
        /// Method: UpdateSyncInfo
        /// Description: It is used to update connector sync info in connectors table by ccid and connectorId
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <param name="status"></param>
        /// <param name="count"></param>
        /// <param name="jobid"></param>
        public void UpdateSyncInfo(int id, string ccid, int status = -1, int count = -1, string jobid = "")
        {
            if (id > 0 && !string.IsNullOrEmpty(ccid))
            {
                //Get connector by ccid and id
                var entity = Find(ccid, id);
                if (entity != null)
                {
                    //Assign sync status
                    if (status >= 0)
                    {
                        entity.sync_status = status;
                        if (status == 2)
                        {
                            entity.sync_ended_at = DateTime.UtcNow;
                        }
                    }
                    //Assign sync count
                    if (count >= 0)
                    {
                        if (count == 0)
                            entity.sync_count = 0;
                        else
                            entity.sync_count += count;
                    }
                    //Assign jobid
                    if (!string.IsNullOrEmpty(jobid))
                    {
                        entity.job_id = jobid;
                    }

                    //Set entity state to modified
                    _context.Entry(entity).State = EntityState.Modified;
                    _context.SaveChanges();
                }
            }
        }

        /// <summary>
        /// Method: GetSyncStatus
        /// Description: It is used to get current sync status of connector from connectors table
        /// by ccid and connectorId
        /// </summary>
        /// <param name="ccid"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public int GetSyncStatus(string ccid, int id)
        {
            var syncStatus = 0;
            //Get connector by ccid and id
            var entity = Find(ccid, id);
            if (entity != null)
            {
                //Get sync status
                syncStatus = (int)entity.sync_status;
            }
            return syncStatus;
        }

        /// <summary>
        /// Method: SwitchToManual
        /// Description: It is used to change auto sync to manual of connector by ccid and id
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        public void SwitchToManual(int id, string ccid)
        {
            if (id > 0 && !string.IsNullOrEmpty(ccid))
            {
                var entity = Find(ccid, id);
                if (entity != null)
                {
                    //convert connector model to ConnectorConfig viewmodel
                    var conConfig = entity.ToModel<ConnectorConfig>();
                    if (conConfig != null)
                    {
                        //delete job if job is processing
                        if (!string.IsNullOrEmpty(conConfig.jobId))
                        {
                            JobScheduler.Instance.DeleteJob(ccid, Convert.ToInt32(conConfig.connectorId), conConfig.jobId, conConfig.scheduleType);

                            //reset job id once deleted
                            entity.job_id = string.Empty;
                        }

                        if (entity.sync_status == 1)
                        {
                            //set status completed
                            entity.sync_status = 2;
                        }
                        else
                        {
                            //reset sync status
                            entity.sync_status = 0;
                        }
                        entity.schedule_type = ScheduleType.MANUAL_SYNC;

                        //entity.sync_count = 0;
                        //entity.sync_status = 0;
                        _context.Entry(entity).State = EntityState.Modified;
                        _context.SaveChanges();
                    }
                }
            }
        }

        public Resources GetResource(string id)
        {
            if (_context.Resources.Where(p => p.uuid == id).Count() > 0)
                return _context.Resources.FirstOrDefault(p => p.uuid == id);

            return null;
        }

        public void UpdateResourcePrivateUrl(Resources resource)
        {
            _context.Entry(resource).State = EntityState.Modified;
            _context.SaveChanges();
        }

        #region 


        #endregion

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

        ~ConnectorsRepository()
        {
            Dispose(false);
        }
    }
}
