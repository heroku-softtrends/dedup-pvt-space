using Dedup.Common;
using Dedup.Models;
using Dedup.Repositories;
using Dedup.ViewModels;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Linq;

namespace Dedup.Extensions
{
    public static class ConnectorsExtensions
    {
        public static T ToModel<T>(this Connectors connector, bool isSetConfig = true) where T : class
        {
            if (connector != null)
            {
                ConnectorConfig conConfig = null;
                conConfig = new ConnectorConfig()
                {
                    ccid = connector.ccid,
                    connectorId = connector.connector_id,
                    connectorName = connector.connector_name,


                    scheduleType = connector.schedule_type,
                    twoWaySyncPriority = connector.two_way_sync_priority,

                    syncStatus = connector.sync_status,
                    dedup_type = connector.dedup_type,
                    jobId = connector.job_id,
                    syncStartedAt = connector.sync_started_at,
                    syncEndedAt = connector.sync_ended_at,
                    lastSyncAt = connector.last_sync_at,
                    lastSyncStatus = connector.last_sync_status,

                    syncCount = connector.sync_count,
                    total_records_count = connector.total_records_count,
                    deduped_count = connector.deduped_count,
                    unique_records_count = connector.unique_records_count,
                    sync_updated_count = connector.sync_updated_count,


                    customScheduleInMinutes = connector.custom_schedule_in_minutes,
                    dedupSourceType = connector.dedup_source_type,

                    dedup_method = connector.dedup_method,
                    review_before_delete = connector.review_before_delete,
                    backup_before_delete = connector.backup_before_delete,
                    simulation_count = connector.simulation_count,
                    dedupMatchType = connector.dedup_match_type,

                    dedup_process_count = connector.dedup_process_count,

                    fuzzy_ratio = connector.fuzzy_ratio
                };

                if (conConfig != null)
                {
                    if (string.IsNullOrEmpty(conConfig.dbSchema))
                    {
                        if ((conConfig.syncDestination == ConnectorType.Heroku_Postgres || conConfig.dataSource == DataSource.Heroku_Postgres
                            || conConfig.syncDestination == ConnectorType.Azure_Postgres || conConfig.dataSource == DataSource.Azure_Postgres
                            || conConfig.syncDestination == ConnectorType.AWS_Postgres || conConfig.dataSource == DataSource.AWS_Postgres))
                        {
                            conConfig.dbSchema = Constants.POSTGRES_DEFAULT_SCHEMA;
                        }
                        else if (conConfig.syncDestination == ConnectorType.Azure_SQL)
                        {
                            conConfig.dbSchema = Constants.MSSQL_DEFAULT_SCHEMA;
                        }
                    }


                    if (!string.IsNullOrEmpty(connector.compare_config_json))// && (connector.dedup_source_type == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination || connector.dedup_source_type == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B))
                    {

                        List<DatabaseConfig> _databaseConfig = JsonConvert.DeserializeObject<List<DatabaseConfig>>(connector.compare_config_json);
                        foreach (DatabaseConfig config in _databaseConfig)
                        {
                            conConfig.multipleDBConfigs.Add(config);
                        }
                        if (conConfig.dedupSourceType != SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                        {
                            conConfig.dbConfig = _databaseConfig[0];
                            conConfig.dbSchema = _databaseConfig[0].db_schema;
                            conConfig.dataSource = _databaseConfig[0].dataSource;
                            conConfig.sourceObjectName = _databaseConfig[0].object_name;
                            conConfig.srcNewRecordFilter = _databaseConfig[0].new_record_filter;
                            conConfig.srcUpdateRecordFilter = _databaseConfig[0].update_record_filter;
                        }
                        if (conConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                        {
                            conConfig.dbConfig_compare = _databaseConfig[0];
                            conConfig.dbConfig = _databaseConfig[1];
                            conConfig.dbSchema = _databaseConfig[1].db_schema;
                            conConfig.dataSource = _databaseConfig[1].dataSource;
                            conConfig.sourceObjectName = _databaseConfig[1].object_name;
                            conConfig.srcNewRecordFilter = _databaseConfig[1].new_record_filter;
                            conConfig.srcUpdateRecordFilter = _databaseConfig[1].update_record_filter;

                            if (conConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                            {
                                conConfig.srcNewRecordFilter2 = _databaseConfig[0].new_record_filter;
                                conConfig.srcUpdateRecordFilter2 = _databaseConfig[0].update_record_filter;
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(connector.compare_object_fields))
                    {
                        List<ObjectFieldsWithRatio> _ObjectFieldsWithRatio = new List<ObjectFieldsWithRatio>();
                        _ObjectFieldsWithRatio = JsonConvert.DeserializeObject<List<ObjectFieldsWithRatio>>(connector.compare_object_fields);
                        if (connector.dedup_source_type == SourceType.Remove_Duplicates_from_a_Single_Table || _ObjectFieldsWithRatio != null)
                        {
                            conConfig.DedupObjectFields = _ObjectFieldsWithRatio;
                            List<string> compareFields = new List<string>();
                            foreach (ObjectFieldsWithRatio ofr in _ObjectFieldsWithRatio)
                            {
                                compareFields.Add(ofr.Similarity_Value);
                            }
                            conConfig.sourceObjectFields = compareFields;
                            conConfig.dbConfig_compare.compareObjectFields = compareFields;
                        }
                    }

                    if (!string.IsNullOrEmpty(connector.dest_config_json))
                    {
                        DestDatabaseConfig _DestDatabaseConfig = new DestDatabaseConfig();

                        _DestDatabaseConfig = JsonConvert.DeserializeObject<DestDatabaseConfig>(connector.dest_config_json);

                        conConfig.destDBConfig.serialNo = _DestDatabaseConfig.serialNo;
                        conConfig.destDBConfig.syncDefaultDatabaseUrl = _DestDatabaseConfig.syncDefaultDatabaseUrl;
                        conConfig.destDBConfig.object_name = _DestDatabaseConfig.object_name;
                        conConfig.destDBConfig.db_schema = _DestDatabaseConfig.db_schema;
                        conConfig.destDBConfig.databaseType = _DestDatabaseConfig.databaseType;
                        conConfig.destDBConfig.dataSource = (DataSource)_DestDatabaseConfig.databaseType;

                        conConfig.destDBConfig.dest_object_fields = _DestDatabaseConfig.dest_object_fields;

                        conConfig.destObjectName = conConfig.destDBConfig.object_name;
                        conConfig.destDBSchema = conConfig.destDBConfig.db_schema;
                        conConfig.syncDestination = (ConnectorType)conConfig.destDBConfig.databaseType;
                    }

                    conConfig.child_record_count = SyncRepository.GetChildRecordsCount(conConfig);

                    if (typeof(T) == typeof(ConnectorConfig))
                    {
                        return conConfig as T;
                    }
                    if (typeof(T) == typeof(List<ConnectorConfig>))
                    {
                        List<ConnectorConfig> connectorConfigs = new List<ConnectorConfig>() { conConfig };
                        return connectorConfigs as T;
                    }
                }
            }

            return null;
        }

        public static T ToModel<T>(this List<Connectors> connectors, bool isSetConfig = true) where T : class
        {
            if (typeof(T) == typeof(List<ConnectorConfig>))
            {
                List<ConnectorConfig> connectorConfigs = null;
                if (connectors != null)
                {
                    DeDupSettings dedupSettings = isSetConfig ? connectors.FirstOrDefault().DeDupSetting : null;
                    connectorConfigs = connectors.Select(c =>
                    {
                        ConnectorConfig conConfig;
                        conConfig = new ConnectorConfig()
                        {
                            ccid = c.ccid,
                            connectorId = c.connector_id,
                            connectorName = c.connector_name,

                            scheduleType = c.schedule_type,
                            twoWaySyncPriority = c.two_way_sync_priority,

                            syncStatus = c.sync_status,
                            dedup_type = c.dedup_type,
                            jobId = c.job_id,
                            syncStartedAt = c.sync_started_at,
                            syncEndedAt = c.sync_ended_at,
                            lastSyncAt = c.last_sync_at,
                            lastSyncStatus = c.last_sync_status,

                            customScheduleInMinutes = c.custom_schedule_in_minutes,
                            dedupSourceType = c.dedup_source_type,

                            dedup_method = c.dedup_method,
                            review_before_delete = c.review_before_delete,
                            backup_before_delete = c.backup_before_delete,

                            simulation_count = c.simulation_count,

                            syncCount = c.sync_count,
                            total_records_count = c.total_records_count,
                            deduped_count = c.deduped_count,
                            unique_records_count = c.unique_records_count,
                            sync_updated_count = c.sync_updated_count,
                            dedupMatchType = c.dedup_match_type,
                            dedup_process_count = c.dedup_process_count,

                            fuzzy_ratio = c.fuzzy_ratio * 100

                        };

                        
                        if (!string.IsNullOrEmpty(c.compare_object_fields))
                        {
                            List<ObjectFieldsWithRatio> _ObjectFieldsWithRatio = new List<ObjectFieldsWithRatio>();
                            _ObjectFieldsWithRatio = JsonConvert.DeserializeObject<List<ObjectFieldsWithRatio>>(c.compare_object_fields);
                            if (c.dedup_source_type == SourceType.Remove_Duplicates_from_a_Single_Table || _ObjectFieldsWithRatio != null)
                            {
                                conConfig.DedupObjectFields = _ObjectFieldsWithRatio;
                                List<string> compareFields = new List<string>();
                                foreach (ObjectFieldsWithRatio ofr in _ObjectFieldsWithRatio)
                                {
                                    compareFields.Add(ofr.Similarity_Value);
                                }
                                conConfig.sourceObjectFields = compareFields;
                                conConfig.dbConfig_compare.compareObjectFields = compareFields;
                                conConfig.compareObjectFieldsMapping = compareFields;
                            }
                        }

                        if (conConfig != null)
                        {
                            if (string.IsNullOrEmpty(conConfig.dbSchema))
                            {
                                if ((conConfig.dataSource == DataSource.Heroku_Postgres
                                || conConfig.dataSource == DataSource.Azure_Postgres
                                || conConfig.dataSource == DataSource.AWS_Postgres))
                                {
                                    conConfig.dbSchema = Constants.POSTGRES_DEFAULT_SCHEMA;
                                }
                                else if (conConfig.dataSource == DataSource.Azure_SQL)
                                {
                                    conConfig.dbSchema = Constants.MSSQL_DEFAULT_SCHEMA;
                                }
                            }

                            if (string.IsNullOrEmpty(conConfig.destDBSchema))
                            {
                                if ((conConfig.syncDestination == ConnectorType.Heroku_Postgres
                                || conConfig.syncDestination == ConnectorType.Azure_Postgres
                                || conConfig.syncDestination == ConnectorType.AWS_Postgres))
                                {
                                    conConfig.destDBSchema = Constants.POSTGRES_DEFAULT_SCHEMA;
                                }
                                else if (conConfig.syncDestination == ConnectorType.Azure_SQL)
                                {
                                    conConfig.destDBSchema = Constants.MSSQL_DEFAULT_SCHEMA;
                                }
                            }

                            if (!string.IsNullOrEmpty(c.compare_config_json))// && (c.dedup_source_type == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination || c.dedup_source_type == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B))
                            {

                                List<DatabaseConfig> _databaseConfig = JsonConvert.DeserializeObject<List<DatabaseConfig>>(c.compare_config_json);
                                foreach (DatabaseConfig config in _databaseConfig)
                                {
                                    conConfig.multipleDBConfigs.Add(config);
                                }
                                if (conConfig.dedupSourceType != SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                                {
                                    conConfig.dbConfig = _databaseConfig[0];
                                    conConfig.dbSchema = _databaseConfig[0].db_schema;
                                    conConfig.dataSource = _databaseConfig[0].dataSource;
                                    conConfig.sourceObjectName = _databaseConfig[0].object_name;
                                    conConfig.srcNewRecordFilter = _databaseConfig[0].new_record_filter;
                                    conConfig.srcUpdateRecordFilter = _databaseConfig[0].update_record_filter;
                                }
                                if (conConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                                {
                                    conConfig.dbConfig_compare = _databaseConfig[0];
                                    conConfig.dbConfig = _databaseConfig[1];
                                    conConfig.dbSchema = _databaseConfig[1].db_schema;
                                    conConfig.dataSource = _databaseConfig[1].dataSource;
                                    conConfig.sourceObjectName = _databaseConfig[1].object_name;
                                    conConfig.srcNewRecordFilter = _databaseConfig[1].new_record_filter;
                                    conConfig.srcUpdateRecordFilter = _databaseConfig[1].update_record_filter;
                                }
                            }


                            if (!string.IsNullOrEmpty(c.dest_config_json))
                            {
                                DestDatabaseConfig _DestDatabaseConfig = new DestDatabaseConfig();

                                _DestDatabaseConfig = JsonConvert.DeserializeObject<DestDatabaseConfig>(c.dest_config_json);

                                conConfig.destDBConfig.serialNo = _DestDatabaseConfig.serialNo;
                                conConfig.destDBConfig.syncDefaultDatabaseUrl = _DestDatabaseConfig.syncDefaultDatabaseUrl;
                                conConfig.destDBConfig.object_name = _DestDatabaseConfig.object_name;
                                conConfig.destDBConfig.db_schema = _DestDatabaseConfig.db_schema;
                                conConfig.destDBConfig.databaseType = _DestDatabaseConfig.databaseType;
                                //Added by kathir on 10-02-2021
                                conConfig.destDBConfig.dest_object_fields = _DestDatabaseConfig.dest_object_fields;

                                conConfig.destDBConfig.dataSource = (DataSource)_DestDatabaseConfig.databaseType;
                                conConfig.destObjectName = conConfig.destDBConfig.object_name;
                                conConfig.destDBSchema = conConfig.destDBConfig.db_schema;
                                conConfig.syncDestination = (ConnectorType)conConfig.destDBConfig.databaseType;
                            }
                            conConfig.child_record_count = SyncRepository.GetChildRecordsCount(conConfig);
                        }
                        return conConfig;
                    }).ToList();
                }

                return connectorConfigs as T;
            }
            else if (typeof(T) == typeof(List<ConnectorLogs>))
            {
                List<ConnectorLogs> connectorLogs = null;
                if (connectors != null)
                {
                    connectorLogs = connectors.Select(c =>
                    {
                        return new ConnectorLogs()
                        {
                            sync_connector_name = c.connector_name,
                            sync_started_at = c.sync_started_at,
                            sync_ended_at = c.sync_ended_at,
                            sync_count = c.sync_count,
                            last_sync_at = c.last_sync_at,
                            last_sync_status = c.last_sync_status,
                            sync_status = c.sync_status,
                            sync_logs = Utilities.GetJsonPropertyValueByKeyPath<List<string>>(c.sync_log_json, "")
                        };
                    }).ToList();
                }

                return connectorLogs as T;
            }

            return null;
        }
    }
}
