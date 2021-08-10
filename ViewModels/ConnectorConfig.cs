using Dedup.Common;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace Dedup.ViewModels
{
    //syncStatus: 0->Not Started, 1->Started but still in progress/Pending, 2-> Completed, 3-> Error while syncing
    public class ConnectorConfig
    {
        [JsonIgnore]
        public string ccid { get; set; }

        public Nullable<int> connectorId { get; set; }

        [Required, Display(Name = "Sync Destination")]
        public ConnectorType? syncDestination { get; set; }

        [Required, Display(Name = "Data Source")]
        public DataSource dataSource { get; set; }

        [Required, Display(Name = "DeDup Process")]
        public string connectorName { get; set; }

        [Required]
        public string dbSchema { get; set; } = Constants.POSTGRES_DEFAULT_SCHEMA; //postgres database schema/namespace
        [Required]
        public string destDBSchema { get; set; } = Constants.POSTGRES_DEFAULT_SCHEMA; //postgres database schema/namespace

        [Required, Display(Name = "Source Table")]
        public string sourceObjectName { get; set; }

        [Required, Display(Name = "Source Table Columns")]
        public List<string> sourceObjectFields { get; set; }

        public List<ObjectFieldsWithRatio> DedupObjectFields { get; set; }

        [Required, Display(Name = "Compare Source Table Columns")]
        public List<string> compareObjectFieldsMapping { get; set; }

        [Required, Display(Name = "Destination Table")]
        public string destObjectName { get; set; }

        [Required, Display(Name = "Schedule Type")]
        public ScheduleType scheduleType { get; set; } = ScheduleType.MANUAL_SYNC;

        [Required, Display(Name = "Minute")]
        [Range(1, 1200, ErrorMessage = "Minutes range 1-1200 only allowed")]
        [RegularExpression("^([1-9]|[1-8][0-9]|9[0-9]|[1-8][0-9]{2}|9[0-8][0-9]|99[0-9]|1[01][0-9]{2}|1200)$", ErrorMessage = "Please enter a valid minute(1-1200)")]
        public Nullable<int> customScheduleInMinutes { get; set; }

        #region DATABASE CONFIG
        [Required]
        public DatabaseConfig dbConfig { get; set; } = new DatabaseConfig();

        [Required]
        public DatabaseConfig dbConfig_compare { get; set; } = new DatabaseConfig();

        [Required]
        public DatabaseConfig destDBConfig { get; set; } = new DatabaseConfig();

        //When its multiple table for reading the compare source one by one for looping
        [JsonIgnore]
        public List<DatabaseConfig> multipleDBConfigs { get; set; } = new List<DatabaseConfig>();

        #endregion

        [Required, Display(Name = "Sync New Record Filter")]
        public string srcNewRecordFilter { get; set; } = "None";

        [Required, Display(Name = "Sync Update Record Filter")]
        public string srcUpdateRecordFilter { get; set; } = "None";

        [Required, Display(Name = "Sync New Record Filter")]
        public string srcNewRecordFilter2 { get; set; } = "None";

        [Required, Display(Name = "Sync Update Record Filter")]
        public string srcUpdateRecordFilter2 { get; set; } = "None";

        [Required, Display(Name = "Two Way Sync Priority")]
        public TwoWaySyncPriority twoWaySyncPriority { get; set; } = TwoWaySyncPriority.None;

        public Nullable<int> syncStatus { get; set; }

        [Required(ErrorMessage = "The DeDup Type is required."), Display(Name = "DeDup Type")]
        public DedupType dedup_type { get; set; }

        public Nullable<int> syncCount { get; set; }

        public Nullable<int> sync_updated_count { get; set; }

        public Nullable<int> unique_records_count { get; set; }

        public Nullable<int> dedup_process_count { get; set; }

        public Nullable<DateTime> syncStartedAt { get; set; }

        public Nullable<DateTime> syncEndedAt { get; set; }

        public Nullable<DateTime> lastSyncAt { get; set; }

        public Nullable<int> lastSyncStatus { get; set; }

        public string jobId { get; set; }
        [Required]
        public SourceType dedupSourceType { get; set; }

        [Required]
        public DedupMatchType dedupMatchType { get; set; }

        [JsonIgnore]
        public ConnectorLogs connectorLogs { get; set; }

        [JsonIgnore]
        public List<SyncObjectColumn> syncObjectColumns { get; set; }

        [JsonIgnore]
        public List<SyncObjectColumn> syncDestObjectColumns { get; set; }

        [JsonIgnore]
        public List<string> orderByColumns { get; set; }

        [JsonIgnore]
        public bool hasOrderColumns { get; set; }

        [JsonIgnore]
        public bool isTableExist { get; set; }

        [JsonIgnore]
        public bool hasPrimaryKey { get; set; }

        //newly added column by kathir on 19-8-2020
        [Required, Display(Name = "Similarity Finding Method")]
        public SimilarityType dedup_method { get; set; }
        [Required(ErrorMessage = "You Want To Review Before Deleting Duplicate")]
        public ReviewBeforeDeleteDups review_before_delete { get; set; }
        [Required(ErrorMessage = "Archive Deleted Records")]
        public ArchiveRecords backup_before_delete { get; set; }
        public string dest_object_fields { get; set; }
        public int simulation_count { get; set; }

        //[Range(0.1, 0.99, ErrorMessage = "value must be less than one !")]
        //[Range(0, 100, ErrorMessage = "Value must be less than hundred !")]
        [Required(ErrorMessage = "Similarity Threshold should not be null")]
        public Nullable<Double> fuzzy_ratio { get; set; }

        public Nullable<int> deduped_count { get; set; }

        public Nullable<int> total_records_count { get; set; }

        public Nullable<int> child_record_count { get; set; }
    }
}

