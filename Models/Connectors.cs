using Dedup.Common;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

namespace Dedup.Models
{
    public class Connectors
    {
        public string ccid { get; set; }

        public int connector_id { get; set; }

        public string connector_name { get; set; }
         
        public string dest_config_json { get; set; }
 
        public TwoWaySyncPriority two_way_sync_priority { get; set; }

        public ScheduleType schedule_type { get; set; }

        public string job_id { get; set; }

        public SourceType dedup_source_type { get; set; }

        public string compare_config_json { get; set; }

        public string compare_object_fields { get; set; }

        public Nullable<int> sync_status { get; set; }

        public Nullable<int> sync_count { get; set; }

        public Nullable<int> sync_updated_count { get; set; }

        public Nullable<int> deduped_count { get; set; }

        public Nullable<Double> fuzzy_ratio { get; set; }

        public Nullable<int> unique_records_count { get; set; }

        public Nullable<int> total_records_count { get; set; }

        public Nullable<int> dedup_process_count { get; set; }

        public Nullable<DateTime> sync_started_at { get; set; }

        public Nullable<DateTime> sync_ended_at { get; set; }

        public Nullable<DateTime> last_sync_at { get; set; }

        public Nullable<int> last_sync_status { get; set; }

        public string sync_log_json { get; set; }
 
        public DedupType dedup_type { get; set; }

        public DedupMatchType dedup_match_type { get; set; }

        public Nullable<int> custom_schedule_in_minutes { get; set; }

        public DateTime created_at { get; set; }

        public DateTime updated_at { get; set; }
        public SimilarityType dedup_method { get; set; }
        public ReviewBeforeDeleteDups review_before_delete { get; set; }
        public ArchiveRecords backup_before_delete { get; set; }
        
        public int simulation_count { get; set; }
        [ForeignKey("ccid")]
        public virtual DeDupSettings DeDupSetting { get; set; }
    }
}
