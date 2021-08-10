using Dedup.Common;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class ConnectorLogs
    {
        public string sync_connector_name { get; set; }

        public DateTime? sync_started_at { get; set; }

        public DateTime? sync_ended_at { get; set; }

        public string deduped_table_name { get; set; }

        public int? sync_status { get; set; }
        public int? sync_count { get; set; }
        public DateTime? last_sync_at { get; set; }
        public int? last_sync_status { get; set; }
        public int? unique_records_count { get; set; }
        public int? total_records_count { get; set; }
        public int? sync_updated_count { get; set; }
        public int? deduped_count { get; set; }
        public List<string> sync_logs { get; set; }
        public string sync_connector_type { get; set; }
        public DedupType dedup_type { get; set; }
        public SourceType source_type { get; set; }
    }
}
