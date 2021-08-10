using Dedup.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct PlanInfos
    {
        public int level { get; set; }
        public string name { get; set; }
        public bool field_to_feild_compare_method { get; set; }
        public bool similarity_finder_method { get; set; }
        public bool api_to_initiate_dedup { get; set; }
        public bool dedup_a_dataset { get; set; }
        public bool merge_datasets_and_dedup { get; set; }
        public bool simulate_dedup_process { get; set; }
        public bool actualize_dedup_process { get; set; }
        public bool schedule_dedup_process { get; set; }
        public int max_dedup_process_allowed { get; set; }
        public int max_fields_to_compare { get; set; }
        public bool is_postgresql { get; set; }
        public bool is_mssql { get; set; }
        public bool is_private_space { get; set; }
        public bool is_shield_space { get; set; }
        public bool is_active { get; set; }

        public bool isLicenseAccepted { get; set; }
        public int addedDedupProcessCount { get; set; }

        public bool IsInitialized
        {
            get { return !this.IsNull(); }
        }
    }
}
