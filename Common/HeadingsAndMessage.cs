using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public static class HeadingsAndMessage
    {
        public const string DEDUP_PROCESS_TYPE = "DeDup Process Type";
        public const string DEDUP_PROCESS_NAME = "DeDup Process Name";
        public const string DEDUP_EXECUTION_MODE = "DeDup Execution Mode";
        public const string DEDUP_MATCH_TYPES = "DeDup Match Type";

        public const string DEDUP_SIMILARITY_FINIDING_METHOD = "Similarity Finding Method";
        public const string DEDUP_YOU_WANT_TO_REVIEW_BEFORE_DELETING_DUPLICATE = "You want to review before deleting duplicate?";
        public const string DEDUP_ARCHIVE_DELETED_RECORDS = "Archive Delete Records";
        public const string DEDUP_SIMULATION_COUNT = "Simulation Count";

        public const string DEDUP_DESTINATION_DATABASE_INFO = "Specify the Destination";
        public const string DEDUP_DESTINATION_DATABASE_TYPE = "Database Type";
        public const string DEDUP_DESTINATION_DATABASE_URL = "Database URL";
        public const string DEDUP_DESTINATION_DB_SCHEMA = "Database Schema";
        public const string DEDUP_DESTINATION_TABLE = "Database Table";

        public const string DEDUP_SOURCE_DATABASE_INFO = "Specify the DeDup Source";
        //Meghna
        public const string DEDUP_SOURCE_DATABASE_INFO2 = "Specify First Source table";

        public const string DEDUP_SOURCE_DATABASE_TYPE = "Data Source Type";
        public const string DEDUP_SOURCE_DATABASE_URL = "Database URL";
        public const string DEDUP_SOURCE_DB_SCHEMA = "Data Source Schema";
        public const string DEDUP_SOURCE_TABLE = "Select the Data Source Table to DeDup";
        public const string DEDUP_SOURCE_TABLE_COLUMNS = "Select which Source Table's Columns to Compare as DeDup Logic(Max";

        public const string DEDUP_COMPARE_DATABASE_TYPE = "Data Source Type";
        public const string DEDUP_COMPARE_DB_SCHEMA = "Data Source Schema";
        public const string DEDUP_COMPARE_DATABASE_URL = "Database URL";
        public const string DEDUP_COMPARE_DATABASE_INFO = "Specify Destination to Merge Source data & Remove duplicates ";
        public const string DEDUP_COMPARE_TABLE = "Select the Compare Table to DeDup";
        public const string DEDUP_COMPARE_TABLE_COLUMNS = "Columns to Compare for DeDup";

        //public const string DEDUP_COMPARE_TABLE_HEADING_1 = "Remove Duplicates from Destination";
        //public const string DEDUP_COMPARE_TABLE_HEADING_2 = "Specify DeDup Source Table ‘B’ to Merge to & Remove Duplicates";

        public const string DEDUP_COMPARE_TABLE_HEADING_1 = "Specify Destination";
        public const string DEDUP_COMPARE_TABLE_HEADING_2 = "Specify Second Source table";

        public const string DEDUP_SYNC_WITH_FILTER_COLUMN = "Access Time based DeDup Source reading (Optional)";
        public const string DEDUP_SYNC_WITH_FILTER_COLUMN_TO_NEW_RECORD_INSERT = "Data Field to check Date-time of New record insertion to Data source";
        public const string DEDUP_SYNC_WITH_FILTER_COLUMN_TO_NEW_RECORD_HINT = "(If specified, only new records inserted after last DeDup will be considered)";
        public const string DEDUP_SYNC_WITH_FILTER_COLUMN_TO_EXISTING_RECORDS_UPDATE = "Data Field to check Date-time of Update to existing records in Data source";
        public const string DEDUP_SYNC_WITH_FILTER_COLUMN_TO_UPDATE_RECORDS_HINT = "(If specified, records updated since last DeDup process will also be considered)";

        public const string DEDUP_SCHEDULE = "DeDup Schedule";
        
        public const string LINK_MESSAGE = "Note : Additional columns for comparison may be available with higher Addon Plans.For more info";
        public const string CONNECTION_STRING_DEFAULT_MESSAGE = "This defaults to using the Postgres Config Var settings of the application. Customization of this property is provided in Enterprise Plans and above.";

        public const string DEDUP_SINGLE_PROCESSS_TOOLTIP = "Permanently removes all duplicate records from the data source using the filter data columns you specify.";
        public const string DEDUP_MULTIPLE_PROCESS_TOOLTIP = "Copies records from data source to destination (using the filter data columns you specify), and then permanently removes all duplicates from destination using the filter data columns";

        public const string DEDUP_SOURCE_WITH_SINGLE_PROCESSS_TYPE_TOOLTIP = "Specify the Data source for the data to DeDup. If you have selected Remove Duplicates from Source table then duplicates are removed and a Unique set of data is retained in this table. If you selected Merge Source Data to Destination and Remove duplicates from Destination then this table data is unaltered after DeDup.";

        public const string DEDUP_SOURCE_WITH_MULTIPLE_PROCESSS_TYPE_TOOLTIP = "Specify the Data source for the data to DeDup. If you have selected Remove Duplicates from Source table then duplicates are removed and a Unique set of data is retained in this table. If you selected Merge Source Data to Destination and Remove duplicates from Destination then this table data is unaltered after DeDup.";
        public const string DEDUP_COMPARE_TABLE_TOOLTIP = "Records from DeDup Source are copied to this Destination table, and then duplicates are removed from this Destination table resulting in a Unique set of data retained in this table leaving the DeDup Source unaltered.";
        public const string DEDUP_DESTINATION_TABLE_TOOLTIP = "Temporary table where simulation result is kept. Every time you execute simulate, data from this table is cleared to make room for new simulation result.";


        public const string SIMULATE_TOOLTIP = "Use this to Simulate the DeDup Process type you selected. Simulation will copy the results to the temporary Simulation Result table. You can then verify if the process worked as you expected and then set the execution mode to ‘DeDup’";
        public const string SAFE_MODE_TOOLTIP = "Removes duplicate records from Data source records to DeDup";
        public const string FULL_DEDUP_TOOLTIP = "Use this to execute the actual DeDup process and permanently remove all duplicates";


        public const string DEDUP_EXACT_MATCH_TYPE = "Exact match will allow to select any type of filter column from checkbox";
        public const string DEDUP_SIMILARITY_MATCH_TYPE = "Similarity match will allow to select only string type of filter column from checkbox";
        //Meghna

        public const string DEDUP_PROCESS_TYPE_SingleTable = "Duplicates will be deleted from Source";
        public const string DEDUP_PROCESS_TYPE_Copy_Source_data_to_Destination = "Destination created the first operaton. Data will be copied each time from Source to Destination & Duplicates removed from Destination";
        public const string DEDUP_PROCESS_TYPE_Merge_Table_A_Data_to_Table_B = "Data from multiple source tables are merged to destination and duplicates removed from destination";
        public const string DEDUP_SIMILARITY_FINDING_METHOD_TEXT = "Similarity threshold 100% would mean the field values have to be identical when comparing. The  fuzzy logic comparison allows you to find ‘nearly similar’ records by changing the similarity threshold.";
        public const string DEDUP_SIMULATION_COUNT_TEXT= "Limited number of records will allow you to verify the Dedup logic applied to your dataset faster";
        public const string DEDUP_EXECUTION_MODE_SIMULATE = "Simulate will allow you to Dedup in a separate temporary table so that you can verify how effective Dedup is on your dataset before you do actual Dedup";
        public const string DEDUP_EXECUTION_MODE_DEDUP = "Dedup will automatically delete the duplicate records. If you want to review duplicate records and selectively delete some of them, set `Review before delete` option to Yes";
        public const string DEDUP_REVIEW_BEFORE_DELETE = "YES will allow you to use the ‘Data Steward’ to view the duplicate record sets and selectively delete records you want to delete. NO will automatically delete all duplicate records";
        public const string DEDUP_ARCHIVE_DELETE_RECORD = "YES will backup the deleted records to a table in your Database for your future reference";
    }
}
