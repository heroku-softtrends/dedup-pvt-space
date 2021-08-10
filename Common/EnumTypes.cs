using System.ComponentModel;

namespace Dedup.Common
{
    public enum ScheduleType
    {
        MANUAL_SYNC,
        EVERY_15_MINS,
        ONCE_DAILY,
        ONCE_WEEKLY,
        EVERY_60_MINS,
        TWICE_DAILY,
        TWICE_WEEKLY,
        CUSTOM
    }

    public enum ConnectorType
    {
        None = 0,
        Heroku_Postgres,
        Azure_Postgres,
        AWS_Postgres,
        Azure_SQL
    }
    public enum SelectedTableType
    {
        Create_New_Table,
        Select_Existing_Table
    }

    public enum DedupType
    {
        [Description("Simulate and Verify")]
        Simulate_and_Verify,
        [Description("Safe Mode")]
        Safe_Mode,
        [Description("DeDup")]
        Full_Dedup
    }
    public enum SourceType
    {
        //[Description("Remove Duplicates from Source table")]
        //Remove_Duplicates_from_a_Single_Table,
        //[Description("Copy Source data to Destination & Remove Duplicates from Destination")]
        //Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination,
        //[Description("Merge Table ‘A’ Data to Table ‘B’ & Remove Duplicates from Table ‘B’")]
        //Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B
        [Description("Remove Duplicates from Source")]
        Remove_Duplicates_from_a_Single_Table,
        [Description("Copy Source data to Destination & Remove Duplicates from Destination")]
        Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination,
        [Description("Merge multiple tables to destination and Remove duplicates from destination")]
        Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B
    }

    public enum DataSource
    {
        None = 0,
        Heroku_Postgres,
        Azure_Postgres,
        AWS_Postgres,
        Azure_SQL
    }
    public enum SimilarityType
    {
        //[Description("Logical AND")]
        //Logical_AND,
        //[Description("Logical OR")]
        //Logical_OR,
        //[Description("Fuzzy Compare")]
        //Fuzzy_Compare
        [Description("AND Fields")]
        Logical_AND,
        [Description("OR Fields")]
        Logical_OR,
        [Description("Fuzzy Compare")]
        Fuzzy_Compare
    }
    public enum DedupMatchType
    {
        [Description("Exact Match")]
        Exact_Match,
        [Description("Similarity Match")]
        Similarity_Match
    }
    public enum ReviewBeforeDeleteDups
    {
        Yes,
        No
    }
    public enum ArchiveRecords
    {
        Yes,
        No
    }
    public enum LoadConfigType
    {
        ALL,
        PLAN,
        CONFIGVARS,
        SMTP
    }

    public enum DatabaseType
    {
        None = 0,
        Heroku_Postgres,
        Azure_Postgres,
        AWS_Postgres,
        Azure_SQL
    }

    public enum TwoWaySyncPriority
    {
        None = 0,
        Source,
        Destination
    }

    public enum CookieExpiryIn
    {
        Seconds,
        Minitues,
        Hours,
        Days,
        Months,
        Years
    }

    public enum QueryFetchType
    {
        Insert_Only,
        Update_Only,
        Both_Insert_Update
    }

    public enum HerokuStackVersion
    {
        [Description("cedar-14")]
        Cedar14,
        [Description("heroku-16")]
        Heroku16
    }

    //public enum AuthGrantType
    //{
    //    authorization_code,
    //    refresh_token
    //}

    public enum AuthGrantType
    {
        [Description("authorization_code")]
        authorization_code,
        [Description("refresh_token")]
        refresh_token,
        [Description("code")]
        code
    }

    public enum AuthScope
    {
        [Description("global")]
        global,
        [Description("identity")]
        identity,
        [Description("read")]
        read,
        [Description("write")]
        write,
        [Description("read-protected")]
        readprotected,
        [Description("write-protected")]
        writeprotected
    }
}
