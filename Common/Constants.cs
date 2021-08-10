using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public static class Constants
    {
        public const string AES_KEY = "TWFya2V0aW5nQ29ubmVjdG9yMjAxN0BTb2Z0cmVuZHM=";
        public const int RESTCLIENT_TIMEOUT = 5000;
        public const int RESTCLIENT_READWRITETIMEOUT = 32000;
        public const int FREE_PLAN_LEVEL = 1;

        public const string HEROKU_USERID = "HRUSERID";

        public const string HEROKU_AUTH_USERID = "HAUSRID";
        public const string HEROKU_USER_EMAIL = "HUSREMAIL";
        public const string PRIVATE_ADDON_APP_NAME = "dedup";
        public const string ADDON_DB_DEFAULT_SCHEMA = "dedup";
        public const string POSTGRES_DEFAULT_SCHEMA = "public";
        public const string MSSQL_DEFAULT_SCHEMA = "dbo";
        public const string DE_DEFAULT_FOLDER = "Data Extensions";
        public const string DEFAULT_AUTH_COOKIE_SCHEME = ".DDCookieScheme";
        public const string DEFAULT_AUTH_COOKIE_NAME = "DDCookies";
       
        public const string HEROKU_ACCESS_TOKEN = "HATOKEN";
        public const string HEROKU_REFRESH_TOKEN = "HRTOKEN";
        public const string HEROKU_TOKEN_EXPIREDIN = "HTEXPIREDIN";
        public const string HEROKU_MAIN_APP_NAME = "HANAME";
        public const string HEROKU_MAIN_APP_ID = "HAPPID";
        public const string HEROKU_ORG_ID = "HORGID";
        public const string HEROKU_ORG_NAME = "HORGNAME";
        public const string DEDUP_PROXY_APP = "ddup";
        public static readonly string JOB_QUEUE_NAME = "critical";
        public static readonly List<string> NonEncodeDataTypes = new List<string>() {
            "int",
            "integer",
            "serial",
            "smallserial",
            "smallint",
            "bigint",
            "bigserial",
            "number",
            "date",
            "timestamp",
            "timestamp without time zone",
            "timestamp with time zone",
            "time without time zone",
            "time with time zone",
            "datetime",
            "decimal",
            "real",
            "double",
            "double precision",
            "numeric",
            "boolean",
            "bit",
            "numeric[]",
            "numrange[]",
            "bigint[]",
            "smallint[]",
            "real[]",
            "integer[]",
            "double precision[]",
            "time without time zone[]",
            "time with time zone[]",
            "timestamp without time zone[]",
            "timestamp with time zone[]"
        };
    }
}
