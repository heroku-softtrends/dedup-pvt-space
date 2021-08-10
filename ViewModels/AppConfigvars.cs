using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class AppConfigvars
    {
        public string DEDUP_PROXY_APP { get; set; } = "dedup-addon-proxy";

        public string JOB_QUEUE_NAME { get; set; } = "pvt_critical";
        //public string AMAZONREDSHIFTODBCINI { get; set; } = "/etc/amazon.redshiftodbc.ini";
        //public string LD_LIBRARY_PATH { get; set; } = "$LD_LIBRARY_PATH:/etc/lib";
        //public string ODBCINI { get; set; } = "/etc/odbc.ini";
        //public string ODBCSYSINI { get; set; } = "/app/etc";

    }
}
