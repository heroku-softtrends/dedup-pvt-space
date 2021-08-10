using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public static class QueryGenerate
    {
        public static string tableName = string.Empty;
        public static string schemaName = string.Empty;

        public static string sourceTableName = string.Empty;
        public static string destTableName = string.Empty;
        public static string destCTIndexTableName = string.Empty;
        public static string additionalTableName = string.Empty;

        public static string destCtIndex = string.Empty;
        public static string destDeleted = string.Empty;

        public static List<string> sourceObjectFields = null;
        public static string dataFields = string.Empty;
        public static StringBuilder queryBuilder = null;
        public static string GetParentRecordsForReview(ConnectorConfig connectorConfig, int offSet, int pageSize)
        {
            tableName = Utilities.RemoveSpecialChars(connectorConfig.destObjectName);
            schemaName = connectorConfig.destDBSchema;

            destTableName = schemaName + "." + tableName;

            destCtIndex = tableName + "_ctindex"; ;

            destCTIndexTableName = schemaName + "." + destCtIndex;

            sourceObjectFields = new List<string>();
            sourceObjectFields = (from c in connectorConfig.sourceObjectFields
                                  select $"\"{c}\"").ToList();

            dataFields = string.Join(",", sourceObjectFields.Select(c => $"\"{tableName}\"" + "." + c).ToArray());

            queryBuilder = new StringBuilder();
            queryBuilder.Append($" SELECT " + dataFields + "," + destCtIndex + ".myctid," + destCtIndex + ".marked_for_delete FROM \"" + destTableName + "\"");
            queryBuilder.Append($" join \"{destCTIndexTableName}\" on " + destTableName + ".ctid=" + destCtIndex + ".myctid ");
            queryBuilder.Append($" where " + destCtIndex + ".parentctid is null LIMIT " + pageSize + " OFFSET " + offSet + ";");

            return queryBuilder.ToString();
        }
    }
}
