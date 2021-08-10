using Hangfire;
using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dedup.HangfireFilters;

namespace Dedup.Repositories
{
    public interface ISyncRepository : IDisposable
    {
        [Queue("critical")]
        [JobsFilter()]
        [AutomaticRetry(Attempts = 0, LogEvents = false, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        Task DeDupRowsFromDatabaseTable(IJobCancellationToken jobCancelToken, int connectorId, string ccid);

        Task<IList<IDictionary<string, object>>> GetParentRecordsPageByPageForReviewAndDelete(string ccid, int connectorId, int cPage, int pageSize);



        Task<IList<IDictionary<string, object>>> GetChildRecordsByParentForReviewAndDelete(string ccid, int connectorId, string ctid, int cPage, int pageSize);

        int GetCTIndexTableCount(string ccid, int connectorId);

        Task<int> UpdateReviewAndSelectedRecordsForDelete(string ccid, int connectorId, List<string> ctid);

        int GetMarkedForDeleteCount(string ccid, int connectorId);

        Task<int> ArchieveRecordsForDelete(ConnectorConfig connectorConfig, List<string> ctid);
        Task<string> DeleteDuplicateChildRecordsByCTids(ConnectorConfig connectorConfig, List<string> ctid, String parentCtid);
        Task<string> DeleteUniqueRecordsByCTids(ConnectorConfig connectorConfig, List<string> ctid);

        Task<string> ConfigureNewParentByCtid(string ccid, int connectorId, string Newctid, string Oldctid);

        //Meghna
        Task<IList<IDictionary<string, object>>> GetUniqueRecordsPageByPageForReviewAndDelete(string ccid, int connectorId, int cPage, int pageSize);
        int GetCTIndexTableUniqueRecordCount(string ccid, int connectorId);
        int GetChildRecordCount(string ccid, int connectorId, string ctid);
    }

}
