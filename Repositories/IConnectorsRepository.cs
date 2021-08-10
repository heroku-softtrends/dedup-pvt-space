using Dedup.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dedup.ViewModels;
using Dedup.Common;

namespace Dedup.Repositories
{
    public interface IConnectorsRepository : IDisposable
    {
        Task AddEditExtensionsConfig(ConnectorConfig connectorConfig);

        bool DeleteExtensionConfigById(string ccid, int connectorId);

        void SwitchToManual(int id, string ccid);

        void UpdateSyncInfo(int id, string ccid, int status = -1, int count = -1, string jobid = "");

        int GetSyncStatus(string ccid, int id);

        T Get<T>(string key, int? id, DatabaseType? connectorType = null, bool IsSetConfig = false) where T : class;

        void AddEditDatabaseConfig(DatabaseConfig dbConfig, int? id);

        ConnectorConfig GetConnectorById(string ccid, int id, ref int consumedProcessCount);

        Resources GetResource(string id);

        void UpdateResourcePrivateUrl(Resources resource);

        Task<int> GetConnectorsCount(string ccid);
    }
}
