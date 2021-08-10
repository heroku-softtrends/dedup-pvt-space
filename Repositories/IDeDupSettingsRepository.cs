using Dedup.Common;
using Dedup.Models;
using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Repositories
{
    public interface IDeDupSettingsRepository : IDisposable
    {
        void Reload(DeDupSettings entity);

        void Add(DeDupSettings item);

        DeDupSettings Find(string key);

        void Remove(string id);

        void Update(DeDupSettings item);

        void AddEditDatabaseConfig(DatabaseConfig databaseConfig);

        void DeleteDatabaseConfig(DatabaseType databaseType, string key);

        void AddEditDatabaseConfig(string ccid, string fieldName, string fieldValue);

        string GetHerokuAppName(string ccid);

        Resources GetResource(string uuid);

        T Get<T>(string key, DatabaseType databaseType = DatabaseType.None) where T : class;
    }
}
