using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dedup.Models;
using Dedup.ViewModels;

namespace Dedup.Repositories
{
    public interface IResourcesRepository: IDisposable
    {
        void Add(Resources item, OauthGrant? authToken);
        IEnumerable<Resources> GetAll();
        Resources FindResources(string key, bool isReload = false);
        Task<Resources> Find(string key, bool isReload = false);
        void Remove(string id);
        Task UpdatePlan(string id, string plan);
        Task UpdateAppName(string id, string appName);
        Task Update(Resources item);
        Task UpdateAppAndUserInfo(Resources item);
        Task UpdateLicenseAgreement(string id, bool isLicenseAccepted);
        Task UpdateResourcePrivateUrl(string key, string privateAppUrl);
    }
}
