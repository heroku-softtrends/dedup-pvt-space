using Dedup.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Repositories
{
    public interface IPartnerAuthTokenRepository : IDisposable
    {
        void Reload(PartnerAuthTokens entity);
        PartnerAuthTokens Add(PartnerAuthTokens item);
        PartnerAuthTokens Find(string key);
        void Remove(string id);
        void Update(PartnerAuthTokens item);
        void UpdateReturnUrl(string authId, string returnUrl);
        bool IsValidResource(string key);
        Resources GetResource(string key);
    }
}
