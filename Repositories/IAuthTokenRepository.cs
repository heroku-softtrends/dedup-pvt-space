using Dedup.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Repositories
{
    public interface IAuthTokenRepository : IDisposable
    {
        Task Reload(AuthTokens entity);
        Task<AuthTokens> Add(AuthTokens item);
        Task<AuthTokens> Find(string key);
        Task<AuthTokens> FindByUserId(string userId);
        void Remove(string id);
        Task Update(AuthTokens item);
        Task UpdateReturnUrl(string authId, string returnUrl);
        Task<bool> IsValidResource(string key);
    }
}
