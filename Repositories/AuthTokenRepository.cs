using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dedup.Models;
using Dedup.Data;
using Microsoft.EntityFrameworkCore;
using System.Text;
using Dedup.Common;

namespace Dedup.Repositories
{
    public class AuthTokenRepository : IAuthTokenRepository
    {
        private DeDupContext _context;

       // private HerokuConnectorContext _context;
        public AuthTokenRepository(DeDupContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Method: Add
        /// Description: It is used to add new token to authtoken table
        /// </summary>
        /// <param name="item"></param>
        public async Task<AuthTokens> Add(AuthTokens item)
        {
            if (!string.IsNullOrEmpty(item.user_id))
            {
                try
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(string.Format("UPDATE \"{0}\".\"AuthTokens\" SET ", Constants.ADDON_DB_DEFAULT_SCHEMA));
                    sb.Append(string.Format(" \"access_token\"='{0}'", item.access_token));
                    sb.Append(string.Format(", \"refresh_token\"='{0}'", item.refresh_token));
                    sb.Append(string.Format(", \"token_type\"='{0}'", item.token_type));
                    sb.Append(string.Format(", \"session_nonce\"='{0}'", item.session_nonce));
                    sb.Append(string.Format(", \"expires_in\"='{0}'", item.expires_in));
                    //set where conditions
                    sb.Append(string.Format(" WHERE \"user_id\"='{0}';", item.user_id));
                    await _context.Database.ExecuteSqlCommandAsync(sb.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error:{0}", ex.Message);
                }
            }

            //Get AuthTokens by auth_id
            var entity = await Find(item.auth_id);
            if (entity == null)
            {
                _context.AuthTokens.Add(item);
                _context.SaveChanges();

                //attach resource reference
                _context.Entry(item).Reference(e => e.Resource).Load();
                entity = item;
            }
            else
            {
                entity.access_token = item.access_token;
                entity.refresh_token = item.refresh_token;
                entity.expires_in = item.expires_in;
                entity.user_id = string.IsNullOrEmpty(item.user_id) ? "" : item.user_id;
                entity.token_type = item.token_type;
                entity.session_nonce = item.session_nonce;
                entity.redirect_url = item.redirect_url;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }

            //assign resource
            item.redirect_url = entity.redirect_url;
            item.Resource = entity.Resource;
            return item;
        }

        /// <summary>
        /// Method: Find
        /// Description: It is used to get AuthTokens by auth_id
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public async Task<AuthTokens> Find(string key)
        {
            if (_context.AuthTokens != null && await _context.AuthTokens.Where(p => p.auth_id == key).CountAsync() > 0)
            {
                var entity = await _context.AuthTokens.FirstOrDefaultAsync(p => p.auth_id == key);
                //get latest database value
                await Reload(entity);
                return entity;
            }

            return null;
        }

        /// <summary>
        /// Method: Reload
        /// Description: It is used to reload AuthTokens entity
        /// </summary>
        /// <param name="entity"></param>
        public async Task Reload(AuthTokens entity)
        {
            if (entity != null)
            {
                await _context.Entry(entity).ReloadAsync();
                _context.Entry(entity).Reference(e => e.Resource).Load();
            }
        }

        /// <summary>
        /// Method: Remove
        /// Description: It is used to delete AuthToken by ccid from AuthTokens table
        /// </summary>
        /// <param name="id"></param>
        public void Remove(string id)
        {
            //Get AuthTokens by auth_id
            var entity = Find(id).Result;
            if (entity != null)
            {
                _context.Entry(entity).State = EntityState.Deleted;
                _context.SaveChanges();
            }
        }

        public async Task Update(AuthTokens item)
        {
            //Get AuthTokens by auth_id
            var entity = await Find(item.auth_id);
            if (entity != null)
            {
                entity.access_token = item.access_token;
                entity.refresh_token = item.refresh_token;
                entity.token_type = item.token_type;
                entity.user_id = item.user_id;
                entity.session_nonce = item.session_nonce;
                entity.expires_in = item.expires_in;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
        }

        public async Task<bool> IsValidResource(string key)
        {
            if (_context.Resources != null && await _context.Resources.Where(p => p.uuid == key).CountAsync() > 0)
            {
                return true;
            }

            return false;
        }

        public async Task UpdateReturnUrl(string authId, string returnUrl)
        {
            //Get AuthTokens by auth_id
            var entity = await Find(authId);
            if (entity == null)
            {
                entity = new AuthTokens();
                entity.auth_id = authId;
                entity.access_token = "";
                entity.refresh_token = "";
                entity.expires_in = DateTime.MinValue;
                entity.redirect_url = returnUrl;
                entity.user_id = "";
                _context.AuthTokens.Add(entity);
                _context.SaveChanges();
            }
            else
            {
                entity.redirect_url = returnUrl;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
        }

        public async Task<AuthTokens> FindByUserId(string userId)
        {
            return await _context.AuthTokens.Where(p => p.user_id == userId).OrderByDescending(p => p.expires_in).FirstOrDefaultAsync();
        }

        public void Dispose()
        {
        }
    }
}
