using Microsoft.EntityFrameworkCore;
using Dedup.Data;
using Dedup.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text;

namespace Dedup.Repositories
{
    public class PartnerAuthTokenRepository : IPartnerAuthTokenRepository
    {
        private DeDupContext _context;
        public PartnerAuthTokenRepository(DeDupContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Method: Add
        /// Description: It is used to add new token to parnerauthtoken table
        /// </summary>
        /// <param name="item"></param>
        public PartnerAuthTokens Add(PartnerAuthTokens item)
        {
            //Get AuthTokens by auth_id
            var entity = Find(item.auth_id);
            if (entity == null)
            {
                _context.PartnerAuthTokens.Add(item);
                _context.SaveChanges();

                //attach resource reference
                _context.Entry(item).Reference(e => e.Resource).Load();
            }
            else
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

            //assign resource
            item.redirect_url = entity.redirect_url;
            item.Resource = entity.Resource;
            return item;
        }

        /// <summary>
        /// Method: Find
        /// Description: It is used to get PartnerAuthTokens by auth_id
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public PartnerAuthTokens Find(string key)
        {
            if (_context.PartnerAuthTokens.Where(p => p.auth_id == key).Count() > 0)
            {
                var entity = _context.PartnerAuthTokens.FirstOrDefault(p => p.auth_id == key);
                //get latest database value
                Reload(entity);

                return entity;
            }

            return null;
        }

        /// <summary>
        /// Method: Reload
        /// Description: It is used to reload PartnerAuthTokens entity
        /// </summary>
        /// <param name="entity"></param>
        public void Reload(PartnerAuthTokens entity)
        {
            if (entity != null)
            {
                _context.Entry(entity).Reload();
                _context.Entry(entity).Reference(e => e.Resource).Load();
            }
        }

        /// <summary>
        /// Method: Remove
        /// Description: It is used to delete PartnerAuthToken by ccid from PartnerAuthTokens table
        /// </summary>
        /// <param name="id"></param>
        public void Remove(string id)
        {
            //Get PartnerAuthTokens by auth_id
            var entity = Find(id);
            if (entity != null)
            {
                _context.Entry(entity).State = EntityState.Deleted;
                _context.SaveChanges();
            }
        }

        public void Update(PartnerAuthTokens item)
        {
            //Get PartnerAuthTokens by auth_id
            var entity = Find(item.auth_id);
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

        public bool IsValidResource(string key)
        {
            var resources = _context.Resources.FirstOrDefault(p => p.uuid == key);
            return (resources == null ? false : true);
        }

        public void UpdateReturnUrl(string authId, string returnUrl)
        {
            //Get AuthTokens by auth_id
            var entity = Find(authId);
            if (entity == null)
            {
                entity = new PartnerAuthTokens();
                entity.auth_id = authId;
                entity.access_token = "";
                entity.refresh_token = "";
                entity.expires_in = DateTime.MinValue;
                entity.oauth_expired_in = DateTime.MinValue;
                entity.redirect_url = returnUrl;
                _context.PartnerAuthTokens.Add(entity);
                _context.SaveChanges();
            }
            else
            {
                entity.redirect_url = returnUrl;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
        }

        public Resources GetResource(string key)
        {
            var entity = _context.Resources.FirstOrDefault(p => p.uuid == key);
            if (entity != null)
            {
                //reload resource
                _context.Entry(entity).Reload();
            }

            return entity;
        }

        private bool _disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _context.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~PartnerAuthTokenRepository()
        {
            Dispose(false);
        }
    }
}
