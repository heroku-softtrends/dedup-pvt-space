using System;
using System.Collections.Generic;
using System.Linq;
using Dedup.Models;
using Dedup.Data;
using Microsoft.EntityFrameworkCore;
using Dedup.Services;
using Dedup.ViewModels;
using Dedup.Extensions;
using Newtonsoft.Json;
using System.Threading.Tasks;

namespace Dedup.Repositories
{
    public class ResourcesRepository : IResourcesRepository
    {
        private DeDupContext _context;
        public ResourcesRepository(DeDupContext context)
        {
            _context = context;
        }

        /// <summary>
        /// Method: Add
        /// Description: It is used to add new resource to resources table when provisioning the addon
        /// </summary>
        /// <param name="item"></param>
        public void Add(Resources item, OauthGrant? oauthGrant)
        {
            using (var transaction = _context.Database.BeginTransaction())
            {
                try
                {
                    Console.WriteLine("Resource: " + JsonConvert.SerializeObject(item));
                    Console.WriteLine("Resource add starts");
                    _context.Resources.Add(item);
                    Console.WriteLine("Resource add ended");

                    HerokuAuthToken authToken = default(HerokuAuthToken);
                    if (oauthGrant.HasValue)
                    {
                        Console.WriteLine("Auth-Token get starts");
                        authToken = HerokuApi.GetAddonAccessTokenSync(oauthGrant.Value.code, oauthGrant.Value.type);
                        if (!authToken.IsNull())
                        {
                            Console.WriteLine("Auth-Token=> {0}:{1}", authToken.access_token, authToken.refresh_token);
                        }
                        else
                        {
                            throw new ArgumentNullException("Heroku access token not able to get");
                        }
                        Console.WriteLine("Auth-Token get ended");
                    }
                    else
                    {
                        Console.WriteLine("OAuth-Grant is null");
                        throw new ArgumentNullException("OAuth-Grant is null");
                    }

                    authToken.auth_id = item.uuid;
                    PartnerAuthTokens pAuthToken = authToken.ToPartnerAuthToken();
                    pAuthToken.oauth_code = oauthGrant.Value.code;
                    pAuthToken.oauth_type = oauthGrant.Value.type;
                    pAuthToken.oauth_expired_in = DateTime.Now.AddSeconds(280);
                    Console.WriteLine("AuthToken: " + JsonConvert.SerializeObject(pAuthToken));
                    Console.WriteLine("AuthToken add starts");
                    _context.PartnerAuthTokens.Add(pAuthToken);
                    Console.WriteLine("AuthToken add ended");
                    _context.SaveChanges();
                    transaction.Commit();
                    if (!authToken.IsNull())
                    {
                        var task = HerokuApi.AddUpdateMainAppConfigByResourceId(item.uuid, authToken.access_token);
                        task.Wait();
                    }
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Method: Add
        /// Description: It is used to add new resource to resources table when provisioning the addon
        /// </summary>
        /// <param name="item"></param>
        public void Add(Resources item, int expiryInDays, bool isPrivatePlan, HerokuAuthToken authToken)
        {
            using (var transaction = _context.Database.BeginTransaction())
            {
                try
                {
                    //Get vendor app info
                    Console.WriteLine("Get vendor app info starts");
                    var appInfo = HerokuApi.GetAppInfos(item.app_name, authToken.access_token);
                    Console.WriteLine("Get vendor app info ended");
                    if (appInfo.IsNull())
                    {
                        throw new ArgumentNullException("Main app info is null");
                    }

                    item.app_name = appInfo.name;
                    item.heroku_id = appInfo.id;
                    if (!appInfo.owner.IsNull())
                        item.user_email = appInfo.owner.email;
                    if (appInfo.organization.HasValue)
                        item.user_organization = appInfo.organization.Value.name;
                    if (!appInfo.region.IsNull())
                        item.region = appInfo.region.name;
                    if (isPrivatePlan && (!appInfo.space.HasValue || (appInfo.space.HasValue && string.IsNullOrEmpty(appInfo.space.Value.name))))
                    {
                        throw new Exception(string.Format("The {0} plan is not supported for the user account.", item.plan));
                    }

                    //set plan expiry date based on plan configured in addon_plans.json which is in app root
                    item.expired_at = DateTime.UtcNow.AddDays(expiryInDays);
                    _context.Resources.Add(item);

                    AuthTokens authTokens = authToken.ToAuthToken();
                    _context.AuthTokens.Add(authTokens);
                    _context.SaveChanges();

                    transaction.Commit();

                    //Update addon app config-var
                    if (!authToken.IsNull())
                    {
                        var task = HerokuApi.UpdateVendorAppConfigVarByResourceId(item.uuid, authToken.access_token);
                        task.Wait();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                    transaction.Rollback();
                    throw;
                }
            }
        }
 

        /// <summary>
        /// Method: Find
        /// Description: It is used to get resource by resourceId fro resources table
        /// </summary>
        /// <param name="key"></param>
        /// <returns>Resources</returns>
        //public Resources Find(string key)
        //{
        //    var entity = _context.Resources.FirstOrDefault(p => p.uuid == key);
        //    if (entity != null)
        //    {
        //        //attach resource reference
        //        //_context.Entry(entity).Reference(e => e.AuthToken).Load();
        //        //Shanmugam modified AuthToken to partnerAuthToken
        //        _context.Entry(entity).Reference(e => e.partnerAuthToken).Load();

        //        return entity;
        //    }

        //    return null;
        //}

        /// <summary>
        /// Method: GetAll
        /// Description: It is used to get all resources from resources table
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Resources> GetAll()
        {
            return _context.Resources.ToList();
        }

        /// <summary>
        /// Method: Remove
        /// Description: It is used to delete resource entry from resources table when de-provisioning addon
        /// </summary>
        /// <param name="id"></param>
        public void Remove(string id)
        {
            //try
            //{
            //    if (_context.Connectors.Where(p => p.ccid == id).Count() > 0)
            //    {
            //        foreach (var connector in _context.Connectors.Where(p => p.ccid == id).Select(p => p.ToModel<ConnectorConfig>(false)))
            //        {
            //            if (!string.IsNullOrEmpty(connector.jobId))
            //            {
            //                JobScheduler.Instance.DeleteJob(connector.jobId, connector.scheduleType, connector.syncStatus);
            //            }
            //        }
            //    }
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("Error: {0}", ex.Message);
            //}

            //var entity = Find(id);
            //_context.Entry(entity).State = EntityState.Deleted;
            //_context.SaveChanges();
            Resources entity = null;
            try
            {
                entity = _context.Resources.FirstOrDefault(p => p.uuid == id);
                var connectors = _context.Connectors.Where(p => p.ccid == id).ToArray();
                if (connectors != null && connectors.Length > 0)
                {
                    //var privateAppUrl = entity.private_app_url;

                    //cancel token
                    Task.Run(() =>
                    {
                        for (int i = 0; i < connectors.Length; ++i)
                        {
                            if (!string.IsNullOrEmpty(connectors[i].job_id))
                            {
                                JobScheduler.Instance.DeleteJob(ccid: connectors[i].ccid, connectorId: connectors[i].connector_id, jobId: connectors[i].job_id, scheduleType: connectors[i].schedule_type);
                            }
                        }
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
            }

            if (entity != null)
            {
                ConnectorsRepository connectorsRepository = new ConnectorsRepository(_context);
                connectorsRepository.DeletectindexConfigById(entity.uuid, 0, false);
                _context.Entry(entity).State = EntityState.Deleted;
                _context.SaveChanges();
            }
        }


        /// <summary>
        /// Method: UpdatePlan
        /// Description: It is used to update resource entry on resources table by resourceId
        /// </summary>
        /// <param name="id"></param>
        /// <param name="plan"></param>
        public async Task UpdatePlan(string id, string plan)
        {
            //Get resource by resourceId
            var entity =await Find(id);

            //Assign updating properties
            entity.plan = plan;
            _context.Entry(entity).State = EntityState.Modified;
            _context.SaveChanges();
        }

        /// <summary>
        /// Method: Update
        /// Description: It is used to update resource entry on resources table by resourceId
        /// </summary>
        /// <param name="item"></param>
        public async Task Update(Resources item)
        {
            //Get resource by resourceId
            var entity =await Find(item.uuid);

            //Assign updating properties
            entity.plan = item.plan;
            if (string.IsNullOrEmpty(item.app_name))
            {
                //Assign app name
                entity.app_name = item.app_name;
            }
            if (string.IsNullOrEmpty(item.user_email))
            {
                //Assign emailkdsss
                entity.user_email = item.user_email;
            }
            //entity.app_name = item.app_name;
            //entity.region = item.region;
            _context.Entry(entity).State = EntityState.Modified;
            _context.SaveChanges();
        }

        public async Task UpdateAppAndUserInfo(Resources item)
        {
           
            var entity = await Find(item.uuid);

            //Assign updating properties
            entity.user_organization = item.user_organization;
            entity.user_email = item.user_email;
            entity.user_name = item.user_name;
            entity.heroku_id = item.heroku_id;
            entity.app_name = item.app_name;
            _context.Entry(entity).State = EntityState.Modified;
            _context.SaveChanges();

        }

        public async Task UpdateLicenseAgreement(string id, bool isLicenseAccepted)
        {
            var entity =await Find(id);
            if (entity != null)
            {
                entity.is_license_accepted = isLicenseAccepted;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
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
        /// <summary>
        /// Method: Find
        /// Description: It is used to get resource by resourceId fro resources table
        /// </summary>
        /// <param name="key"></param>
        /// <returns>Resources</returns>
        public async Task<Resources> Find(string key, bool isReload = false)
        {
            if (await _context.Resources.Where(p => p.uuid == key).CountAsync() > 0)
            {
                var entity = await _context.Resources.FirstOrDefaultAsync(p => p.uuid == key);

                if (isReload)
                {
                    //attach resource reference
                    _context.Entry(entity).Reference(e => e.AuthToken).Load();
                }

                return entity;
            }

            return null;
        }

        public Resources FindResources(string key, bool isReload = false)
        {
            if (_context.Resources.Where(p => p.uuid == key).Count() > 0)
            {
                var entity = _context.Resources.FirstOrDefault(p => p.uuid == key);

                if (isReload)
                {
                    //attach resource reference
                    _context.Entry(entity).Reference(e => e.AuthToken).Load();
                }

                return entity;
            }

            return null;
        }
        public async Task UpdateResourcePrivateUrl(string key, string privateAppUrl)
        {
            if (!string.IsNullOrEmpty(privateAppUrl)
                && !string.IsNullOrEmpty(key))
            {
                //Get resource by resourceId
                var entity =await Find(key);

                //update app url
                entity.private_app_url = privateAppUrl;
                _context.Entry(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
        }
        /// <summary>
        /// Method: Update
        /// Description: It is used to update resource entry on resources table by resourceId
        /// </summary>
        /// <param name="item"></param>
        public async Task UpdateAppName(string id, string appName)
        {
            if (!string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(appName))
            {
                //Get resource by resourceId
                var entity =await Find(id);
                if (entity != null)
                {
                    if (string.IsNullOrEmpty(entity.app_name) || (!string.IsNullOrEmpty(entity.app_name) && !appName.Equals(entity.app_name, StringComparison.OrdinalIgnoreCase)))
                    {
                        //Assign app_name
                        entity.app_name = appName;
                        _context.Entry(entity).State = EntityState.Modified;
                        _context.SaveChanges();
                    }
                }
            }
        }

        //void IResourcesRepository.UpdatePlan(string id, string plan)
        //{
        //    throw new NotImplementedException();
        //}

        ~ResourcesRepository()
        {
            Dispose(false);
        }
    }
}
