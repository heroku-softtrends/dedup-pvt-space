using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Dedup.Services;
using System.Threading.Tasks;
using Dedup.Repositories;
using Microsoft.Extensions.Logging;
using Dedup.ViewModels;
using System;
using Dedup.Extensions;
using System.Security.Claims;
using Microsoft.Extensions.Hosting;
using System.Collections.Generic;
using Dedup.Common;

namespace Dedup.ViewComponents
{
    [ViewComponent(Name = "Account")]
    public class Account : ViewComponent
    {
        private readonly ILogger _logger;
        private IResourcesRepository _resourcesRepository { get; set; }

        public Account(IResourcesRepository resourcesRepository, ILogger<Account> logger)
        {
            _resourcesRepository = resourcesRepository;
            _logger = logger;
        }

        /// <summary>
        /// Action: InvokeAsync
        /// Description: It is used to get the current user name of heroku account
        /// </summary>
        /// <returns></returns>
        public async Task<IViewComponentResult> InvokeAsync()
        {
            //Get username from session
            ViewBag.userName = HttpContext.GetClaimValue(ClaimTypes.Name);
            if (string.IsNullOrEmpty(ViewBag.userName)
                || string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.Email)))
            {
                //Get username using heroku api
                ViewBag.userName = await GetAccountInfoAsync();
            }

            return View();
        }

        /// <summary>
        /// Action: GetAccountInfoAsync
        /// Description: It is used to get the current user name of heroku account
        /// </summary>
        /// <returns></returns>
        private async Task<string> GetAccountInfoAsync()
        {
            try
            {
                var resourceId = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                if (!string.IsNullOrEmpty(resourceId))
                {
                    //validate account by resource id
                    var resources =await _resourcesRepository.Find(resourceId);
                    if (resources != null)
                    {
                        //Update name/email in resource table based on resource-id if null
                        if (string.IsNullOrEmpty(resources.user_organization) || string.IsNullOrEmpty(resources.user_name) || string.IsNullOrEmpty(resources.user_email))
                        {
                            //Get heroku auth token
                            var herokuAuthToken = HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN);
                            if (!string.IsNullOrEmpty(herokuAuthToken))
                            {
                                if (string.IsNullOrEmpty(resources.user_email) || string.IsNullOrEmpty(resources.app_name))
                                {
                                    //Get app info using heroku api
                                    var appInfo = HerokuApi.GetVendorAppInfoByResourceId(resources.uuid);
                                    if (!appInfo.IsNull())
                                    {
                                        if (string.IsNullOrEmpty(resources.app_name))
                                        {
                                            resources.app_name = appInfo.name;
                                        }

                                        if (string.IsNullOrEmpty(resources.user_email))
                                        {
                                            resources.user_email = appInfo.owner_email;
                                        }
                                    }
                                }

                                if (!string.IsNullOrEmpty(resources.app_name) && string.IsNullOrEmpty(resources.user_organization))
                                {
                                    //Get app info using heroku api
                                    var appInfo = HerokuApi.GetAppInfos(resources.app_name, herokuAuthToken);
                                    if (!appInfo.IsNull())
                                    {
                                        if (string.IsNullOrEmpty(resources.user_organization) && appInfo.organization.HasValue)
                                        {
                                            resources.user_organization = ((AppOrganization)appInfo.organization).name;
                                        }
                                    }
                                }

                                if (string.IsNullOrEmpty(resources.user_name))
                                {
                                    //Get username using heroku api
                                    var accInfo = HerokuApi.GetAccountInfo(herokuAuthToken);
                                    if (!accInfo.IsNull())
                                    {
                                        //Assign user name
                                        resources.user_name = accInfo.name;
                                    }
                                }

                                //Update user organization/name/email
                               await _resourcesRepository.Update(resources);
                            }
                        }

                        var claims = new List<Claim>();
                        if (!string.IsNullOrEmpty(resources.user_name) && HttpContext.GetClaimValue(ClaimTypes.Name) != resources.user_name)
                        {
                            claims.Add(new Claim(ClaimTypes.Name, resources.user_name));
                        }
                        if (!string.IsNullOrEmpty(resources.user_email) && HttpContext.GetClaimValue(ClaimTypes.Email) != resources.user_email)
                        {
                            claims.Add(new Claim(ClaimTypes.Email, resources.user_email));
                        }

                        if (claims != null && claims.Count > 0)
                        {
                            HttpContext.AddUpdateClaims(claims);
                        }

                        return await Task.FromResult(HttpContext.GetClaimValue(ClaimTypes.Name));
                    }
                }
            }
            catch (Exception ex)
            {
                if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == Environments.Development)
                    _logger.LogError(ex.Message, ex);
                else
                    Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return await Task.FromResult(string.Empty);
        }
    }
}