using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Dedup.Extensions;
using Dedup.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using Dedup.Services;
using Dedup.ViewModels;
using Dedup.Common;

namespace Dedup.HttpFilters
{
    public class UserRoleFilter : ActionFilterAttribute
    {
        private readonly IResourcesRepository _resourcesRepository;
        public UserRoleFilter(IResourcesRepository resourcesRepository)
        {
            _resourcesRepository = resourcesRepository;
        }

        /// <summary>
        /// ActionFilter: OnActionExecuting
        /// Description: It is used to check current user role where admin or owner for allowing user to access resources based on role.
        /// </summary>
        /// <param name="context"></param>
        public override void OnActionExecuting(ActionExecutingContext context)
        {
            base.OnActionExecuting(context);
            if (string.IsNullOrEmpty(context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_MAIN_APP_NAME)))
            {
                //local variables
                var claims = new List<Claim>();
                string orgId = string.Empty;
                string appId = string.Empty;
                string authToken = string.Empty;
                string resourceId = string.Empty;

                //get resourceId
                resourceId = context.HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);

                //get heroku auth token
                authToken = context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_ACCESS_TOKEN);

                //get app id using resourceId
                appId = HerokuApi.GetHerokuAppIdByAddonId(context.HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), authToken).Result;
                if (!string.IsNullOrEmpty(appId))
                {
                    //get app info using app id
                    AppInfo appInfo = HerokuApi.GetAppInfos(appId, authToken);
                    if (appInfo.IsNull())
                    {
                        claims = null;
                        authToken = null;
                        Console.WriteLine("UPF-appInfo is null by {0}", resourceId);
                        context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                        return;
                    }
                    claims.Add(new Claim(Dedup.Common.Constants.HEROKU_MAIN_APP_NAME, appInfo.name));
                    claims.Add(new Claim(Dedup.Common.Constants.HEROKU_MAIN_APP_ID, appInfo.id));
                    if (appInfo.organization.HasValue)
                    {
                        orgId = appInfo.organization?.id;
                        claims.Add(new Claim(Dedup.Common.Constants.HEROKU_ORG_ID, appInfo.organization?.id));
                        claims.Add(new Claim(Dedup.Common.Constants.HEROKU_ORG_NAME, appInfo.organization?.name));
                    }

                    var resourceEntity = _resourcesRepository.FindResources(resourceId);
                    if (resourceEntity != null)
                    {
                        bool isUpdate = false;
                        if (!appInfo.name.Equals(resourceEntity.app_name, StringComparison.OrdinalIgnoreCase))
                        {
                            resourceEntity.app_name = appInfo.name;
                            isUpdate = true;
                        }
                        if (!appInfo.id.Equals(resourceEntity.heroku_id, StringComparison.OrdinalIgnoreCase))
                        {
                            resourceEntity.heroku_id = appInfo.id;
                            isUpdate = true;
                        }
                        if (!appInfo.owner.IsNull() && !appInfo.owner.email.Equals(resourceEntity.user_email, StringComparison.OrdinalIgnoreCase))
                        {
                            resourceEntity.user_email = appInfo.owner.email;
                            isUpdate = true;
                        }
                        if (appInfo.organization.HasValue && !appInfo.organization.Value.name.Equals(resourceEntity.user_organization, StringComparison.OrdinalIgnoreCase))
                        {
                            resourceEntity.user_organization = appInfo.organization?.name;
                            isUpdate = true;
                        }
                        if (isUpdate)
                        {
                            _resourcesRepository.UpdateAppAndUserInfo(resourceEntity);
                        }
                    }

                    //update user identity
                    if (claims != null && claims.Count() > 0)
                    {
                        context.HttpContext.AddUpdateClaims(claims);
                    }
                }
                claims = null;
            }
        }
    }
}
