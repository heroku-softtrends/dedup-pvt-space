using Dedup.Extensions;
using Dedup.Common;
using Dedup.Repositories;
using Dedup.ViewModels;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using System;
using System.Linq;
using System.Security.Claims;

namespace Dedup.HttpFilters
{
    public class AddonPlanFilter : ActionFilterAttribute
    {
        private readonly IResourcesRepository _resourcesRepository;
        public AddonPlanFilter(IResourcesRepository resourcesRepository)
        {
            _resourcesRepository = resourcesRepository;
        }

        /// <summary>
        /// ActionFilter: OnActionExecuting
        /// Description: It is used to check the current addon plan for the subscription is expired or not.
        /// If not expired then allow to use the features of the current plan else allow to use the features of test/free plan
        /// </summary>
        /// <param name="context"></param>
        public override void OnActionExecuting(ActionExecutingContext context)
        {
            base.OnActionExecuting(context);
            if (context.Controller != null)
            {
                var controller = context.Controller as Controller;
                var uuid = context.HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                var isLoadDefaultPlan = true;


                if (!string.IsNullOrEmpty(uuid))
                {
                    //Get resource by resourceId
                    var resource =_resourcesRepository.FindResources(uuid);
                    if (resource == null)
                    {
						Console.WriteLine("APF-Resource is null by {0}", uuid);
                        context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                        return;
                    }
                    else if (resource != null && !string.IsNullOrEmpty(resource.plan))
                    {
                        //no need to load default plan
                        isLoadDefaultPlan = false;

                        //Get the default/free/test plan features from addon_plans.json
                        controller.ViewBag.CurrentPlan = Utilities.GetAddonPlanByLevel(resource.plan);
						//set license acceptance
                        if (controller.ViewBag.CurrentPlan.IsInitialized && resource.is_license_accepted.HasValue)
                        {
                            controller.ViewBag.CurrentPlan.isLicenseAccepted = resource.is_license_accepted.Value;
                        }
                    }
                }

                if (isLoadDefaultPlan)
                {
                    //Get the default/free/test plan features from addon_plans.json
                    controller.ViewBag.CurrentPlan = Utilities.GetAddonPlanByLevel(Dedup.Common.Constants.FREE_PLAN_LEVEL.ToString());
                }
            }
        }
    }
}
