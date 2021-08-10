using System;
using Microsoft.AspNetCore.Mvc;
using Dedup.ViewModels;
using Dedup.Extensions;
using System.Net;
using Dedup.Repositories;
using Dedup.Common;
using Newtonsoft.Json;
using Dedup.HttpFilters;

namespace Dedup.Controllers
{
    [Route("heroku/resources")]
    [HerokuAuthorizationFilter]
    public class HerokuController : Controller
    {
        private IResourcesRepository _resourcesRepository { get; set; }

        public HerokuController(IResourcesRepository resourcesRepository)
        {
            _resourcesRepository = resourcesRepository;
        }

        /// <summary>
        /// Action: Add
        /// Description: When the addon is going to provision, It is called by client. It will insert a record in resources table
        /// If Addon provisioned successfully then it will return unique resource id else
        /// it will return -1.
        /// </summary>
        /// <param name="resource"></param>
        /// <returns>unique guid/-1</returns>
        [HttpPost]
        public IActionResult Add([FromBody] ResourceViewModel resource)
        {
            string provisionError = string.Empty;
            try
            {
                if (ModelState.IsValid)
                {
                    if (resource.oauth_grant.HasValue)
                    {
                        Console.WriteLine("Input resource: " + JsonConvert.SerializeObject(resource));
                        Console.WriteLine("Get plan info by plan {0}", resource.plan);
                        var addonPlan = Utilities.GetAddonPlanByPlan(resource.plan);
                        Console.WriteLine("Addon plan info fetched");
                        if (!addonPlan.IsNull() && addonPlan.is_active)
                        {
                            var lResources = resource.ToResources();

                            //assign name level, we are going with name internally instead of name
                            lResources.plan = addonPlan.level.ToString();

                            //insert resource 
                            _resourcesRepository.Add(lResources, resource.oauth_grant);

                            //return resource id when getting successful provision 
                            var addonProvisionVM = new AddonProvisionViewModel();
                            addonProvisionVM.id = lResources.uuid;
                            if (addonPlan.is_private_space)
                            {
                                addonProvisionVM.message = Environment.GetEnvironmentVariable("PVT_PROVISION_MSG");
                            }
                            else
                            {
                                addonProvisionVM.message = ConfigVars.Instance.DFT_PROVISION_MSG;
                            }
                            Console.WriteLine("Addon provision response: {0}", JsonConvert.SerializeObject(addonProvisionVM));
                            Response.StatusCode = (int)HttpStatusCode.OK;
                            return Json(addonProvisionVM);
                        }
                        else
                        {
                            Console.WriteLine($"Not able to load addon {addonPlan.name} plan details.");
                            Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        }
                    }
                    else
                    {
                        Console.WriteLine("OAuth-Grant is null");
                        Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    }
                }
                else
                {
                    Console.WriteLine("Input parameter(s) not valid");
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }

            return Json(new { id = -1, message = ConfigVars.Instance.PROVISION_ERR_MSG });
        }

        /// <summary>
        /// Action: Update
        /// Description: When the addon is going to change the plan/renewing the existing plan/change the existing region, It is called by client.
        /// It will update resorce record in resources table. If it went successfully then it will return empty always but http staus code
        /// will vary based on the success or fail.
        /// </summary>
        /// <param name="id">ResourceViewModel ID</param>
        /// <param name="resource"></param>
        /// <returns>{}</returns>
        [HttpPut("{id}")]
        public IActionResult Update(string id, [FromBody] ResourceViewModel resource)
        {
            Console.WriteLine("Heroku Controller- Update");
            try
            {
                if (ModelState.IsValid && !string.IsNullOrEmpty(id) && !string.IsNullOrEmpty(resource.plan))
                {
                    //get addon plan based on plan name
                    var addonPlan = Utilities.GetAddonPlanByPlan(resource.plan);
                    if (!addonPlan.IsNull() && addonPlan.is_active)
                    {
                        //update resource
                        _resourcesRepository.UpdatePlan(id, addonPlan.level.ToString());
                    }
                    else
                    {
                        Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    }
                }
                else
                {
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }

            return Json(new { });
        }

        /// <summary>
        /// Action: Delete
        /// Description: When the addon is going to deprovision, It is called by client. It will delete resource record from resources table
        /// and also do the casecade delete in other tables by resorce id
        /// </summary>
        /// <param name="id">resource id</param>
        /// <returns>{}</returns>
        [HttpDelete("{id}")]
        public IActionResult Delete(string id)
        {
            Console.WriteLine("Heroku Controller- Delete");
            try
            {
                if (!string.IsNullOrEmpty(id))
                {
                    //delete resource
                    _resourcesRepository.Remove(id);
                    Console.WriteLine("Heroku Controller- Delete - success");
                    Response.StatusCode = (int)HttpStatusCode.NoContent;
                }
                else
                {
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            return Json(new { });
        }

        protected override void Dispose(bool disposing)
        {
            _resourcesRepository.Dispose();
            base.Dispose(disposing);
        }
    }
}
