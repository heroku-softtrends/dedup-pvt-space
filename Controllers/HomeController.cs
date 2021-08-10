using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Dedup.Repositories;
using Dedup.ViewModels;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;
using System.Net;
using System.IO;
using System.Linq;
using Dedup.Common;
using Dedup.Extensions;
using System.Security.Claims;
using Dedup.HttpFilters;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Dedup.Services;


namespace Dedup.Controllers
{
    public class HomeController : Controller
    {
        private readonly IConnectorsRepository _connectorsRepository;
        private readonly IResourcesRepository _resourcesRepository;
        public HomeController(IConnectorsRepository connectorsRepository, IResourcesRepository resourcesRepository)
        {
            _connectorsRepository = connectorsRepository;
            _resourcesRepository = resourcesRepository;
        }

        /// <summary>
        /// Action: Index
        /// Description: It is called to get all connectors to display on home page.
        /// </summary>
        /// <returns>List<ConnectorConfig></returns>
        [LoginAuthorizeAttribute]
        [TypeFilter(typeof(AddonPlanFilter))]
        [TypeFilter(typeof(UserRoleFilter))]
        public async Task<IActionResult> Index()
        {
            List<ConnectorConfig> ConnectorConfigs = null;

            try
            {
                if (ViewBag.CurrentPlan.IsInitialized)
                {
                    //if (!ConfigVars.Instance.deDupPvtEdition)
                    //{
                    //    //Check current plan is private or not.
                    //    ViewBag.CurrentPlan.is_private_space = true;
                    //    if (ViewBag.CurrentPlan.IsInitialized && ViewBag.CurrentPlan.is_private_space)
                    //    {
                    //        string pversion = string.Empty;
                    //        //Get resource by resourceId
                    //        var resource =await _resourcesRepository.Find(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
                    //        if (resource != null && !string.IsNullOrEmpty(resource.app_name))
                    //        {
                    //            //var isODBC = false;
                    //            PlanInfos planInfo = (PlanInfos)ViewBag.CurrentPlan;
                    //            //if (resource.plan == "11" || resource.plan == "13" || resource.plan == "15")
                    //            //if (planInfo.isRedShiftSupport)
                    //            //{
                    //            //    isODBC = true;
                    //            //    Console.WriteLine("Is ODBC: {0}", isODBC);
                    //            //}

                    //            //Get Latest Version by plan
                    //            //if (!string.IsNullOrEmpty(version) && !string.IsNullOrEmpty(resource.plan) && ConfigVars.Instance.versions != null && ConfigVars.Instance.versions.Count > 0)
                    //            //{
                    //            //    ViewModels.Version lversion = ConfigVars.Instance.versions.Where(x => x.plan == Convert.ToInt32(resource.plan) && x.version == version.Trim()).OrderByDescending(x => x.id).FirstOrDefault();
                    //            //    if (!lversion.IsNull())
                    //            //    {
                    //            //        pversion = lversion.version;
                    //            //        Console.WriteLine("Version from query string: {0}", lversion.version);
                    //            //        ConfigVars.Instance.mcPvtAppVersion = lversion.version;
                    //            //        ConfigVars.Instance.mcBuildGitRepoUrl = string.Format("{0}/{1}/{2}/{3}", ConfigVars.Instance.GIT_REPO_BASE_URL, lversion.private_addon_git_repository_branch_name, lversion.version, "build.tar.gz");
                    //            //        Console.WriteLine("Github Base URL: {0}", ConfigVars.Instance.mcBuildGitRepoUrl);
                    //            //    }
                    //            //}
                    //            //else if (string.IsNullOrEmpty(version) && !string.IsNullOrEmpty(resource.plan))
                    //            //{
                    //            //    ViewModels.Version lversion = ConfigVars.Instance.versions.Where(x => x.plan == Convert.ToInt32(resource.plan)).OrderByDescending(x => x.id).FirstOrDefault();
                    //            //    if (!lversion.IsNull())
                    //            //    {
                    //            //        Console.WriteLine("Version from version table: {0}", lversion.version);
                    //            //        ConfigVars.Instance.mcPvtAppVersion = lversion.version;
                    //            //        ConfigVars.Instance.mcBuildGitRepoUrl = string.Format("{0}/{1}/{2}/{3}", ConfigVars.Instance.GIT_REPO_BASE_URL, lversion.private_addon_git_repository_branch_name, lversion.version, "build.tar.gz");
                    //            //        Console.WriteLine("Github Base URL: {0}", ConfigVars.Instance.mcBuildGitRepoUrl);
                    //            //    }
                    //            //}
                    //            //Get app by app name
                    //            var appInfo = await HerokuApi.GetAppInfo(resource.app_name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN));
                    //            if (!appInfo.IsNull() && appInfo.space.HasValue)
                    //            {
                    //                //If the plan is private then check mc-pvt app exist or not, if not then create the dedup-pvt app first then
                    //                //else get the url of dedup-pvt app and redirect to it directly.
                    //                // Console.WriteLine("Is ODBC inside appinfo not null: {0}", isODBC);
                    //                var pvtDeDupUrl = await DeployPrivateDeDupApp(appInfo, resource, pversion);//, isODBC: isODBC);

                    //                //redirect to private app if it's available
                    //                if (!string.IsNullOrEmpty(pvtDeDupUrl))
                    //                {
                    //                    Console.WriteLine("Redirecting to private space app url: {0}", pvtDeDupUrl);
                    //                    return Redirect(pvtDeDupUrl);
                    //                }
                    //            }
                    //        }
                    //    }
                    //}
                    if (ViewBag.CurrentPlan.isLicenseAccepted)
                    {
                        //Get all connectors from connectors table by ccid
                        ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, null);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return View(await Task.FromResult(ConnectorConfigs));
        }
        [NonAction]
        private async Task<string> DeployPrivateDeDupApp(AppInfo appInfo, Models.Resources resource, string pversion = "")//, bool isODBC = false)
        {
            string webUrl = string.Empty;
            try
            {
                AppInfo dedupAppInfo = default(AppInfo);
                AppSetupInfo appSetupInfo = default(AppSetupInfo);
                string errMsg = string.Empty;

                //Publish dedup-pvt app
                dedupAppInfo = await HerokuApi.PublishDeDupAppInPrivateSpace(resource.uuid, resource.region, resource.user_organization,
                                Constants.PRIVATE_ADDON_APP_NAME, appInfo.space.Value.name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ref appSetupInfo, out errMsg);//, isODBC: isODBC);

                if (!string.IsNullOrEmpty(errMsg) && errMsg != "succeeded")
                {
//TempData["msg"] = "<script>Helpers.showAlert('DeDup Release has failed. You can try after sometime and if you continue to face this issue, Please contact Heroku add-on support for help.','Error');</script>";
                    TempData["msg"] = "<script>Swal.fire('','DeDup Release has failed. You can try after sometime and if you continue to face this issue, Please contact Heroku add-on support for help.','error');</script>";

                }

                if (!appSetupInfo.IsNull())
                {
                    HttpContext.Session.SetString("PvtAppBuildId", appSetupInfo.id);
                }

                if (!string.IsNullOrEmpty(HttpContext.Session.GetString("PvtAppBuildId")))
                {
                    //Get dedup-pvt app published status by buildId
                    appSetupInfo = await HerokuApi.GetPublishedAppStatusInPrivateSpace(HttpContext.Session.GetString("PvtAppBuildId"), HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), out errMsg);
                    if (!appSetupInfo.IsNull() && appSetupInfo.status == "succeeded" && !string.IsNullOrEmpty(appSetupInfo.resolved_success_url))
                    {
                        HttpContext.Session.Remove("PvtAppBuildId");

                        //Set dedup pvt app url for redirect
                        webUrl = string.Format("{0}/sso/auth/{1}", appSetupInfo.resolved_success_url.TrimEnd('/'), resource.uuid);

                        if (!string.IsNullOrEmpty(webUrl))
                        {
                            //Update dedup pvt app url in resources table for the resourceId
                            await _resourcesRepository.UpdateResourcePrivateUrl(resource.uuid, webUrl);
                        }

                        //Enable ACM
                        //await HerokuApiService.EnableAppACMById(appSetupInfo.app.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN));
                    }
                    else if (!string.IsNullOrEmpty(errMsg))
                    {
                        TempData["msg"] = "<script>Swal.fire('','DeDup Release has failed. You can try after sometime and if you continue to face this issue, Please contact Heroku add-on support for help.','error');</script>";
                    }
                    else
                    {
                        TempData["msg"] = "<script>Swal.fire('','DeDup add-on is being configured. It may be unavailable for 15 minutes while DNS and other resources are configured for the first deployment. Please use DeDup after 15 minutes.','info');</script>";
                    }
                }
                else if (!dedupAppInfo.IsNull())
                {
                    HttpContext.Session.Remove("PvtAppBuildId");
                    webUrl = string.Format("{0}/sso/auth/{1}", dedupAppInfo.web_url.TrimEnd('/'), resource.uuid);

                    //Get app latest build by app name
                    var appBuild = await HerokuApi.GetAppLatestBuild(dedupAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN));

                    //Check app build version is same or not. If not then create new build with latest source available in git repository
                    //else redirect to mc-pvt url
                    if (!appBuild.IsNull() && !appBuild.source_blob.IsNull() && !string.IsNullOrEmpty(appBuild.source_blob.version))
                    {
                        if ((appBuild.status == "failed" && appBuild.source_blob.version == ConfigVars.Instance.deDupPvtAppVersion))
                        {

                            //update the stack version if its lower then the value configured in configvars table in db
                            if (!dedupAppInfo.stack.IsNull() && !string.IsNullOrEmpty(dedupAppInfo.stack.name) && dedupAppInfo.stack.name != ConfigVars.Instance.dedupPvtAppStackVersion)
                            {
                                HerokuApi.UpdateHerokuappStackVersion(dedupAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ConfigVars.Instance.dedupPvtAppStackVersion);
                            }
                            //Create new build from git repository
                            appBuild = await HerokuApi.CreateAppBuild(dedupAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ConfigVars.Instance.deDupPvtAppVersion);//, isODBC: isODBC);
                        }
                        else if (appBuild.source_blob.version != ConfigVars.Instance.deDupPvtAppVersion && !string.IsNullOrEmpty(pversion))
                        {
                            TempData.PutValue("appInfo", appInfo);
                            TempData["msg"] = "<script>upgradeVersion();</script>";
                            //TempData["msg"] = "<script>Helpers.showConfirm('A new version of Marketing Connector is available now. Select Upgrade button to upgrade to new version now or Cancel to upgrade later.', function yes() { upgradeVersion(); }, function no() { location.href='" + webUrl + "'; }, 'Upgrade', 'Upgrade Marketing Connector?');</script>";
                            return null;
                        }
                        else if (appBuild.source_blob.version != ConfigVars.Instance.deDupPvtAppVersion)
                        {
                            TempData.PutValue("appInfo", appInfo);
                            TempData["msg"] = "<script>SweetAlertConfirm('" + webUrl + "');</script> ";
                           // TempData["msg"] = "<script>Swal.fire({title: 'Upgrade DeDup?',text: 'A new version of DeDup is available now. Select Upgrade button to upgrade to new version now or Cancel to upgrade later.',showDenyButton: true,showCancelButton: false,confirmButtonText: 'Upgrade',denyButtonText: 'Cancel',}).then((result) => { if (result.isConfirmed) { upgradeVersion(); } else if (result.isDenied) { location.href = '" + webUrl + "'; }});</script> ";
                          //  TempData["msg"] = "<script>showConfirm('A new version of DeDup is available now. Select Upgrade button to upgrade to new version now or Cancel to upgrade later.', function yes() { upgradeVersion(); }, function no() { location.href='" + webUrl + "'; }, 'Upgrade', 'Upgrade DeDup?');</script>";
                            return null;
                        }
                    }

                    //redirect if app build status succeeded
                    if (!appBuild.IsNull())
                    {
                        if (appBuild.status == "failed")
                        {
                            TempData["msg"] = "<script>Swal.fire('','DeDup Release has failed. You can try after sometime and if you continue to face this issue, Please contact Heroku add-on support for help.','error');</script>";
                        }
                        else
                        {
                            if (appBuild.status == "succeeded")
                            {
                                var configVars = await HerokuApi.GetHerokuAppConfigVars(dedupAppInfo.name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                                string appName = string.Empty, queueName = string.Empty;
                                // string AMAZONREDSHIFTODBCINI = string.Empty, LD_LIBRARY_PATH = string.Empty, ODBCINI = string.Empty, ODBCSYSINI = string.Empty;
                                if (configVars != null)
                                {
                                    configVars.TryGetValue("DEDUP_PROXY_APP", out appName);
                                    configVars.TryGetValue("JOB_QUEUE_NAME", out queueName);
                                }
                                if (string.IsNullOrEmpty(appName) || appName == "dedup-addon-proxy" || appName == "dedup-connect-addon"
                                    || string.IsNullOrEmpty(queueName) || queueName == "pvt_critical" || queueName == "critical")
                                {
                                    //Update app config-vars
                                    await HerokuApi.AddUpdatePrivateAppConfigvars(dedupAppInfo.name, dedupAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                                }
                                //if (isODBC && configVars != null)
                                //{
                                //    configVars.TryGetValue("AMAZONREDSHIFTODBCINI", out AMAZONREDSHIFTODBCINI);
                                //    configVars.TryGetValue("LD_LIBRARY_PATH", out LD_LIBRARY_PATH);
                                //    configVars.TryGetValue("ODBCINI", out ODBCINI);
                                //    configVars.TryGetValue("ODBCSYSINI", out ODBCSYSINI);

                                //    if (string.IsNullOrEmpty(AMAZONREDSHIFTODBCINI) || string.IsNullOrEmpty(LD_LIBRARY_PATH) || string.IsNullOrEmpty(ODBCINI) || string.IsNullOrEmpty(ODBCSYSINI))
                                //    {
                                //        AMAZONREDSHIFTODBCINI = ConfigVars.Instance.amazon_redshift_odbc_ini_path;
                                //        LD_LIBRARY_PATH = ConfigVars.Instance.ld_library_path;
                                //        ODBCINI = ConfigVars.Instance.odbc_ini_path;
                                //        ODBCSYSINI = ConfigVars.Instance.odbc_sys_ini_path;
                                //        //Update app config-vars
                                //        await HerokuApiService.AddUpdatePrivateAppODBCConfigvars(mcAppInfo.name, mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), AMAZONREDSHIFTODBCINI, LD_LIBRARY_PATH, ODBCINI, ODBCSYSINI).ConfigureAwait(false);
                                //    }
                                //}
                            }
                            else
                            {
                                webUrl = string.Empty;
                            }

                            TempData["msg"] = "<script>Swal.fire('','DeDup add-on is being updated, Please use DeDup after 5 minutes','Info');</script>";
                        }
                    }

                    if (!string.IsNullOrEmpty(webUrl) && resource.private_app_url != webUrl)
                    {
                        //Update dedup-pvt url
                        await _resourcesRepository.UpdateResourcePrivateUrl(resource.uuid, webUrl);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return webUrl;
        }

        /// <summary>
        /// Action: Index
        /// Description: It is called to get all connectors to display on home page.
        /// </summary>
        /// <returns>List<ConnectorConfig></returns>
        [LoginAuthorizeAttribute]
        [TypeFilter(typeof(AddonPlanFilter))]
        public async Task<JsonResult> Upgrade()
        {
            string webUrl = string.Empty;
            string status = string.Empty;
            try
            {
                if (!ConfigVars.Instance.deDupPvtEdition)
                {
                    //Check current plan is private or not.
                    if (ViewBag.CurrentPlan.IsInitialized && ViewBag.CurrentPlan.is_private_space)
                    {
                        //Get resource by resourceId
                        var resource =await _resourcesRepository.Find(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
                        if (resource != null && !string.IsNullOrEmpty(resource.app_name))
                        {
                            //var isODBC = false;
                            PlanInfos planInfo = (PlanInfos)ViewBag.CurrentPlan;
                            //if (resource.plan == "11" || resource.plan == "13" || resource.plan == "15")
                            //if (planInfo.isRedShiftSupport)
                            //{
                            //    isODBC = true;
                            //}
                            AppInfo appInfo = TempData.GetValue<AppInfo>("appInfo");
                            if (appInfo.IsNull())
                            {
                                //Get app by app name
                                appInfo = await HerokuApi.GetAppInfo(resource.app_name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                            }

                            if (!appInfo.IsNull() && appInfo.space.HasValue)
                            {
                                string errMsg = string.Empty;
                                AppSetupInfo appSetupInfo = default(AppSetupInfo);
                                AppInfo mcAppInfo = default(AppInfo);

                                //Publish mc-pvt app
                                mcAppInfo = await HerokuApi.PublishMCAppInPrivateSpace(resource.uuid, resource.region, resource.user_organization,
                                                Constants.PRIVATE_ADDON_APP_NAME, appInfo.space.Value.name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ref appSetupInfo, out errMsg).ConfigureAwait(false);

                                if (!string.IsNullOrEmpty(errMsg) && errMsg != "succeeded")
                                {
                                    TempData.PutValue("appInfo", appInfo);
                                    return Json(new { url = webUrl, status = errMsg });
                                }

                                if (string.IsNullOrEmpty(status) && !mcAppInfo.IsNull())
                                {
                                    //Get app latest build by app name
                                    var appBuild = await HerokuApi.GetAppLatestBuild(mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);

                                    //Check app build version is same or not. If not then create new build with latest source available in git repository
                                    //else redirect to mc-pvt url
                                    if (!appBuild.IsNull() && !appBuild.source_blob.IsNull() && !string.IsNullOrEmpty(appBuild.source_blob.version))
                                    {
                                        if ((appBuild.status == "failed" && appBuild.source_blob.version == ConfigVars.Instance.deDupPvtAppVersion) || appBuild.source_blob.version != ConfigVars.Instance.deDupPvtAppVersion)
                                        {
                                            //update the stack version if its lower then the value configured in configvars table in db
                                            if (!mcAppInfo.stack.IsNull() && !string.IsNullOrEmpty(mcAppInfo.stack.name) && mcAppInfo.stack.name != ConfigVars.Instance.dedupPvtAppStackVersion)
                                            {
                                                HerokuApi.UpdateHerokuappStackVersion(mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ConfigVars.Instance.dedupPvtAppStackVersion);
                                            }
                                            //Create new build from git repository
                                            appBuild = await HerokuApi.CreateAppBuild(mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), ConfigVars.Instance.deDupPvtAppVersion).ConfigureAwait(false);
                                        }
                                    }

                                    //redirect if app build status succeeded
                                    if (!appBuild.IsNull() && !string.IsNullOrEmpty(appBuild.status))
                                    {
                                        if (appBuild.status == "failed")
                                        {
                                            status = appBuild.status;
                                        }
                                        else
                                        {
                                            if (appBuild.status == "succeeded")
                                            {
                                                status = appBuild.status;
                                                var configVars = await HerokuApi.GetHerokuAppConfigVars(mcAppInfo.name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                                                string appName = string.Empty, queueName = string.Empty;
                                                if (configVars != null)
                                                {
                                                    configVars.TryGetValue("DEDUP_PROXY_APP", out appName);
                                                    configVars.TryGetValue("JOB_QUEUE_NAME", out queueName);
                                                }

                                                if (string.IsNullOrEmpty(appName) || appName == "mc-addon-proxy" || appName == "mc-connect-addon"
                                                    || string.IsNullOrEmpty(queueName) || queueName == "pvt_critical" || queueName == "critical")
                                                {
                                                    //Update app config-vars
                                                    await HerokuApi.AddUpdatePrivateAppConfigvars(mcAppInfo.name, mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                                                }

                                                //get dyno info
                                                var dynoInfo = await HerokuApi.GetDynoInfo(mcAppInfo.id, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN)).ConfigureAwait(false);
                                                if (!dynoInfo.IsNull())
                                                {
                                                    if ((new string[] { "idle", "starting", "up" }).Contains(dynoInfo.state))
                                                    {
                                                        TempData.Remove("appInfo");
                                                        webUrl = string.Format("{0}/sso/auth/{1}", mcAppInfo.web_url.TrimEnd('/'), resource.uuid);

                                                        if (!string.IsNullOrEmpty(webUrl) && resource.private_app_url != webUrl)
                                                        {
                                                            //Update mc-pvt url
                                                            await _resourcesRepository.UpdateResourcePrivateUrl(resource.uuid, webUrl);
                                                        }

                                                        return Json(new { url = webUrl, status = status });
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                status = appBuild.status;
                                            }
                                        }
                                    }
                                }
                            }
                            TempData.PutValue("appInfo", appInfo);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return Json(new { url = webUrl, status = status });

        }
        [LoginAuthorizeAttribute]
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult ExternalObjects()
        {
            return View();
        }

        /// <summary>
        /// Action: Error
        /// Description: It is called when any exception will occur in any action
        /// </summary>
        /// <returns></returns>
        [AllowAnonymous]
        public IActionResult Error()
        {
            var feature = HttpContext.Features.Get<IExceptionHandlerFeature>();
            var exception = feature?.Error;
            return View(new ErrorViewModel() { Code = HttpStatusCode.InternalServerError, Message = (exception.Message.IndexOf("DeDup") == -1 ? exception.Message : exception.Message.Substring(exception.Message.IndexOf("DeDup"))) });
        }

        /// <summary>
        /// Action: Forbidden
        /// Description: It is called when user is not authorised
        /// </summary>
        /// <returns></returns>
        [AllowAnonymous]
        public IActionResult Forbidden()
        {
            ErrorViewModel errorViewModel = new ErrorViewModel();
            var httpStatusCode = HttpStatusCode.Forbidden;
            if (TempData["httpStatusCode"] != null)
                httpStatusCode = (HttpStatusCode)TempData["httpStatusCode"];
            switch (httpStatusCode)
            {
                case HttpStatusCode.NotFound:
                    errorViewModel.Code = httpStatusCode;
                    errorViewModel.Message = string.Format("You did not add the {0} addon to anyone of your heroku app.", ConfigVars.Instance.herokuAddonId);
                    break;
                case HttpStatusCode.Unauthorized:
                case HttpStatusCode.Forbidden:
                default:
                    errorViewModel.Code = httpStatusCode;
                    if (TempData["errorMessage"] == null)
                        errorViewModel.Message = string.Format("You are not authenticated to access the {0} addon.", ConfigVars.Instance.herokuAddonId);
                    else
                        errorViewModel.Message = (string)TempData["errorMessage"];
                    break;
            }

            //clear all session
            HttpContext.Session.Clear();
            //clear all cookie
            foreach (var cookie in Request.Cookies.Keys)
            {
                Response.Cookies.Delete(cookie);
            }
            return View("~/Views/Shared/Unauthorized.cshtml", errorViewModel);
        }

        [LoginAuthorizeAttribute]
        public IActionResult GetLogs()
        {
            try
            {
                string dirPath = Path.Combine(Utilities.HostingEnvironment.ContentRootPath, "wwwroot", "Logs");
                DirectoryInfo dirInfo = new DirectoryInfo(dirPath);
                if (dirInfo.Exists && dirInfo.GetFiles().Count() > 0)
                {
                    var myFile = (from file in dirInfo.GetFiles()
                                  orderby file.LastWriteTime descending
                                  select file).FirstOrDefault();

                    var fileContent = string.Empty;
                    using (FileStream fileStream = new FileStream(myFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    {
                        using (StreamReader streamReader = new StreamReader(fileStream))
                        {
                            fileContent = streamReader.ReadToEnd();
                        }
                    }

                    return Content(fileContent);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return Content("File not found!");
        }

        [LoginAuthorizeAttribute]
        public async Task<JsonResult> UpdateLicenseAgreement(bool isAccepted)
        {
            try
            {
                Console.WriteLine("UpdateLicenseAgreement starts");
               await _resourcesRepository.UpdateLicenseAgreement(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), isAccepted);
                Console.WriteLine("UpdateLicenseAgreement ended");
                return Json(await Task.FromResult(new { status = HttpStatusCode.OK }));
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                return Json(await Task.FromResult(new { status = HttpStatusCode.InternalServerError }));
            }
        }

        protected override void Dispose(bool disposing)
        {
            _resourcesRepository.Dispose();
            _connectorsRepository.Dispose();
            base.Dispose(disposing);
        }
    }
}
