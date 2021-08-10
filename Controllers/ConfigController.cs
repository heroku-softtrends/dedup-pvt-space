using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Dedup.Repositories;
using Dedup.ViewModels;
using Microsoft.AspNetCore.Http;
using Dedup.Extensions;
using System.Net;
using Dedup.Services;
using Microsoft.Extensions.Hosting;
using System.Security.Claims;
using Dedup.HttpFilters;
using Dedup.Common;
using System.Threading.Tasks;

namespace Dedup.Controllers
{
    [LoginAuthorizeAttribute]
    public class ConfigController : Controller
    {
        private readonly IDeDupSettingsRepository _dedupSettingsRepository;

        public ConfigController(IDeDupSettingsRepository dedupSettingsRepository)
        {
            _dedupSettingsRepository = dedupSettingsRepository;
        }

        /// <summary>
        /// Action: DBConfig
        /// Description: It is called to get DatabaseConfig from DedupSettings table by ccid.
        /// If not then create a new instance and assign ccid from session and return it.
        /// </summary>
        /// <param name="databaseType"></param>
        /// <returns>DatabaseConfig</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult DBConfig(DatabaseType databaseType = DatabaseType.Heroku_Postgres)
        {
            Console.WriteLine("DBConfig Start");
            DatabaseConfig dbConfig = null;
            try
            {
                if (ViewBag.CurrentPlan.IsInitialized && !ViewBag.CurrentPlan.is_postgresql)
                {
                    TempData["msg"] = "<script>Swal.fire('','The current plan doesn't support this feature, Upgrade your plan to get more features.','error');</script>";
                    return RedirectToAction("index", "home");
                }

                //get DatabaseConfig from DedupSettings table by ccid. If not then create a new instance
                //and assign ccid from session
                dbConfig = _dedupSettingsRepository.Get<DatabaseConfig>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), databaseType);
                if (dbConfig == null)
                {
                    dbConfig = new DatabaseConfig();
                    dbConfig.ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                }
                dbConfig.databaseType = databaseType;
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("DBConfig End");
            return View(dbConfig);
        }

        /// <summary>
        /// Action: DBConfig
        /// Description: It is called to insert/update the specific property of postgres DatabaseConfig in DeDupSettings table
        /// by ccid
        /// </summary>
        /// <param name="dbConfig"></param>
        /// <returns>HttpStatusCode as JSON object</returns>
        [HttpPost]
        [TypeFilter(typeof(AddonPlanFilter))]
        public JsonResult DBConfig(DatabaseConfig dbConfig)
        {
            Console.WriteLine("DBConfig Start");
            try
            {
                ModelState.Remove("syncFollowerDatabaseUrl");
                if (ModelState.IsValid && dbConfig.databaseType != DatabaseType.None && ViewBag.CurrentPlan.IsInitialized && ViewBag.CurrentPlan.is_postgresql)
                {
                    if (string.IsNullOrEmpty(dbConfig.ccid))
                    {
                        dbConfig.ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                    }

                    Console.WriteLine("Add/Edit DBConfig Starts");
                    _dedupSettingsRepository.AddEditDatabaseConfig(dbConfig);
                    Console.WriteLine("Add/Edit DBConfig Ended");
                }
                else
                {
                    Console.WriteLine("DBConfig Invalid Input");
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            Console.WriteLine("DBConfig End");
            return Json(new { Status = Response.StatusCode });
        }

        /// <summary>
        /// Action: DeletePGConfig
        /// Description: It is called to delete the specific postgres DatabaseConfig in DeDupSettings table
        /// by ccid
        /// </summary>
        /// <param name="databaseType"></param>
        /// <returns></returns>
        [HttpGet]
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult DeletePGConfig([FromQuery] DatabaseType databaseType)
        {
            Console.WriteLine("DeletePGConfig Start");
            try
            {
                var resourceId = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                if (ModelState.IsValid && !string.IsNullOrEmpty(resourceId))
                {
                    Console.WriteLine("Delete DBConfig Start");
                    _dedupSettingsRepository.DeleteDatabaseConfig(databaseType, resourceId);
                    Console.WriteLine("Delete DBConfig End");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("DeletePGConfig End");
            return RedirectToAction("pgconfig", "config", new { list = true });
        }

        /// <summary>
        /// Action: GetDBConfig
        /// Description: It is called to get DatabaseConfig from DeDupSettings table by ccid.
        /// If not then create a new instance and assign ccid from session and return it.
        /// </summary>
        /// <returns>DatabaseConfig</returns>
        [HttpPost]
        [TypeFilter(typeof(AddonPlanFilter))]
        public JsonResult GetDBConfig(DatabaseType databaseType)
        {
            Console.WriteLine("GetDBConfig Start");
            DatabaseConfig dbConfig = null;
            try
            {
                //get DatabaseConfig from DeDupSettings table by ccid. If not then create a new instance
                //and assign ccid from session
                if (databaseType == DatabaseType.Heroku_Postgres
                    || databaseType == DatabaseType.Azure_Postgres
                    || databaseType == DatabaseType.AWS_Postgres
                    || databaseType == DatabaseType.Azure_SQL)
                {
                    dbConfig = _dedupSettingsRepository.Get<DatabaseConfig>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), databaseType);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("GetDBConfig End");
            return Json(dbConfig);
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult Utilities()
        {
            return View();
        }

        /// <summary>
        /// Action: GetConfigVar
        /// Description: It is called to get the name the config variable value of main app which has using this addon.
        /// It is calling heroku api to access config var value.
        /// </summary>
        /// <returns>JSON object</returns>
        [HttpPost]
        [TypeFilter(typeof(AddonPlanFilter))]
        public async Task<JsonResult> GetConfigVar(string name)
        {
            string conValue = string.Empty;
            try
            {
                Console.WriteLine("Get Config Variable Start");
                string resourceId = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                HerokuApi.PostlogMessage("Get Config Variable Start", resourceId);
                if (!string.IsNullOrEmpty(name))
                {
                    //Get heroku auth token from session
                    var herokuAuthToken = HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN);

                    //Get heroku app name from resources table by resource id
                    var appName = _dedupSettingsRepository.GetHerokuAppName(resourceId);
                    if (!string.IsNullOrEmpty(appName) && !string.IsNullOrEmpty(herokuAuthToken))
                    {
                        //Get config var value
                        conValue =await HerokuApi.GetHerokuAppConfigVarByName(appName, name, herokuAuthToken);
                    }
                }

                Console.WriteLine("Get Config Variable Start");
                HerokuApi.PostlogMessage("Get Config Variable End", resourceId);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            return Json(await Task.FromResult(new { Value = conValue }));
            //return Json(new { Value = conValue });
        }

        /// <summary>
        /// Action: AppConfig
        /// Description: It is called to get the info of main app which has using this addon.
        /// It is calling heroku api to access addons and config_vars of main app.
        /// </summary>
        /// <returns>HerokuAppConfig</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public async Task<IActionResult> AppConfig()
        {
            HerokuAppConfig appConfig = new HerokuAppConfig();
            try
            {
                Console.WriteLine("Get App Config Start");
                string resourceId = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                HerokuApi.PostlogMessage("Get App Config Start", resourceId);
                var herokuAuthToken = HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN);

                //Get resource info from resourses table by resource id
                appConfig.resource = _dedupSettingsRepository.GetResource(resourceId).ToResource();
                if (!string.IsNullOrEmpty(herokuAuthToken))
                {
                    //Get addons details of the main app
                    appConfig.addons = HerokuApi.GetHerokuAppAddons(appConfig.resource.app_name, herokuAuthToken);

                    //Commented on 24th May, 2020 due to partner auth token not permitted to access
                    ////Get config_vars of the main app
                    //appConfig.config_vars = HerokuApi.GetHerokuAppConfigVars(appConfig.resource.app_name, herokuAuthToken);
                }

                Console.WriteLine("Get App Config End");
                HerokuApi.PostlogMessage("Get App Config End", resourceId);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return View(await Task.FromResult(appConfig));
        }


        /// <summary>
        /// Action: LoadMCConfigsByType
        /// Description: It is called to refresh addon configs by type
        /// </summary>
        /// <returns></returns>
        [HttpGet("config/refreshconfigsbytype"), Route("config/refreshconfigsbytype/{type?}")]
        public async Task<JsonResult> RefreshConfigsByType(LoadConfigType type = LoadConfigType.ALL)
        {
            try
            {
                Console.WriteLine("RefreshConfigsByType starts");
                await ConfigVars.Instance.LoadDeDupConfigsByTypeAsync(type);
                Console.WriteLine("RefreshConfigsByType ended");
                return Json(new { message = $"{type.ToString()} has been refreshed" });
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            return Json(new { message = $"{type.ToString()} has not refreshed" });
        }

        protected override void Dispose(bool disposing)
        {
            _dedupSettingsRepository.Dispose();
            base.Dispose(disposing);
        }
    }
}
