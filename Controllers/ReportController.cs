using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Dedup.Repositories;
using Microsoft.AspNetCore.Http;
using Dedup.Services;
using Dedup.Common;
using Dedup.ViewModels;
using Dedup.Extensions;
using System.Security.Claims;
using Dedup.HttpFilters;

namespace Dedup.Controllers
{
    [LoginAuthorizeAttribute]
    [TypeFilter(typeof(AddonPlanFilter))]
    public class ReportController : Controller
    {
        private readonly IConnectorsRepository _connectorsRepository;

        public ReportController(IConnectorsRepository connectorsRepository)
        {
            _connectorsRepository = connectorsRepository;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult DataReports()
        {
            return View();
        }

        /// <summary>
        /// Action: ConnectorLogs
        /// Description: It is called to get all connector logs and assign to ViewBag which will be accessed on view
        /// </summary>
        /// <returns></returns>
        public IActionResult ConnectorLogs()
        {
            List<ConnectorLogs> connectorLogs = null;

            try
            {
                //Get all connectors logs from connectors table by ccid
                connectorLogs = _connectorsRepository.Get<List<ConnectorLogs>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, null);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return View(connectorLogs);
        }

        /// <summary>
        /// Action: AppLogs
        /// Description: It is called to get all logs of the main app and assign to ViewBag which will be accessed on view
        /// </summary>
        /// <returns></returns>
        public IActionResult AppLogs(bool tail)
        {
            Console.WriteLine("Report-App Logs Inside");
            string dbUrl = string.Empty;
            ViewBag.appLogInfo = string.Empty;

            try
            {
                var resource = _connectorsRepository.GetResource(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
                if (resource != null)
                {
                    //Heroku api is using authtoken to call the api
                    //Get log plex url for the main by using app name
                    string log_plex_url = HerokuApi.GetHerokuAppLogUrl(false, resource.app_name, HttpContext.GetClaimValue(Constants.HEROKU_ACCESS_TOKEN), tail);
                    if (!string.IsNullOrEmpty(log_plex_url))
                    {
                        //Get app logs by using log plex url
                        ViewBag.appLogInfo = HerokuApi.GetHerokuAppLogs(log_plex_url, resource.app_name);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            Console.WriteLine("App Logs Outside");
            return View();
        }

        protected override void Dispose(bool disposing)
        {
            _connectorsRepository.Dispose();
            base.Dispose(disposing);
        }
    }
}
