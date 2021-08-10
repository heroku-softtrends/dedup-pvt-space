using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Dedup.Repositories;
using Dedup.ViewModels;
using Dedup.Models;
using Dedup.Extensions;
using Microsoft.AspNetCore.Http;
using Dedup.Services;
using System.Net;
using Dedup.Common;
using System.Security.Claims;
using Microsoft.Extensions.Hosting;
using Dedup.HttpFilters;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Linq;


// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Dedup.Controllers
{
    [LoginAuthorizeAttribute]
    public class ConnectorController : Controller
    {
        private readonly IConnectorsRepository _connectorsRepository;
        private readonly IDeDupSettingsRepository _dedupSettingsRepository;
        private readonly ISyncRepository _syncRepository;
        public ConnectorController(IConnectorsRepository connectorsRepository, IDeDupSettingsRepository dedupSettingsRepository, ISyncRepository syncRepository)
        {
            _connectorsRepository = connectorsRepository;
            _dedupSettingsRepository = dedupSettingsRepository;
            _syncRepository = syncRepository;
        }

        /// <summary>
        /// Action: Index
        /// Description: It is called to get ConnectorConfig from Connectors table by ccid.
        /// If not then create a new instance and assign ccid from session and return it.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <returns>ConnectorConfig</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public async Task<IActionResult> Index(int id = default(int), string ccid = default(string))
        {
            Console.WriteLine("Connecter List Start");
            ConnectorConfig connectorConfig = null;
            try
            {
                HttpContext.RemoveCookie("tableColumns");
                if (string.IsNullOrEmpty(ccid))
                {
                    ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                }

                //connectors count
                int consumedProcessCount = 0;

                //Get all connectors by ccid
                connectorConfig = _connectorsRepository.GetConnectorById(ccid, id, ref consumedProcessCount);

                //Assign total postgres and sql count for current plan
                if (ViewBag.CurrentPlan.IsInitialized)
                {
                    //Get total postgres and sql count
                    ViewBag.CurrentPlan.addedDedupProcessCount = consumedProcessCount;
                }

                //If connectorId is null then create new instance of ConnectorConfig and assign ccid
                //and default postgres database url from DeDupSettings table
                if (id == default(int) && connectorConfig == null)
                {
                    connectorConfig = new ConnectorConfig() { ccid = ccid };
                    var dedupSettings = _dedupSettingsRepository.Find(ccid);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Connecter List End");

            return View(await Task.FromResult(connectorConfig));
        }

        /// <summary>
        /// Action: GetRows
        /// Description: It is called to get only postgres connector(s) from connectors table by ccid and connectorId.
        /// If connectorId is default value then return all postgres connectors else only return specific connector
        /// </summary>
        /// <param name="id"></param>
        /// <returns>List<ConnectorConfig></returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetRows(int id = default(int), int pagesize = 0)
        {
            Console.WriteLine("Postgres Data Explorer Start");
            List<ConnectorConfig> ConnectorConfigs = new List<ConnectorConfig>();
            try
            {
                //Save connector type in session. It is used later in GetSyncRecordsByPageNo action while page navigation
                HttpContext.Session.SetString("SourceType", "postgres");
                if (id > 0)
                {
                    //Get connector from connectors table by ccid and connectorId
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), id, IsSetConfig: true);
                }
                else
                {
                    //Get all postgres connectors from connectors table by ccid
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, DatabaseType.Heroku_Postgres, true);
                }
                ViewBag.pagesize = pagesize;
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Postgres Data Explorer End");
            return View(ConnectorConfigs);
        }

        /// <summary>
        /// Action: GetSqlRows
        /// Description: It is called to get only sql connector(s) from connectors table by ccid and connectorId.
        /// If connectorId is default value then return all sql connectors else only return specific connector
        /// </summary>
        /// <param name="id"></param>
        /// <returns>List<ConnectorConfig></returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetSqlRows(int id = default(int))
        {
            Console.WriteLine("Sql Data Explorer Start");
            List<ConnectorConfig> ConnectorConfigs = new List<ConnectorConfig>();
            try
            {
                //Save connector type in session. It is used later in GetSyncRecordsByPageNo action while page navigation
                HttpContext.Session.SetString("SourceType", "mssql");
                if (id > 0)
                {
                    //Get connector from connectors table by ccid and connectorId
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), id, IsSetConfig: true);
                }
                else
                {
                    //Get all postgres connectors from connectors table by ccid
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, DatabaseType.Azure_SQL, true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Sql Data Explorer End");
            return View(ConnectorConfigs);
        }

        /// <summary>
        /// Action: GetSyncRecordsByPageNo
        /// Description: It is called to get sync records from postgres data source by ccid, connectorId and page.
        /// It is calling postgres partial view by syncDestination
        /// </summary>
        /// <param name="id"></param>
        /// <param name="page"></param>
        /// <returns>ViewComponent</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetSyncRecordsByPageNo(int id, int page, string tab, string ctid, int pagesize)
        {
            Console.WriteLine("GetSyncRecordsByPageNo starts");
            var viewComponent = "PGRows";
            Console.WriteLine("GetSyncRecordsByPageNo loding the viewcomponent " + viewComponent);
            return ViewComponent(viewComponent, new { id = id, page = page, tab = tab, ctid = ctid, pagesize = pagesize });
        }

        /// <summary>
        /// Action: AddEdit
        /// Description: It is called to insert/update connector by ccid and connectorId
        /// </summary>
        /// <param name="connectorConfig"></param>
        /// <returns>HttpStatusCode as JSON Object</returns>
        [HttpPost]
        [TypeFilter(typeof(AddonPlanFilter))]
        public async Task<JsonResult> AddEdit(ConnectorConfig connectorConfig)
        {
            Console.WriteLine("Connector AddEdit Start");
            string errorMessage = string.Empty;

            try
            {
                if (connectorConfig != null)
                {
                    //Exclude optional model properties
                    ModelState.Remove("jobId");
                    if (connectorConfig.scheduleType != ScheduleType.CUSTOM)
                    {
                        ModelState.Remove("customScheduleInMinutes");
                    }
                    if (string.IsNullOrEmpty(connectorConfig.dbSchema))
                    {
                        ModelState.Remove("dbSchema");
                        if (connectorConfig.dataSource == DataSource.Azure_SQL)
                        {
                            connectorConfig.dbSchema = Constants.MSSQL_DEFAULT_SCHEMA;
                        }
                        else
                        {
                            connectorConfig.dbSchema = Constants.POSTGRES_DEFAULT_SCHEMA;
                        }
                    }
                    if (string.IsNullOrEmpty(connectorConfig.destDBSchema))
                    {
                        ModelState.Remove("destDBSchema");
                        if (connectorConfig.syncDestination == ConnectorType.Azure_SQL)
                        {
                            connectorConfig.destDBSchema = Constants.MSSQL_DEFAULT_SCHEMA;
                        }
                        else
                        {
                            connectorConfig.destDBSchema = Constants.POSTGRES_DEFAULT_SCHEMA;
                        }
                    }
                    if (connectorConfig.dedupSourceType == SourceType.Remove_Duplicates_from_a_Single_Table || connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination)
                    {
                        ModelState.Remove("dbConfig_compare.syncDefaultDatabaseUrl");
                        ModelState.Remove("dbConfig_compare.db_schema");
                        ModelState.Remove("dbConfig_compare.object_name");
                        ModelState.Remove("compareObjectFieldsMapping");
                        ModelState.Remove("syncNewRecordFilter");
                        ModelState.Remove("syncUpdateRecordFilter");
                    }
                    else
                    {
                        if (connectorConfig.dbConfig_compare.table_type == SelectedTableType.Create_New_Table)
                        {
                            ModelState.Remove("dbConfig_compare.object_name");
                        }

                    }
                    if (DedupType.Full_Dedup == connectorConfig.dedup_type)
                    {
                        if (connectorConfig.dedupSourceType == SourceType.Remove_Duplicates_from_a_Single_Table || connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination)
                        {
                            ModelState.Remove("dbConfig_compare.syncDefaultDatabaseUrl");
                            ModelState.Remove("dbConfig_compare.db_schema");
                            ModelState.Remove("dbConfig_compare.object_name");
                            ModelState.Remove("compareObjectFieldsMapping");
                            ModelState.Remove("syncNewRecordFilter");
                            ModelState.Remove("syncUpdateRecordFilter");

                        }
                        ModelState.Remove("destDBConfig.syncDefaultDatabaseUrl");
                        ModelState.Remove("destObjectName");
                        ModelState.Remove("destDBSchema");
                        ModelState.Remove("syncDestination");
                    }
                }

                if (ModelState.IsValid)
                {
                    if (ViewBag.CurrentPlan.IsInitialized)
                    {
                        ViewBag.CurrentPlan.addedDedupProcessCount = await _connectorsRepository.GetConnectorsCount(connectorConfig.ccid).ConfigureAwait(false);
                        if (ViewBag.CurrentPlan.schedule_dedup_process == false)
                        {
                            connectorConfig.scheduleType = ScheduleType.MANUAL_SYNC;
                        }
                        if (ViewBag.CurrentPlan.addedDedupProcessCount <= ViewBag.CurrentPlan.max_dedup_process_allowed)
                        {
                            if (connectorConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
                            {
                                connectorConfig.dedup_type = DedupType.Simulate_and_Verify;
                            }
                            else
                            {
                                connectorConfig.dedup_type = DedupType.Full_Dedup;
                            }
                            connectorConfig.dedup_method = SimilarityType.Logical_AND;
                            connectorConfig.simulation_count = -1;
                            //Insert/Update connector by ccid and connectorId
                            _connectorsRepository.AddEditExtensionsConfig(connectorConfig);
                        }
                        else
                        {
                            Response.StatusCode = (int)HttpStatusCode.NotAcceptable;
                            errorMessage = "Add-on plan limit reached, Upgrade to create more.";
                        }
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
                Console.WriteLine("ERROR: {0}", ex.Message);
                if (!string.IsNullOrEmpty(ex.Message) && ex.Message.Contains("table already exists"))
                    Response.StatusCode = (int)HttpStatusCode.Ambiguous;
                else if (!string.IsNullOrEmpty(ex.Message))
                    Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                errorMessage = ex.Message;
            }
            Console.WriteLine("Connector AddEdit End");

            return Json(await Task.FromResult(new { Status = Response.StatusCode, Message = errorMessage }));
        }

        /// <summary>
        /// Action: SyncTableAddEdit
        /// Description: It is called to insert/update connector by ccid and connectorId
        /// </summary>
        /// <returns>HttpStatusCode as JSON Object</returns>
        [HttpPost]
        public JsonResult SyncTableAddEdit(string ccid, int connectorId, string fieldName, string fieldValue)
        {
            Console.WriteLine("SyncTableAddEdit Start");
            try
            {
                HerokuApi.PostlogMessage("sync table create/edit start", ccid);
                if (ModelState.IsValid)
                {
                    //Insert/Update connector by ccid and connectorId
                    if (_connectorsRepository.Get<Connectors>(ccid, connectorId) == null)
                    {
                        return Json(new { Status = (int)HttpStatusCode.InternalServerError, Message = "First save connector then try it." });
                    }

                    //As discussed with Samant, Edit destination table name restricted on 30/03/2018
                    Response.StatusCode = (int)HttpStatusCode.Forbidden;

                    //_connectorsRepository.AddEditExtensionsConfig(ccid, connectorId, fieldName, fieldValue);
                }
                else
                {
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }
                HerokuApi.PostlogMessage("sync table create/edit end", ccid);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            Console.WriteLine("SyncTableAddEdit End");
            return Json(new { Status = Response.StatusCode });
        }

        /// <summary>
        /// Action: Delete
        /// Description: It is called to delete connector by ccid and connectorId and redirect to connectors list page
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <returns></returns>
        [HttpGet]
        public IActionResult Delete(int id, string ccid)
        {
            Console.WriteLine("Delete Start");
            try
            {
                if (ModelState.IsValid)
                {
                    //Delete connector by ccid and connectorId
                    _connectorsRepository.DeleteExtensionConfigById(ccid, id);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Delete End");
            return RedirectToAction("index", "home");
        }

        /// <summary>
        /// Action: Schedule
        /// Description: It is called to get all schedules for connectors
        /// </summary>
        /// <returns>List<ConnectorConfig></returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult Schedule()
        {
            Console.WriteLine("Get Schedule Start");
            List<ConnectorConfig> ConnectorConfigs = null;

            try
            {
                //Get all connectors from connectors table by ccid
                ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, null);
                Console.WriteLine("Get Schedule End");
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Console.WriteLine("Get Schedule Error");
            }

            return View(ConnectorConfigs);
        }

        /// <summary>
        /// Action: SwitchToManual
        /// Description: It is called to change sync schedule type to manual type for connector
        /// </summary>
        /// <returns>HttpStatusCode as JSON Object</returns>
        [HttpPost]
        public IActionResult SwitchToManual(int id, string ccid)
        {
            Console.WriteLine("SwitchToManual Start");
            try
            {
                if (ModelState.IsValid)
                {
                    //Set sync schedule type to manual type
                    _connectorsRepository.SwitchToManual(id, ccid);
                    Console.WriteLine("SwitchToManual Success");
                }
                else
                {
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    Console.WriteLine("SwitchToManual Invalid Input");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            Console.WriteLine("SwitchToManual End");

            return Json(new { Status = Response.StatusCode });
        }

        /// <summary>
        /// Action: GetDataSourceObjectsListByChar
        /// Description: It is called to get all data sources from postgres/sql
        /// It is using sql/postgres configuration to access sql database or postgres database.
        /// </summary>
        /// <returns>List<ET_DataExtension>/List<DatabaseTables></returns>
        [HttpGet]
        public async Task<JsonResult> GetDataSourceObjectsListByChar(string datasource, string dbURL, string schema, string tablename)
        {
            Console.WriteLine("Get Data Source Objects Start");
            try
            {
                DatabaseConfig databaseConfig = new DatabaseConfig();
                DataSource e = (DataSource)Enum.Parse(typeof(DataSource), datasource);
                databaseConfig.dataSource = e;
                databaseConfig.syncDefaultDatabaseUrl = dbURL;
                databaseConfig.db_schema = schema;
                databaseConfig.object_name = tablename;

                if (databaseConfig.dataSource == DataSource.Heroku_Postgres
                    || databaseConfig.dataSource == DataSource.Azure_Postgres
                    || databaseConfig.dataSource == DataSource.AWS_Postgres)
                {
                    //declare DatabaseConfig


                    //Check whether DatabaseConfig is saved or not. If not then throw error
                    if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                    {
                        Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        return Json(new { Status = Response.StatusCode, Message = "Postgres not yet configured." });
                    }

                    var databaseTables = SyncRepository.GetPGDatabaseTablesListByChar(databaseConfig);
                    Console.WriteLine("Get Data Source Objects End");
                    return Json(await Task.FromResult(databaseTables));
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);

                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode, Message = ex.Message });
            }

            Console.WriteLine("Get Data Source Objects End");
            return Json(null);
        }

        /// <summary>
        /// Action: GetDataSourceObjects
        /// Description: It is called to get all data sources from postgres/sql
        /// It is using sql/postgres configuration to access sql database or postgres database.
        /// </summary>
        /// <returns>List<ET_DataExtension>/List<DatabaseTables></returns>
        [HttpPost]
        public async Task<JsonResult> GetDataSourceObjects(DataSource dataSource, DatabaseConfig dbConfig = default(DatabaseConfig))
        {
            Console.WriteLine("Get Data Source Objects Start");
            try
            {
                if (dataSource == DataSource.Heroku_Postgres
                    || dataSource == DataSource.Azure_Postgres
                    || dataSource == DataSource.AWS_Postgres)
                {
                    //declare DatabaseConfig
                    DatabaseConfig databaseConfig = dbConfig;

                    //Check whether DatabaseConfig is saved or not. If not then throw error
                    if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                    {
                        Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        return Json(new { Status = Response.StatusCode, Message = "Postgres not yet configured." });
                    }

                    var databaseTables = SyncRepository.GetPGDatabaseTables(databaseConfig);
                    Console.WriteLine("Get Data Source Objects End");
                    return Json(await Task.FromResult(databaseTables));
                }
                else if (dataSource == DataSource.Azure_SQL)
                {
                    //declare DatabaseConfig
                    DatabaseConfig databaseConfig = dbConfig;

                    //set database type
                    databaseConfig.databaseType = DatabaseType.Azure_SQL;

                    //Check whether DatabaseConfig is saved or not. If not then throw error
                    if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                    {
                        Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        return Json(new { Status = Response.StatusCode, Message = "MsSql not yet configured." });
                    }

                    //var databaseTables = SyncRepository.GetSqlDatabaseTables(databaseConfig);
                    //Console.WriteLine("Get Data Source Objects End");
                    //return Json(databaseTables);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);

                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode, Message = ex.Message });
            }

            Console.WriteLine("Get Data Source Objects End");
            return Json(null);
        }

        /// <summary>
        /// Action: GetDataSourceObjectColumns
        /// Description: It is called to get all data source columns by customerKey from postgres/sql
        /// It is using postgres/sql configuration to access postgres or sql database
        /// </summary>
        /// <param name="sourceValue"></param>
        /// <returns>List<ET_DataExtensionColumn>/List<DatabaseTableColumns></returns>
        [HttpPost]
        public async Task<JsonResult> GetDataSourceObjectColumns(DataSource dataSource, string sourceValue, string dbSchema = "", DatabaseConfig dbConfig = default(DatabaseConfig))
        {
            Console.WriteLine("Get Data Source Object Columns Start");

            try
            {
                if (!string.IsNullOrEmpty(sourceValue))
                {
                    if ((dataSource == DataSource.Heroku_Postgres
                        || dataSource == DataSource.Azure_Postgres
                        || dataSource == DataSource.AWS_Postgres) && !string.IsNullOrEmpty(dbSchema))
                    {
                        //Get DatabaseConfig by ccid
                        DatabaseConfig databaseConfig = dbConfig;

                        //Check whether DatabaseConfig is saved or not. If not then throw error
                        if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                        {
                            Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                            return Json(new { Status = Response.StatusCode, Message = "Postgres not yet configured." });
                        }

                        var databaseTableColumns = SyncRepository.GetPGDatabaseTableColumns(databaseConfig, sourceValue, dbSchema);

                        Console.WriteLine("Get Data Source Object Columns Ended");
                        return Json(await Task.FromResult(databaseTableColumns));
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);

                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode, Message = ex.Message });
            }

            Console.WriteLine("Get Data Source Object Columns Ended");
            return Json(null);
        }

        /// <summary>
        /// Action: GetDataSourceObjects
        /// Description: It is called to get all data sources from postgres/sql
        /// It is using sql/postgres configuration to access sql or postgres database.
        /// </summary>
        /// <returns>List<ET_DataExtension>/List<DatabaseTables></returns>
        [HttpPost]
        public async Task<JsonResult> GetDestNamespaces(ConnectorType syncDestination, DatabaseConfig dbConfig = default(DatabaseConfig))
        {
            try
            {
                Console.WriteLine("Get destination namespaces starts");
                //if (syncDestination == ConnectorType.Heroku_Postgres
                //   || syncDestination == ConnectorType.Azure_Postgres
                //   || syncDestination == ConnectorType.AWS_Postgres)
                //{
                //declare DatabaseConfig
                DatabaseConfig databaseConfig = dbConfig;

                //Check whether DatabaseConfig is saved or not. If not then throw error
                if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                {
                    Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    return Json(new { Status = Response.StatusCode, Message = "Postgres not yet configured." });
                }

                var databaseTables = SyncRepository.GetPGDatabaseTables(databaseConfig);

                Console.WriteLine("Get destination namespaces end");
                return Json(await Task.FromResult(databaseTables));
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);

                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode, Message = ex.Message });
            }

           // Console.WriteLine("Get destination namespaces end");
         //   return Json(null);
        }


        /// <summary>
        /// Action: GetDataSourceObjects
        /// Description: It is called to get all data sources from postgres/sql
        /// It is using sql/postgres configuration to access sql or postgres database.
        /// </summary>
        /// <returns>List<ET_DataExtension>/List<DatabaseTables></returns>
        [HttpPost]
        public async Task<JsonResult> GetALLDestNamespaces(ConnectorType syncDestination, DatabaseConfig dbConfig = default(DatabaseConfig))
        {
            try
            {
                Console.WriteLine("Get destination namespaces starts");
                if (syncDestination == ConnectorType.Heroku_Postgres
                   || syncDestination == ConnectorType.Azure_Postgres
                   || syncDestination == ConnectorType.AWS_Postgres)
                {
                    //declare DatabaseConfig
                    DatabaseConfig databaseConfig = dbConfig;

                    //Check whether DatabaseConfig is saved or not. If not then throw error
                    if (databaseConfig == null || (databaseConfig != null && string.IsNullOrEmpty(databaseConfig.syncDefaultDatabaseUrl)))
                    {
                        Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                        return Json(new { Status = Response.StatusCode, Message = "Postgres not yet configured." });
                    }

                    var databaseTables = SyncRepository.GetALLPGDatabaseTables(databaseConfig);

                    Console.WriteLine("Get destination namespaces end");
                    return Json(await Task.FromResult(databaseTables));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);

                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode, Message = ex.Message });
            }

            Console.WriteLine("Get destination namespaces end");
            return Json(null);
        }

        /// <summary>
        /// Action: DoSync
        /// Description: It is called to start backgound sync process manually for connector
        /// and return sync status as json object
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <returns>HttpStatusCode as JSON Object</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        [HttpPost]
        public async Task<IActionResult> DoSync(int id, string ccid)
        {
            int syncStatus = 0;

            try
            {
                Console.WriteLine("DoSync ends");
                HerokuApi.PostlogMessage("DoSync ends", ccid);
                if (ModelState.IsValid)
                {
                    //Check whether the current addon plan supports sync process or not. If not then throw error
                    if (!ViewBag.CurrentPlan.IsInitialized || (ViewBag.CurrentPlan.IsInitialized && (ViewBag.CurrentPlan.is_postgresql == false)))
                    {
                        return Json(new { Status = (int)HttpStatusCode.Unauthorized, Message = "This plan does not support sending synchronized Kafka data to Postgres." });
                    }

                    //Get connector by ccid and connectorId
                    var connector = _connectorsRepository.Get<Connectors>(ccid, id);

                    //Don't allow if already sync process is in progress. If sync_status is 1 then it is in progress
                    if (connector != null && connector.sync_status != 1)
                    {
                        //Get ConnectorConfig
                        //Delete job based on jobid
                        //if (connector.schedule_type == ScheduleType.MANUAL_SYNC ||
                        //    connector.schedule_type == ScheduleType.STREAMING_SYNC)
                        if (connector.schedule_type == ScheduleType.MANUAL_SYNC)
                        {
                            JobScheduler.Instance.DeleteJob(ccid, connector.connector_id, connector.job_id, ScheduleType.MANUAL_SYNC);
                        }

                        //Start sync process for connector
                        syncStatus = await JobScheduler.Instance.ScheduleJob(connector.ccid, connector.connector_id, JsonConvert.DeserializeObject<DatabaseConfig>(connector.dest_config_json).databaseType, ScheduleType.MANUAL_SYNC);
                    }
                }

                Console.WriteLine("DoSync ends");
                HerokuApi.PostlogMessage("DoSync ends", ccid);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return Json(await Task.FromResult(new { Status = syncStatus }));
        }

        /// <summary>
        /// Action: ClearData
        /// Description: It is called to clear sync data from/stop data sync process to database table by connectorId, ccid.
        /// When clearing data from sync table, Connector sync staus info will reset in Connectors table.
        /// When stopping data sync process to table, Connector sync staus info will set to 2 in Connectors table.
        /// The process(Clear/Stop) will determine by isStop flag
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <param name="isStop"></param>
        /// <returns>HttpStatusCode as JSON object</returns>
        [TypeFilter(typeof(AddonPlanFilter))]
        [HttpPost]
        public async Task<IActionResult> ClearData(int id, string ccid, bool isStop)
        {
            int syncStatus = 0;
            Console.WriteLine("Clear the extension data from the connector");
            HerokuApi.PostlogMessage("Clear the extension data from the connector", ccid);
            try
            {
                if (ModelState.IsValid)
                {
                    //Get connector from connectors table by ccid and connectorId
                    var connectorConfig = _connectorsRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);
                    if (connectorConfig != null)
                    {
                        //Check to be cleared/stopped
                        if (isStop)
                        {
                            //Delete job if it is runing
                            await JobScheduler.Instance.StopSyncJob(connectorConfig.ccid, (int)connectorConfig.connectorId, connectorConfig.jobId, connectorConfig.scheduleType).ConfigureAwait(false);

                            //Set sync staus
                            syncStatus = 10;

                            //Update connector sync_status to 4 by id and ccid
                            SyncRepository.UpdateSyncInfo((int)connectorConfig.connectorId, connectorConfig.ccid, syncStatus, -1);
                        }
                        else
                        {
                            //Set sync staus
                            syncStatus = 0;

                            //clear table rows
                            SyncRepository.RemoveDataRowsFromConnector(connectorConfig);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            HerokuApi.PostlogMessage("Clear the extension data from the connector end", ccid);
            return Json(await Task.FromResult(new { Status = syncStatus }));
        }

        /// <summary>
        /// Action: GetSyncStatus
        /// Description: It is used to get all connectors sync status info from connectors table by ccid
        /// These are used to update connector sync status info on the view in every 15 secs interval
        /// </summary>
        /// <returns>List<ConnectorConfig></returns>
        public async Task<IActionResult> GetSyncStatus()
        {
            List<ConnectorConfig> connectorConfigs = null;

            try
            {
                var addonPlan = HttpContext.GetClaimValue(ClaimTypes.Version);
                var uuid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                Console.WriteLine("GetSyncStatus starts");
                // HerokuApi.PostlogMessage("GetSyncStatus starts", uuid);

                //Get all connectors from connectors table by ccid
                connectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(uuid, null, null);

                Console.WriteLine("GetSyncStatus ends");
                // HerokuApi.PostlogMessage("GetSyncStatus ends", uuid);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return await Task.FromResult(Json(new { Items = connectorConfigs }));
        }

        /// <summary>
        /// Action: SyncTableIsExist
        /// Description: It is called to check sync table already exist or not in sync database.
        /// If table exists then it will reurn 1 else return 0 with http status code
        /// </summary>
        /// <param name="connectorConfig"></param>
        /// <returns>JSON object</returns>
        [HttpPost]
        [TypeFilter(typeof(AddonPlanFilter))]
        public JsonResult SyncTableIsExist(ConnectorConfig connectorConfig)
        {
            try
            {
                var uuid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                HerokuApi.PostlogMessage("SyncTableIsExist starts", uuid);
                //Exclude optional model properties
                ModelState.Remove("jobId");
                ModelState.Remove("customScheduleInMinutes");
                if (connectorConfig.syncDestination == ConnectorType.Heroku_Postgres
                    || connectorConfig.syncDestination == ConnectorType.Azure_Postgres
                    || connectorConfig.syncDestination == ConnectorType.AWS_Postgres
                    || connectorConfig.syncDestination == ConnectorType.Azure_SQL)
                {
                    if (connectorConfig.dedupSourceType == SourceType.Remove_Duplicates_from_a_Single_Table || connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination)
                    {
                        ModelState.Remove("dbConfig_compare.syncDefaultDatabaseUrl");
                        ModelState.Remove("dbConfig_compare.db_schema");
                        ModelState.Remove("dbConfig_compare.object_name");
                        ModelState.Remove("compareObjectFieldsMapping");
                        ModelState.Remove("syncNewRecordFilter");
                        ModelState.Remove("syncUpdateRecordFilter");
                    }

                }
                if (DedupType.Simulate_and_Verify == connectorConfig.dedup_type)
                {
                    if (connectorConfig.dedupSourceType == SourceType.Remove_Duplicates_from_a_Single_Table || connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination)
                    {
                        ModelState.Remove("dbConfig_compare.syncDefaultDatabaseUrl");
                        ModelState.Remove("dbConfig_compare.db_schema");
                        ModelState.Remove("dbConfig_compare.object_name");
                        ModelState.Remove("compareObjectFieldsMapping");
                        ModelState.Remove("syncNewRecordFilter");
                        ModelState.Remove("syncUpdateRecordFilter");
                    }

                    ModelState.Remove("destDBConfig.syncDefaultDatabaseUrl");
                }
                if (DedupType.Full_Dedup == connectorConfig.dedup_type)
                {
                    if (connectorConfig.dedupSourceType == SourceType.Remove_Duplicates_from_a_Single_Table || connectorConfig.dedupSourceType == SourceType.Copy_Source_data_to_Destination_and_Remove_Duplicates_from_Destination)
                    {
                        ModelState.Remove("dbConfig_compare.syncDefaultDatabaseUrl");
                        ModelState.Remove("dbConfig_compare.db_schema");
                        ModelState.Remove("dbConfig_compare.object_name");
                        ModelState.Remove("compareObjectFieldsMapping");
                        ModelState.Remove("syncNewRecordFilter");
                        ModelState.Remove("syncUpdateRecordFilter");
                    }

                    ModelState.Remove("destDBConfig.syncDefaultDatabaseUrl");
                    ModelState.Remove("destObjectName");
                    ModelState.Remove("destDBSchema");
                    ModelState.Remove("syncDestination");
                }
                if (ModelState.IsValid)
                {
                    Console.WriteLine("Check the Sync Table is exist or not");
                    //Check table exists or not
                    int result = SyncRepository.SyncTableIsExist(connectorConfig);
                    if (result > 0)
                    {
                        Console.WriteLine(string.Format("Sync Table {0} is already exist", connectorConfig.destObjectName));
                        HerokuApi.PostlogMessage(string.Format("Sync Table {0} is already exist", connectorConfig.destObjectName), uuid);
                    }

                    HerokuApi.PostlogMessage("SyncTableIsExist ended", uuid);
                    Response.StatusCode = (int)HttpStatusCode.OK;
                    return Json(new { Status = Response.StatusCode, Value = result });
                }
                else
                {
                    HerokuApi.PostlogMessage("SyncTableIsExist ended", uuid);
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                    return Json(new { Status = Response.StatusCode });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                return Json(new { Status = Response.StatusCode });
            }
        }

        /// <summary>
        /// Action: GetSyncRowsCount
        /// Description: It is called to get sync table rows count in sync database.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ccid"></param>
        /// <returns>JSON object</returns>
        [HttpPost]
        public JsonResult GetSyncRowsCount(int id, string ccid)
        {
            var rowCount = 0;
            try
            {
                Console.WriteLine("GetSyncRowsCount starts");
                HerokuApi.PostlogMessage("GetSyncRowsCount starts", ccid);

                if (ModelState.IsValid)
                {
                    //Get ConnectorConfig by ccid and id
                    var connectorConfig = _connectorsRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);
                    if (connectorConfig != null)
                    {
                        //Get sync rows count for connector

                        rowCount = SyncRepository.GetPGRecordCountByName(connectorConfig);

                    }
                }
                else
                {
                    Response.StatusCode = (int)HttpStatusCode.BadRequest;
                }

                Console.WriteLine("GetSyncRowsCount ends");
                HerokuApi.PostlogMessage("GetSyncRowsCount ends", ccid);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }

            return Json(new { Status = Response.StatusCode, Count = rowCount });
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetDuplicateRows([FromRoute] int id, [FromQuery] string ctid = null)
        {
            // return ViewComponent("PGChildRows", new { id = id, page = 1, isRowsRead = true, ctid=ctid });
            //try
            //{
            //    var ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
            //    if (!string.IsNullOrEmpty(ccid) && id > 0)
            //    {
            //        var result = _syncRepository.GetChildRecordsByParentForReviewAndDelete(ccid, id, ctid);

            //        ViewBag.ChildRows = result;

            //    }
            //    return View();
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine("ERROR: {0}", ex.Message);
            //    Response.StatusCode = (int)HttpStatusCode.InternalServerError;
            //}
            //return View();
            Console.WriteLine("Postgres Data Explorer Start");
            List<ConnectorConfig> ConnectorConfigs = new List<ConnectorConfig>();
            try
            {
                //Save connector type in session. It is used later in GetSyncRecordsByPageNo action while page navigation
                HttpContext.Session.SetString("SourceType", "postgres");
                if (id > 0)
                {
                    //Get connector from connectors table by ccid and connectorId
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), id, IsSetConfig: true);
                }
                else
                {
                    //Get all postgres connectors from connectors table by ccid
                    ConnectorConfigs = _connectorsRepository.Get<List<ConnectorConfig>>(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), null, DatabaseType.Heroku_Postgres, true);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Postgres Data Explorer End");
            ViewBag.ctid = ctid;
            return View(ConnectorConfigs);
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetSyncChildRecordsByPageNo(int id, int page, string ctid)
        {
            Console.WriteLine("GetSyncRecordsByPageNo starts");
            var viewComponent = "PGChildRows";
            Console.WriteLine("GetSyncRecordsByPageNo loding the viewcomponent " + viewComponent);
            return ViewComponent(viewComponent, new { id = id, page = page, ctid = ctid });
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetRecordsByPageSize(int id, int page, string ctid, int pagesize, string tab)
        {
            Console.WriteLine("GetSyncRecordsByPageNo starts");
            var viewComponent = "PGRows";
            Console.WriteLine("GetSyncRecordsByPageNo loding the viewcomponent " + viewComponent);
            return ViewComponent(viewComponent, new { id = id, page = page, ctid = ctid, pagesize = pagesize, tab = tab });
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetDuplicateRecord(int id = default(int), string ctid = "", int pagesize = 0, int currentpage = 0)
        {
            Console.WriteLine("GetSyncRecordsByPageNo starts");
            var viewComponent = "PGRows";
            Console.WriteLine("GetSyncRecordsByPageNo loding the viewcomponent " + viewComponent);
            return ViewComponent(viewComponent, new { id = id, ctid = ctid, pagesize = pagesize, page = currentpage });
        }

        [TypeFilter(typeof(AddonPlanFilter))]
        public IActionResult GetUniqueAndDuplicate(int id = default(int), string tab = "", int pagesize = 0, int currentpage = 0)
        {
            Console.WriteLine("GetSyncRecordsByPageNo starts");
            var viewComponent = "PGRows";
            Console.WriteLine("GetSyncRecordsByPageNo loding the viewcomponent " + viewComponent);
            return ViewComponent(viewComponent, new { id = id, tab = tab, pagesize = pagesize, page = currentpage });
        }
        [HttpGet]
        public Task<string> DeleteRecordsReviewedByUser(int id, string itemlist, string ctid)
        {
            var ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
            //var viewComponent = "PGChildRows";
            dynamic DynamicData = JsonConvert.DeserializeObject(itemlist);
            List<string> ctidlist = new List<string>();
            //ctidlist = JsonConvert.DeserializeObject(itemlist);
            Task<string> resultmsg = null;
           // var result = "";
            try
            {
                if (id > 0)
                {
                    var connectorConfig = _connectorsRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);

                    foreach (var item in DynamicData)
                    {
                        //var itemctid = item.Split(",");
                        var itemctid = item["ctid"].Value;
                        ctidlist.Add(itemctid);
                    }
                    if (connectorConfig.backup_before_delete == ArchiveRecords.Yes)
                    {
                        _syncRepository.ArchieveRecordsForDelete(connectorConfig, ctidlist);
                    }
                    //Delete connector by ccid and connectorId
                    //count = _syncRepository.UpdateReviewAndSelectedRecordsForDelete(ccid, id, ctidlist);

                    resultmsg = _syncRepository.DeleteDuplicateChildRecordsByCTids(connectorConfig, ctidlist, ctid);

                    //result = resultmsg;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Delete End");
            //return RedirectToAction("index", "home");
            //return ViewComponent(viewComponent, new { id = id, page = 1, ctid = ctid });

            return resultmsg;
        }

        [HttpGet]
        public Task<string> DeleteUniqueRecordsReviewedByUser(int id, string itemlist)
        {
            var ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
            dynamic DynamicData = JsonConvert.DeserializeObject(itemlist);
            List<string> ctidlist = new List<string>();
            //ctidlist = JsonConvert.DeserializeObject(itemlist);
            Task<string> resultmsg = null;
            try
            {
                if (id > 0)
                {
                    var connectorConfig = _connectorsRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);

                    foreach (var item in DynamicData)
                    {
                        //var itemctid = item.Split(",");
                        var itemctid = item["ctid"].Value;
                        ctidlist.Add(itemctid);
                    }
                    if (connectorConfig.backup_before_delete == ArchiveRecords.Yes)
                    {
                        _syncRepository.ArchieveRecordsForDelete(connectorConfig, ctidlist);
                    }

                    resultmsg = _syncRepository.DeleteUniqueRecordsByCTids(connectorConfig, ctidlist);

                    //result = resultmsg;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            Console.WriteLine("Delete End");
            //return RedirectToAction("index", "home");
            //return ViewComponent(viewComponent, new { id = id, page = 1, ctid = ctid });

            return resultmsg;
        }
        [HttpPost]
        public JsonResult CheckMisMatchingDatatype(ConnectorConfig connectorConfig)
        {
            List<DatabaseConfig> _databaseConfig = new List<DatabaseConfig>();
            if (connectorConfig.dedupSourceType == SourceType.Merge_Table_A_Data_to_Table_B_and_Remove_Duplicates_from_Table_B)
            {
                _databaseConfig = new List<DatabaseConfig>();
                _databaseConfig.Add(connectorConfig.dbConfig_compare);
                _databaseConfig.Add(connectorConfig.dbConfig);
            }
            else
            {
                _databaseConfig = new List<DatabaseConfig>();
                _databaseConfig.Add(connectorConfig.dbConfig);
            }
            int count = 0;
            List<SyncObjectColumn> syncObjectColumn = new List<SyncObjectColumn>();
            List<SyncObjectColumn> matchingColumn1 = null;
            foreach (DatabaseConfig dbc in _databaseConfig)
            {
                var srcTbl = SyncRepository.GetPGDatabaseTableColumns(dbc, dbc.object_name, dbc.db_schema);
                if (count == 0)
                {
                    syncObjectColumn = (from c in srcTbl
                                        select new SyncObjectColumn() { name = c.name, fieldType = c.fieldType.ToString() }).ToList();
                }
                else
                {
                    var notmatchingDatatype = srcTbl.Where(a => syncObjectColumn.Any(b => a.name == b.name && a.fieldType != b.fieldType)).ToList();
                    matchingColumn1 = (from c in notmatchingDatatype
                                       select new SyncObjectColumn() { name = c.name, fieldType = c.fieldType.ToString() }).ToList();
                }
                count++;
            }
            if (matchingColumn1 == null || matchingColumn1.Count() == 0)
            {
                Response.StatusCode = (int)HttpStatusCode.OK;
                return Json(new { Status = Response.StatusCode, Value = matchingColumn1 });
            }
            else
            {
                Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                List<string> fetchDestColumns = (from c in matchingColumn1
                                                 select $"{c.name}").ToList();
                return Json(new { Status = Response.StatusCode, Value = fetchDestColumns });
            }
        }

        [HttpGet]
        public async Task<string> ConfigureNewParentForDuplicates(int id, string newctid, string oldctid)
        {

            Task<string> resultmsg = null;
            var ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
            try
            {
                if (id > 0)
                {

                    var connectorConfig = _connectorsRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);

                    var result = _syncRepository.ConfigureNewParentByCtid(ccid, id, newctid, oldctid);
                    resultmsg = result;
                }
            }
            //return resultmsg;
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            return await resultmsg;
        }

        /// <summary>
        /// Method: GetDeDupSettings
        /// Description: It is used to get dedup settings from dedupsettings table by ccid
        /// </summary>
        /// <param name="ccid"></param>
        /// <returns>DeDupSettings</returns>
        /// 
        [NonAction]
        private DeDupSettings GetDeDupSettings(string ccid)
        {
            DeDupSettings dedupSettings = null;
            try
            {
                //Get DeDupSettings by ccid
                dedupSettings = _connectorsRepository.Get<DeDupSettings>(ccid, null, null);
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            return dedupSettings;
        }

        /// <summary>
        /// Method: GetResources
        /// Description: It is used to get resource info from resources table by resourceId
        /// </summary>
        /// <param name="resourceId"></param>
        /// <returns>Resources</returns>
        [NonAction]
        private async Task<Resources> GetResources(string resourceId)
        {
            Resources resources = null;
            try
            {
                if (!string.IsNullOrEmpty(resourceId))
                {
                    //Create ResourcesRepository instance
                    using (var resourcesRepository = (ResourcesRepository)Utilities.AppServiceProvider.GetService(typeof(IResourcesRepository)))
                    {
                        //Get resource by id
                        resources =await resourcesRepository.Find(resourceId);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
            }
            return resources;
        }

        protected override void Dispose(bool disposing)
        {
            _dedupSettingsRepository.Dispose();
            _connectorsRepository.Dispose();
            base.Dispose(disposing);
        }
    }
}