using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Dedup.Repositories;
//using Pioneer.Pagination;
using Dedup.Extensions;
using Dedup.ViewModels;
using System.Security.Claims;
using Microsoft.Extensions.Hosting;

namespace Dedup.ViewComponents
{
    [ViewComponent(Name = "SQLRows")]
    public class SQLRows : ViewComponent
    {
        private readonly ILogger _logger;
        private readonly IConnectorsRepository _connectorRepository;
       // private readonly IPaginatedMetaService _paginatedMetaService;
      //  private int PAGESIZE = 10;

        public SQLRows(IConnectorsRepository connectorRepository,// IPaginatedMetaService paginatedMetaService,
            ILogger<SQLRows> logger)
        {
            _connectorRepository = connectorRepository;
          //  _paginatedMetaService = paginatedMetaService;
            _logger = logger;
        }

        /// <summary>
        /// Method: InvokeAsync
        /// Description: It is used to get postgres sync data by page wise.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="page"></param>
        /// <param name="isRowsRead"></param>
        /// <param name="connectorConfig"></param>
        /// <returns></returns>
        public async Task<IViewComponentResult> InvokeAsync(int id, int? page, bool isRowsRead = true, ConnectorConfig connectorConfig = null)
        {
            //Get postgres sync data
            return View(await GetSQLRowsAsync(id, page, isRowsRead, connectorConfig));
        }

        /// <summary>
        /// Method: GetPGRowsAsync
        /// Description: It is used to get postgres sync data by page wise.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="page"></param>
        /// <param name="isRowsRead"></param>
        /// <param name="connectorConfig"></param>
        /// <returns></returns>
        private Task<IEnumerable<dynamic>> GetSQLRowsAsync(int id, int? page, bool isRowsRead, ConnectorConfig connectorConfig)
        {
            IEnumerable<dynamic> etDataRows = null;

            try
            {
                var resourceId = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                if (!string.IsNullOrEmpty(resourceId) && id > 0)
                {
                    //Get ConnectorConfig
                    if (connectorConfig == null)
                        connectorConfig = _connectorRepository.Get<ConnectorConfig>(resourceId, id, IsSetConfig: true);

                    //Get the current page
                    int currentPage = page.HasValue ? (int)page : 1;
                    int totalRecords = 0;
                    if (connectorConfig != null)
                    {
                        ViewBag.connectorConfig = connectorConfig;
                        //Get total record
                        totalRecords = 0;// SyncRepository.GetSqlRecordCountByName(connectorConfig);
                        if (totalRecords > 0)
                        {
                            ViewData["count_" + id.ToString()] = totalRecords;
                            //Read sync data if isRowsRead flag is true
                            if (isRowsRead)
                            {
                                //Get page settings
                               // ViewData[id.ToString()] = _paginatedMetaService.GetMetaData(totalRecords, currentPage, PAGESIZE);

                                //Get sync data by pageNo, ccid, connectorId and limit
                               // etDataRows = SyncRepository.FindSqlRowsByPageNo(connectorConfig, currentPage, PAGESIZE);
                            }
                        }
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

            return Task.FromResult(etDataRows);
        }
    }
}
