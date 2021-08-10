using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Dedup.Repositories;
using Pioneer.Pagination;
using Dedup.Extensions;
using Dedup.ViewModels;
using Microsoft.Extensions.Hosting;
using System.Security.Claims;

namespace Dedup.ViewComponents
{
    [ViewComponent(Name = "PGChildRows")]
    public class PGChildRows : ViewComponent
    {
        private readonly ILogger _logger;
        private readonly IConnectorsRepository _connectorRepository;
        private readonly IPaginatedMetaService _paginatedMetaService;
        private readonly ISyncRepository _syncRepository;
        private int PAGESIZE = 10;

        public PGChildRows(IConnectorsRepository connectorRepository, IPaginatedMetaService paginatedMetaService, ILogger<PGRows> logger, ISyncRepository syncRepository)
        {
            _connectorRepository = connectorRepository;
            _paginatedMetaService = paginatedMetaService;
            _logger = logger;
            _syncRepository = syncRepository;
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
        public async Task<IViewComponentResult> InvokeAsync(int id, int? page, bool isRowsRead = true, string ctid = null)
        {
            //Get postgres sync data
            return View(await GetChildPGRowsAsync(id, page, isRowsRead, ctid));
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
        private async Task<IList<IDictionary<string, object>>> GetChildPGRowsAsync(int id, int? page, bool isRowsRead, string ctid)
        {
            Task<IList<IDictionary<string, object>>> pgRows = null;

            try
            {

                var ccid = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                ConnectorConfig connectorConfig = _connectorRepository.Get<ConnectorConfig>(ccid, id, IsSetConfig: true);

                if (!string.IsNullOrEmpty(ccid) && id > 0)
                {
                    
                    ViewBag.connectorConfig = connectorConfig;

                    //Get the current page
                    int currentPage = page.HasValue ? (int)page : 1;
                   
                        var result = _syncRepository.GetChildRecordsByParentForReviewAndDelete(ccid, id, ctid, currentPage, PAGESIZE);
                      pgRows = result;    
                    
                }
            }
            catch (Exception ex)
            {
                if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == Environments.Development)
                    _logger.LogError(ex.Message, ex);
                else
                    Console.WriteLine("ERROR: {0}", ex.Message);
            }

            return await pgRows;
        }

    }
}
