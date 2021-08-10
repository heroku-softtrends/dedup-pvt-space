using Hangfire.Annotations;
using Hangfire.Dashboard;
using Microsoft.AspNetCore.Http;
using System.Collections.Generic;

namespace Dedup.HangfireFilters
{
    /// <summary>
    /// ActionFilter: HangFireAuthorizationFilter
    /// Description: It is used to navigate hangfire dashboard to see all scheduled tasks. We can re-queue/delete/cancel the background jobs
    /// from the hangfire dashboard.
    /// </summary>
    public class HangFireAuthorizationFilter : IDashboardAuthorizationFilter
    {
        List<string> emailLst = new List<string>() { "shanmugamm@softtrends.com", "heroku@softtrends.com", "samant@softtrends.com", "heroku-softtrends@herokumanager.com" };

        public bool Authorize([NotNull] DashboardContext context)
        {
            var httpcontext = context.GetHttpContext();
            if (httpcontext != null && emailLst.Contains(httpcontext.Session.GetString("userEmail")))
                return true;
            else
                return true;
        }
    }
}
