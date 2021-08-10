using System;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Dedup.Common;

namespace Dedup.HttpFilters
{
    /// <summary>
    /// Action Filter: HerokuAuthorizationFilter
    /// Description : It is used to authorize the addon provision request while this addon provisioning/de-provisioning
    /// </summary>
    public class HerokuAuthorizationFilter : ActionFilterAttribute, IAuthorizationFilter
    {
        public void OnAuthorization(AuthorizationFilterContext context)
        {
            string authHeader = context.HttpContext.Request.Headers["Authorization"];
            if (authHeader != null && authHeader.StartsWith("Basic"))
            {
                //Extract credentials
                string encodedUsernamePassword = authHeader.Substring("Basic ".Length).Trim();
                Encoding encoding = Encoding.GetEncoding("iso-8859-1");
                string usernamePassword = encoding.GetString(Convert.FromBase64String(encodedUsernamePassword));

                int seperatorIndex = usernamePassword.IndexOf(':');

                //Get username & password
                var username = usernamePassword.Substring(0, seperatorIndex);
                var password = usernamePassword.Substring(seperatorIndex + 1);
                Console.WriteLine(string.Format("{0}:{1}", username, password));
                //Validate username and password are matching with addon manifest credentials
                if (username != ConfigVars.Instance.herokuAddonId || password != ConfigVars.Instance.herokuPassword)
                {
                    //Send unauthorized response
                    context.HttpContext.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                    context.Result = new JsonResult("Unauthorized Request");
                }
            }
            else
            {
                //Send unauthorized response
                context.HttpContext.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                context.Result = new JsonResult("Unauthorized Request");
            }
        }
    }
}
