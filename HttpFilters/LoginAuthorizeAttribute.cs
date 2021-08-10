using System;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Dedup.Extensions;
using System.Web;
using Dedup.Common;

namespace Dedup.HttpFilters
{
    /// <summary>
    /// ActionFilter: LoginAuthorizeAttribute
    /// Description: It is used to check the user session is expired or not.
    /// If not expired then it is allowing to preceed the current request else it will redirect to login controller.
    /// </summary>
    public class LoginAuthorizeAttribute : ActionFilterAttribute, IAuthorizationFilter
    {
        public void OnAuthorization(AuthorizationFilterContext context)
        {
            context.HttpContext.Response.Headers.Remove("Server");
            context.HttpContext.Response.Headers.Remove("X-AspNet-Version");
            context.HttpContext.Response.Headers.Remove("X-AspNetMvc-Version");
            context.HttpContext.Response.Headers.Remove("X-Powered-By");

            //check whether heroku auth token expired or not
            if (context.HttpContext.User.Identity.IsAuthenticated)
            {
                //Console.WriteLine("NameIdentifier: {0}", context.HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
                //Console.WriteLine("Token expiry : {0}", context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_TOKEN_EXPIREDIN));
                //Console.WriteLine("Refresh token: {0}", context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_REFRESH_TOKEN));
                DateTime expiresIn = Convert.ToDateTime(context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_TOKEN_EXPIREDIN));
                if (expiresIn == DateTime.MinValue || string.IsNullOrEmpty(context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_REFRESH_TOKEN)))
                {
                    Console.WriteLine("The current token has expired, Get new token");
                    context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "login", action = "herokuauth", returnUrl = HttpUtility.UrlEncode(context.HttpContext.GetCurrentUrl()) }));
                    return;
                }
                else if (expiresIn < DateTime.Now || expiresIn.AddSeconds(-600) < DateTime.Now)
                {
                    if (string.IsNullOrEmpty(context.HttpContext.GetClaimValue(Dedup.Common.Constants.HEROKU_REFRESH_TOKEN)))
                    {
                        Console.WriteLine("The current token has expired, Get new token");
                        context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "login", action = "herokuauth", returnUrl = HttpUtility.UrlEncode(context.HttpContext.GetCurrentUrl()) }));
                    }
                    else
                    {
                        Console.WriteLine("The current token has expired, Refresh token");
                        context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "login", action = "refreshtoken", returnUrl = HttpUtility.UrlEncode(context.HttpContext.GetCurrentUrl()) }));
                    }
                    return;
                }

                //check resourceId exists or not. If not then read from cookie and set to session
                if (string.IsNullOrEmpty(context.HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)))
                {
                    Console.WriteLine("The current session has expired");
                    context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                    return;
                }

                if (string.IsNullOrEmpty(context.HttpContext.Session.GetString("userEmail"))
                    && !string.IsNullOrEmpty(context.HttpContext.GetClaimValue(ClaimTypes.Email)))
                {
                    context.HttpContext.Session.SetString("userEmail", context.HttpContext.GetClaimValue(ClaimTypes.Email));
                }
            }
            else
            {
                context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "sso", action = "auth", id = "c45af8cf-d142-47d3-bfb5-ccf2ad611ade" }));
                Console.WriteLine("The current session has expired now");
                //context.Result = new RedirectToRouteResult(new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                return;
            }
        }
    }
}
