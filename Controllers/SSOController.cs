using System;
using Microsoft.AspNetCore.Mvc;
using Dedup.Common;
using Dedup.Repositories;
using Microsoft.AspNetCore.Http;
using Dedup.ViewModels;
using Dedup.Extensions;
using Microsoft.AspNetCore.Routing;
using System.Net;
using System.Threading.Tasks;
using System.Security.Claims;
using System.Collections.Generic;
using Microsoft.AspNetCore.Authentication;
using RestSharp.Extensions.MonoHttp;

namespace Dedup.Controllers
{
    public class SSOController : Controller
    {
        private IResourcesRepository _resourcesRepository;
        private readonly IAuthTokenRepository _authTokenRepository;

        public SSOController(IResourcesRepository resourcesRepository, IAuthTokenRepository authTokenRepository)
        {
            _resourcesRepository = resourcesRepository;
            _authTokenRepository = authTokenRepository;
        }

        /// <summary>
        /// Action: Index
        /// Description: It is called to authorise user to access the addon when user is coming from heroku dashboard
        /// by clicking on addon
        /// </summary>
        /// <param name="data"></param>
        /// <returns>StatusCodeResult</returns>
        [HttpPost("sso/login")]
        public async Task<IActionResult> Index([FromQuery] SSOData data)
        {
            try
            {
                Console.WriteLine("SSO Controller- Index");
                string id = HttpContext.Request.Form["resource_id"];
                string token = HttpContext.Request.Form["resource_token"];
                string timestamp = HttpContext.Request.Form["timestamp"];
                string appName = HttpContext.Request.Form["app"];
                string userEmail = HttpContext.Request.Form["email"];
                string userId = HttpContext.Request.Form["user_id"];
                foreach (var key in HttpContext.Request.Form.Keys)
                {
                    Console.WriteLine("{0}=>{1}", key, HttpContext.Request.Form[key]);
                }

                //Clear all sessions
                HttpContext.Session.Clear();
                //Generate token by using resource-id,Heroku Salt and timestamp value
                var preToken = string.Format("{0}:{1}:{2}", id, ConfigVars.Instance.herokuSalt, timestamp);

                //Convert to hash string
                preToken = Utilities.SHA1HashStringForUTF8String(preToken);
                Console.WriteLine(token + " : " + preToken);
                Console.WriteLine(timestamp);

                //Check token is matching with preToken. If not then throw error
                if (token != preToken)
                {
                    Console.WriteLine("token not match");
                    return new StatusCodeResult(403);
                }

                //Check timestamp value is expired or not. If it expired then throw error
                if (Convert.ToInt64(timestamp) < Utilities.ConvertToUnixTime(DateTime.Now.AddMinutes(-(2 * 60))))
                {
                    return new StatusCodeResult(403);
                }

                //validate account by resource id
                var resources = await _resourcesRepository.Find(id, true);
                if (resources == null)
                {
                    return new StatusCodeResult(404);
                }

                string lAppName = resources.app_name;
                await Task.Run(async () =>
                {
                    //Update the main app name in resource table based on resource-id if app name is null
                    if (!string.IsNullOrEmpty(appName) && (string.IsNullOrEmpty(lAppName)
                    || (!string.IsNullOrEmpty(lAppName) && !lAppName.Trim().Equals(appName.Trim(), StringComparison.OrdinalIgnoreCase))))
                    {
                        //Update app name
                        await _resourcesRepository.UpdateAppName(id, appName).ConfigureAwait(false);
                    }
                }).ConfigureAwait(false);

                //clear all cookie
                foreach (var cookie in Request.Cookies.Keys)
                {
                    Response.Cookies.Delete(cookie);
                }

                var claims = new List<Claim>();
                claims.Add(new Claim(ClaimTypes.NameIdentifier, resources.uuid));
                claims.Add(new Claim(ClaimTypes.Version, resources.plan));
                if (!string.IsNullOrEmpty(resources.app_name))
                    claims.Add(new Claim(ClaimTypes.Name, resources.app_name));
                if (!string.IsNullOrEmpty(resources.user_email))
                    claims.Add(new Claim(ClaimTypes.Email, resources.user_email));
                if (!string.IsNullOrEmpty(resources.private_app_url))
                    claims.Add(new Claim(ClaimTypes.Uri, resources.private_app_url));
                //if (!string.IsNullOrEmpty(userId))
                //{
                //    //get auth token by userId
                //    var lAuthToken = await _authTokenRepository.FindByUserId(userId);
                //    if (lAuthToken != null)
                //    {
                //        Console.WriteLine("auth_id {0}:{1}", id, lAuthToken.auth_id);
                //        Console.WriteLine("access_token {0}", lAuthToken.access_token);
                //        Console.WriteLine("refresh_token token {0}", lAuthToken.refresh_token);
                //        Console.WriteLine("expires_in {0}", lAuthToken.expires_in);
                //        Console.WriteLine("user_id {0}", lAuthToken.user_id);
                //        Console.WriteLine("token_type {0}", lAuthToken.token_type);
                //        //claims.Add(new Claim(Constants.HEROKU_ACCESS_TOKEN, lAuthToken.access_token));
                //        //claims.Add(new Claim(Constants.HEROKU_REFRESH_TOKEN, lAuthToken.refresh_token));
                //        //claims.Add(new Claim(Constants.HEROKU_TOKEN_EXPIREDIN, lAuthToken.expires_in.ToString()));
                //        //claims.Add(new Claim(Constants.HEROKU_USERID, lAuthToken.user_id));
                //        //Add authtoken
                //        //if (resources.AuthToken == null)
                //        //{
                //        //    var authTokenEntity = new AuthTokens()
                //        //    {
                //        //        auth_id = id,
                //        //        access_token = lAuthToken.access_token,
                //        //        refresh_token = lAuthToken.refresh_token,
                //        //        expires_in = lAuthToken.expires_in,
                //        //        user_id = lAuthToken.user_id,
                //        //        token_type = lAuthToken.token_type,
                //        //    };
                //        //    authTokenEntity = await _authTokenRepository.Add(authTokenEntity);
                //        //}
                //    }
                //}

                await HttpContext.SignOutAsync(Constants.DEFAULT_AUTH_COOKIE_SCHEME);
                ClaimsPrincipal principal = new ClaimsPrincipal(new ClaimsIdentity(claims, Constants.DEFAULT_AUTH_COOKIE_SCHEME));
                await HttpContext.SignInAsync(Constants.DEFAULT_AUTH_COOKIE_SCHEME, principal);

                //redirect to home page
                return RedirectToAction("index", "home");
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                return new StatusCodeResult(500);
            }
        }

        /// <summary>
        /// Action: Auth
        /// Description: It is called to re-authorise by app itself if the current session has expired.
        /// If user authorised then redirect to home page else redirect to forbidden page
        /// </summary>
        /// <returns></returns>
        [HttpGet("sso/auth"), Route("sso/auth/{id?}")]
        public async Task<IActionResult> auth(string id = "")
        {
            try
            {
                Console.WriteLine("SSO Controller- Auth");
                //clear all session
                HttpContext.Session.Clear();

                if (string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)) && string.IsNullOrEmpty(id))
                {
                    Console.WriteLine("The current session has expired");
                    TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                    TempData["errorMessage"] = string.Format("The current session has expired. Please try to access the {0} addon from heroku dashboard.", ConfigVars.Instance.herokuAddonId);
                    return RedirectToAction("forbidden", new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                }
                else
                {
                    if (string.IsNullOrEmpty(id))
                    {
                        //assign resource id
                        id = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                    }

                    //validate account by resource id
                    var resources = await _resourcesRepository.Find(id, true);
                    if (resources == null)
                    {
                        Console.WriteLine("The resource-Id is not matching");
                        TempData["httpStatusCode"] = HttpStatusCode.NotFound;
                        return RedirectToAction("forbidden", new RouteValueDictionary(new { controller = "home", action = "forbidden" }));
                    }

                    //clear all cookie
                    foreach (var cookie in Request.Cookies.Keys)
                    {
                        Response.Cookies.Delete(cookie);
                    }

                    //Clear all sessions
                    HttpContext.Session.Clear();

                    var claims = new List<Claim>();
                    claims.Add(new Claim(ClaimTypes.NameIdentifier, resources.uuid));
                    claims.Add(new Claim(ClaimTypes.Version, resources.plan));
                    if (!string.IsNullOrEmpty(resources.app_name))
                        claims.Add(new Claim(ClaimTypes.Name, resources.app_name));
                    if (!string.IsNullOrEmpty(resources.user_email))
                        claims.Add(new Claim(ClaimTypes.Email, resources.user_email));
                    if (!string.IsNullOrEmpty(resources.private_app_url))
                    {
                        claims.Add(new Claim(ClaimTypes.Uri, resources.private_app_url));
                        if (ConfigVars.Instance.deDupPvtEdition)
                        {
                            //refresh all configs
                            await ConfigVars.Instance.LoadDeDupConfigsByTypeAsync();

                            //set kafka serevr url
                           // Utilities.SetPrivateKafkaServerUrl(resources.private_app_url);
                        }
                    }
                    if (resources.AuthToken != null)
                    {
                        claims.Add(new Claim(Constants.HEROKU_ACCESS_TOKEN, resources.AuthToken.access_token));
                        claims.Add(new Claim(Constants.HEROKU_REFRESH_TOKEN, resources.AuthToken.refresh_token));
                        claims.Add(new Claim(Constants.HEROKU_TOKEN_EXPIREDIN, resources.AuthToken.expires_in.ToString()));
                        if (!string.IsNullOrEmpty(resources.AuthToken.user_id))
                            claims.Add(new Claim(Constants.HEROKU_USERID, resources.AuthToken.user_id));
                    }

                    await HttpContext.SignOutAsync(Constants.DEFAULT_AUTH_COOKIE_SCHEME);
                    ClaimsPrincipal principal = new ClaimsPrincipal(new ClaimsIdentity(claims, Constants.DEFAULT_AUTH_COOKIE_SCHEME));
                    await HttpContext.SignInAsync(Constants.DEFAULT_AUTH_COOKIE_SCHEME, principal);
                    if (ConfigVars.Instance.deDupPvtEdition)
                    {
                        //redirect to home page
                        return RedirectToAction("index", "home");
                    }
                    else
                    {
                        //redirect to auth page
                        return RedirectToAction("herokuauth", "login");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: {0}", ex.Message);
                return new StatusCodeResult(500);
            }
        }
    }
}