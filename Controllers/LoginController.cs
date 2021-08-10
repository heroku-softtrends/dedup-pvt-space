using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Dedup.Services;
using Dedup.ViewModels;
using System.Net;
using Dedup.Extensions;
using Dedup.Repositories;
using System.Security.Claims;
using System.Collections.Generic;
using System.Web;
using Dedup.Common;
using Dedup.Models;
using Microsoft.AspNetCore.Hosting;

namespace Dedup.Controllers
{
    public class LoginController : Controller
    {
        private readonly IAuthTokenRepository _authTokenRepository;

        public LoginController(IAuthTokenRepository authTokenRepository)
        {
            _authTokenRepository = authTokenRepository;
        }

        /// <summary>
        /// Action: Index
        /// Description: It is called to get heroku auth token url. If token url is empty then redirect to forbidden page
        /// else redirect to action(GetToken) and get the heroku auth token url
        /// </summary>
        /// <returns></returns>
        [ActionName("herokuauth")]
        public async Task<ActionResult> Index(string returnUrl = "")
        {
            Console.WriteLine("Login Controller- Index");
            AuthTokens authTokenEntity = null;
            if (!string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)))
            {
                //get auth token by userId
                authTokenEntity = await _authTokenRepository.Find(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
            }
            else
            {
                TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                TempData["errorMessage"] = string.Format("The current session has expired. Please try to access the {0} addon from heroku dashboard.", ConfigVars.Instance.herokuAddonId);
                return RedirectToAction("forbidden", "home");
            }

            if (authTokenEntity == null || (null != authTokenEntity && (authTokenEntity.expires_in == DateTime.MinValue || string.IsNullOrEmpty(authTokenEntity.user_id))
                || (authTokenEntity.expires_in != DateTime.MinValue && authTokenEntity.expires_in.AddSeconds(-600) < DateTime.Now)))
            {
                var tokenUrl = GetToken();
                if (string.IsNullOrEmpty(tokenUrl))
                {
                    TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                    TempData["errorMessage"] = "You are not authenticated due to heroku auth token not accessed.";
                    return RedirectToAction("forbidden", "home");
                }
                else
                {
                    if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") != EnvironmentName.Development
                    && !string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)))
                    {
                        if (await _authTokenRepository.IsValidResource(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)))
                        {
                            if (!Utilities.IsAddonPrivatePlan(HttpContext.GetClaimValue(ClaimTypes.Version)))
                            {
                                //update return url before sending auth request
                                await _authTokenRepository.UpdateReturnUrl(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier), returnUrl);
                            }
                        }
                        else
                        {
                            //clear all old claims
                            HttpContext.ClearClaims();
                            TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                            TempData["errorMessage"] = "You are not authenticated due to heroku auth token not accessed.";
                            return RedirectToAction("forbidden", "home");
                        }
                    }

                    return Redirect(tokenUrl);
                }
            }
            else
            {
                var claims = new List<Claim>();
                claims.Add(new Claim(ClaimTypes.NameIdentifier, authTokenEntity.Resource.uuid));
                claims.Add(new Claim(ClaimTypes.Version, authTokenEntity.Resource.plan));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.user_email))
                    claims.Add(new Claim(ClaimTypes.Email, authTokenEntity.Resource.user_email));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.app_name))
                    claims.Add(new Claim(ClaimTypes.Name, authTokenEntity.Resource.app_name));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.private_app_url))
                    claims.Add(new Claim(ClaimTypes.Uri, authTokenEntity.Resource.private_app_url));
                claims.Add(new Claim(Constants.HEROKU_ACCESS_TOKEN, authTokenEntity.access_token));
                claims.Add(new Claim(Constants.HEROKU_REFRESH_TOKEN, authTokenEntity.refresh_token));
                claims.Add(new Claim(Constants.HEROKU_TOKEN_EXPIREDIN, authTokenEntity.expires_in.ToString()));
                if (!string.IsNullOrEmpty(authTokenEntity.user_id))
                    claims.Add(new Claim(Constants.HEROKU_USERID, authTokenEntity.user_id));
                HttpContext.AddUpdateClaims(claims);

                //redirect to url
                if (string.IsNullOrEmpty(returnUrl)
                    || (!string.IsNullOrEmpty(returnUrl) && !Uri.IsWellFormedUriString(returnUrl, UriKind.RelativeOrAbsolute)))
                {
                    return RedirectToAction("index", "home");
                }
                else
                {
                    Console.WriteLine("Redirect to: {0}", HttpUtility.UrlDecode(returnUrl));

                    //init uri
                    var redirectUrl = new Uri(HttpUtility.UrlDecode(returnUrl));

                    //Redirect to home page
                    return Redirect(redirectUrl.OriginalString);
                }
            }
        }

        /// <summary>
        /// Action: GetToken
        /// Description: It is called to get heroku auth token by calling heroku auth token url.
        /// If user is not yet logged-in to his/her heroku account then it will redirect to heroku official login page else
        /// it will redirect to oauthcallback action
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [ActionName("GetToken")]
        public string GetToken()
        {
            Console.WriteLine("Login Controller- Get Token");
            //get heroku auth token url
            return HerokuApi.GetToken();
        }

        /// <summary>
        /// Action: oauthcallback
        /// Description: It is a callback action. The re-direction call will come here after calling heroku auth token url.
        /// Heroku auth token code is read from query string and that code is used to get heroku auth token.
        /// After getting heroku auth token it will redirect to home page. If not getting token then redirect to forbidden page
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [ActionName("oauthcallback")]
        public async Task<ActionResult> oauthcallback()
        {
            Console.WriteLine("Login Controller- oauthcallback");
            //Get heroku auth token
            HerokuAuthToken authToken = await HerokuApi.GetHerokuAccessToken(Request.Query["code"], AuthGrantType.authorization_code);
            if (authToken.IsNull())
            {
                Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                TempData["errorMessage"] = "You are not authenticated due to heroku auth token not received.";
                return RedirectToAction("forbidden", "home");
            }
            else
            {
                //update heroku auth token
                var authTokenEntity = authToken.ToAuthToken();
                //assign current resourceId as auth_id
                authTokenEntity.auth_id = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);
                if (string.IsNullOrEmpty(authTokenEntity.auth_id))
                {
                    TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                    TempData["errorMessage"] = string.Format("The current session has expired. Please try to access the {0} addon from heroku dashboard.", ConfigVars.Instance.herokuAddonId);
                    return RedirectToAction("forbidden", "home");
                }

                authTokenEntity = await _authTokenRepository.Add(authTokenEntity);
                if (authTokenEntity.Resource != null)
                {
                    var claims = new List<Claim>();
                    claims.Add(new Claim(ClaimTypes.NameIdentifier, authTokenEntity.Resource.uuid));
                    claims.Add(new Claim(ClaimTypes.Version, authTokenEntity.Resource.plan));
                    if (!string.IsNullOrEmpty(authTokenEntity.Resource.user_email))
                        claims.Add(new Claim(ClaimTypes.Email, authTokenEntity.Resource.user_email));
                    if (!string.IsNullOrEmpty(authTokenEntity.Resource.app_name))
                        claims.Add(new Claim(ClaimTypes.Name, authTokenEntity.Resource.app_name));
                    if (!string.IsNullOrEmpty(authTokenEntity.Resource.private_app_url))
                        claims.Add(new Claim(ClaimTypes.Uri, authTokenEntity.Resource.private_app_url));
                    claims.Add(new Claim(Constants.HEROKU_ACCESS_TOKEN, authTokenEntity.access_token));
                    claims.Add(new Claim(Constants.HEROKU_REFRESH_TOKEN, authTokenEntity.refresh_token));
                    claims.Add(new Claim(Constants.HEROKU_TOKEN_EXPIREDIN, authTokenEntity.expires_in.ToString()));
                    if (!string.IsNullOrEmpty(authTokenEntity.user_id))
                        claims.Add(new Claim(Constants.HEROKU_USERID, authTokenEntity.user_id));
                    HttpContext.AddUpdateClaims(claims);

                    if (string.IsNullOrEmpty(authTokenEntity.redirect_url)
                    || (!string.IsNullOrEmpty(authTokenEntity.redirect_url) && !Uri.IsWellFormedUriString(authTokenEntity.redirect_url, UriKind.RelativeOrAbsolute)))
                    {
                        return RedirectToAction("index", "home");
                    }
                    else
                    {
                        if ((ConfigVars.Instance.addonPrivatePlanLevels != null && ConfigVars.Instance.addonPrivatePlanLevels.Contains(HttpContext.GetClaimValue(ClaimTypes.Version))))
                        {
                            if (string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.Uri)))
                            {
                                return RedirectToAction("index", "home");
                            }
                            else
                            {
                                //Redirect to home page
                                return Redirect(HttpContext.GetClaimValue(ClaimTypes.Uri));
                            }
                        }
                        else
                        {
                            Console.WriteLine("Redirect to: {0}", HttpUtility.UrlDecode(authTokenEntity.redirect_url));

                            //init uri
                            var redirectUrl = new Uri(HttpUtility.UrlDecode(authTokenEntity.redirect_url));

                            //Redirect to home page
                            return Redirect(redirectUrl.OriginalString);
                        }
                    }
                }
                else
                {
                    TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                    TempData["errorMessage"] = string.Format("The current session has expired. Please try to access the {0} addon from heroku dashboard.", ConfigVars.Instance.herokuAddonId);
                    return RedirectToAction("forbidden", "home");
                }
            }
        }

        [HttpGet]
        public async Task<ActionResult> refreshtoken(string returnUrl = "")
        {
            Console.WriteLine("Login Controller- refreshtoken");
            AuthTokens authTokenEntity = null;
            if (!string.IsNullOrEmpty(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier)))
            {
                //get auth token by userId
                authTokenEntity = await _authTokenRepository.Find(HttpContext.GetClaimValue(ClaimTypes.NameIdentifier));
            }
            else
            {
                TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                TempData["errorMessage"] = string.Format("The current session has expired. Please try to access the {0} addon from heroku dashboard.", ConfigVars.Instance.herokuAddonId);
                return RedirectToAction("forbidden", "home");
            }

            //Get heroku auth token
            HerokuAuthToken authToken = await HerokuApi.GetHerokuAccessToken(HttpContext.GetClaimValue(Constants.HEROKU_REFRESH_TOKEN), AuthGrantType.refresh_token);
            if (authToken.IsNull())
            {
                return RedirectToAction("herokuauth", "login", new { returnUrl = returnUrl });
            }
            else
            {
                //assign current resourceId as auth_id
                authToken.auth_id = HttpContext.GetClaimValue(ClaimTypes.NameIdentifier);

                //update heroku auth token
                authTokenEntity = authToken.ToAuthToken();
                authTokenEntity.redirect_url = returnUrl;
                authTokenEntity = await _authTokenRepository.Add(authTokenEntity);
            }

            if (null != authTokenEntity)
            {
                var claims = new List<Claim>();
                claims.Add(new Claim(ClaimTypes.NameIdentifier, authTokenEntity.Resource.uuid));
                claims.Add(new Claim(ClaimTypes.Version, authTokenEntity.Resource.plan));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.user_email))
                    claims.Add(new Claim(ClaimTypes.Email, authTokenEntity.Resource.user_email));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.app_name))
                    claims.Add(new Claim(ClaimTypes.Name, authTokenEntity.Resource.app_name));
                if (!string.IsNullOrEmpty(authTokenEntity.Resource.private_app_url))
                    claims.Add(new Claim(ClaimTypes.Uri, authTokenEntity.Resource.private_app_url));
                claims.Add(new Claim(Constants.HEROKU_ACCESS_TOKEN, authTokenEntity.access_token));
                claims.Add(new Claim(Constants.HEROKU_REFRESH_TOKEN, authTokenEntity.refresh_token));
                claims.Add(new Claim(Constants.HEROKU_TOKEN_EXPIREDIN, authTokenEntity.expires_in.ToString()));
                if (!string.IsNullOrEmpty(authTokenEntity.user_id))
                    claims.Add(new Claim(Constants.HEROKU_USERID, authTokenEntity.user_id));

                HttpContext.AddUpdateClaims(claims);

                //redirect to url
                if (string.IsNullOrEmpty(authTokenEntity.redirect_url)
                    || (!string.IsNullOrEmpty(authTokenEntity.redirect_url) && !Uri.IsWellFormedUriString(authTokenEntity.redirect_url, UriKind.RelativeOrAbsolute)))
                {
                    return RedirectToAction("index", "home");
                }
                else
                {
                    Console.WriteLine("Redirect to: {0}", HttpUtility.UrlDecode(authTokenEntity.redirect_url));

                    //init uri
                    var redirectUrl = new Uri(HttpUtility.UrlDecode(authTokenEntity.redirect_url));

                    //Redirect to home page
                    return Redirect(redirectUrl.OriginalString);
                }
            }
            else
            {
                Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                TempData["httpStatusCode"] = HttpStatusCode.Unauthorized;
                TempData["errorMessage"] = "You are not authenticated due to heroku auth token not received.";
                return RedirectToAction("forbidden", "home");
            }
        }
    }
}
