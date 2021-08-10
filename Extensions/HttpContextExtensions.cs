using Dedup.Common;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;

namespace Dedup.Extensions
{
    public static class HttpContextExtensions
    {
        public static void SetCookie(this HttpContext httpContext, string key, string value, CookieOptions options = null)
        {
            if (httpContext == null || string.IsNullOrEmpty(value))
                return;
            if (httpContext.Request == null || httpContext.Response == null)
                return;

            if (httpContext.Request.Cookies.ContainsKey(key))
            {
                httpContext.Response.Cookies.Delete(key, new CookieOptions() { Expires = DateTimeOffset.MinValue });
            }
            value = Utilities.EncryptText(value);
            if (options == null)
            {

                httpContext.Response.Cookies.Append(key, value);
            }
            else
            {
                //options.Domain = httpContext.Request.Host.ToString();
                options.Path = "/";
                options.Secure = true;
                httpContext.Response.Cookies.Append(key, value, options);
            }
        }

        public static void SetCookie(this HttpContext httpContext, string key, string value, int? expireTime, CookieExpiryIn? expiryIn)
        {
            if (httpContext == null || string.IsNullOrEmpty(value))
                return;
            if (httpContext.Request == null || httpContext.Response == null)
                return;

            if (httpContext.Request.Cookies.ContainsKey(key))
            {
                httpContext.Response.Cookies.Delete(key, new CookieOptions() { Expires = DateTimeOffset.MinValue });
            }
            value = Utilities.EncryptText(value);
            if (expireTime.HasValue && expiryIn.HasValue)
            {
                var options = new CookieOptions() { Path = "/", Secure = true };
                if (expiryIn.HasValue)
                {
                    switch (expiryIn.Value)
                    {
                        case CookieExpiryIn.Seconds:
                            options.Expires = DateTime.Now.AddSeconds(expireTime.Value);
                            break;
                        case CookieExpiryIn.Minitues:
                            options.Expires = DateTime.Now.AddMinutes(expireTime.Value);
                            break;
                        case CookieExpiryIn.Hours:
                            options.Expires = DateTime.Now.AddHours(expireTime.Value);
                            break;
                        case CookieExpiryIn.Days:
                            options.Expires = DateTime.Now.AddDays(expireTime.Value);
                            break;
                        case CookieExpiryIn.Months:
                            options.Expires = DateTime.Now.AddMonths(expireTime.Value);
                            break;
                        case CookieExpiryIn.Years:
                            options.Expires = DateTime.Now.AddYears(expireTime.Value);
                            break;
                    }
                }
                else
                {
                    options.Expires = DateTime.Now.AddDays(expireTime.Value);
                }
                httpContext.Response.Cookies.Append(key, value, options);
            }
            else
            {
                httpContext.Response.Cookies.Append(key, value);
            }
        }

        public static void RemoveCookie(this HttpContext httpContext, string key)
        {
            if (httpContext == null || string.IsNullOrEmpty(key))
                return;
            if (httpContext.Request == null)
                return;
            if (httpContext.Request.Cookies.ContainsKey(key))
                httpContext.Response.Cookies.Delete(key);
        }

        public static string GetCookie(this HttpContext httpContext, string key)
        {
            if (httpContext == null || string.IsNullOrEmpty(key))
                return "";
            if (httpContext.Request == null)
                return "";

            string cookieValue = string.Empty;
            if (httpContext.Request.Cookies.ContainsKey(key))
            {
                cookieValue = httpContext.Request.Cookies[key];
                if (!string.IsNullOrEmpty(cookieValue))
                    cookieValue = Utilities.DecryptText(cookieValue);
            }
            return cookieValue;
        }

        public async static void AddUpdateClaims(this HttpContext httpContext, List<Claim> claims)
        {
            var identity = httpContext.User.Identity as ClaimsIdentity;
            if (identity == null || claims == null || (claims != null && claims.Count() == 0))
                return;

            for (int index = 0; index < claims.Count(); index++)
            {
                // check for existing claim and remove it
                var existingClaim = identity.FindFirst(claims.ElementAt(index).Type);
                if (existingClaim != null)
                    identity.RemoveClaim(existingClaim);

                //clear all cookie
                if (httpContext.Request.Cookies.Keys.Where(p => p == claims.ElementAt(index).Type).Count() > 0)
                    httpContext.Response.Cookies.Delete(claims.ElementAt(index).Type);

                // add new claim
                if (!string.IsNullOrEmpty(claims.ElementAt(index).Value))
                {
                    identity.AddClaim(new Claim(claims.ElementAt(index).Type, claims.ElementAt(index).Value));
                }
            }
 
            await httpContext.SignOutAsync(Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME);
            ClaimsPrincipal principal = new ClaimsPrincipal(identity);
            await httpContext.SignInAsync(Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME, principal);
        }

        public async static void ClearClaims(this HttpContext httpContext)
        {
            var identity = httpContext.User.Identity as ClaimsIdentity;
            if (identity == null)
                return;

            for (int index = 0; index < identity.Claims.Count(); index++)
            {
                //clear all cookie
                if (httpContext.Request.Cookies.Keys.Where(p => p == identity.Claims.ElementAt(index).Type).Count() > 0)
                    httpContext.Response.Cookies.Delete(identity.Claims.ElementAt(index).Type);

                //remove claim at position
                identity.RemoveClaim(identity.Claims.ElementAt(index));

                //decrease by 1
                index--;
            }

            await httpContext.SignOutAsync(Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME);
        }

        public static string GetClaimValue(this HttpContext httpContext, string key)
        {
            var identity = httpContext.User.Identity as ClaimsIdentity;
            if (identity == null)
                return null;


            if (identity.Claims.Where(c => c.Type == key).Count() > 0)
                return identity.Claims.FirstOrDefault(c => c.Type == key).Value;

            return null;
        }

        public static string GetCurrentUrl(this HttpContext httpContext)
        {
            return string.Format("{0}://{1}{2}{3}", httpContext.Request.Scheme, httpContext.Request.Host, httpContext.Request.Path, httpContext.Request.QueryString);
        }
    }

}
