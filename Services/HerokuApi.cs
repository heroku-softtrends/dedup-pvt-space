using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using Dedup.Extensions;
using Dedup.Common;
using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Text;
using System.Linq;
using System.IO;
using Newtonsoft.Json.Serialization;
using System.Web;

namespace Dedup.Services
{
    public static class HerokuApi
    {
        private static IRequestFactory mFactory { get; set; }

        static HerokuApi()
        {
            mFactory = new RequestFactory();
        }

        public static AccountInfo GetAccountInfo(string herokuAuthToken)
        {
            AccountInfo accountInfo = default(AccountInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "/account";
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    accountInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AccountInfo>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return accountInfo;
        }

        public static string GetVendorAppName(string addonId, string herokuAuthToken)
        {
            string appName = string.Empty;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("addons/{0}", addonId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    AddonInfo appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AddonInfo>(httpResponse);
                    if (!appInfo.IsNull() && !appInfo.app.IsNull())
                    {
                        appName = appInfo.app.name;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appName;
        }

        public static IDictionary<string, string> GetAppDetails(string name, string herokuAuthToken)
        {
            IDictionary<string, string> appInfo = null;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}", name);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appInfo;
        }

        public static Task<AppInfo> PublishMCAppInPrivateSpace(string resourceId, string regionName, string organizationName, string appName, string spaceName, string herokuAuthToken, ref AppSetupInfo appSetupInfo, out string errorMessage)//, bool isODBC)
        {
            AppInfo appInfo = default(AppInfo);
            errorMessage = string.Empty;
            try
            {
                appInfo = GetAppInfo(string.Format("{0}-{1}", spaceName, appName), herokuAuthToken).Result;
                if (appInfo.IsNull())
                {
                    var request = mFactory.CreateRequest();
                    request.Resource = "app-setups";
                    request.Method = Method.POST;
                    request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                    request.AddHeader("Content-Type", "application/json");
                    request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                    var appSetups = new AppSetups();
                    appSetups.source_blob = new AppSourceBlob()
                    {
                        url = ConfigVars.Instance.deDupBuildGitRepoUrl,
                        version = ConfigVars.Instance.deDupPvtAppVersion
                    };
                    appSetups.app = new AppSetupAppInfo()
                    {
                        locked = false,
                        name = string.Format("{0}-{1}", spaceName, appName),
                        organization = organizationName,
                        personal = false,
                        region = regionName,
                        space = spaceName,
                        stack = ConfigVars.Instance.dedupPvtAppStackVersion
                    };

                    //create queue from heroku app name
                    var guidFromAppName = Utilities.ConvertToMd5HashGUID(appSetups.app.Value.name);
                    Console.WriteLine("Guid from app name: {0} => {1}", appSetups.app.Value.name, guidFromAppName);
                    var queueName = $"critical_{Math.Abs(guidFromAppName.ToInt())}".Trim();
                    Console.WriteLine("Queue: {0} assigned for app: {1}", queueName, appSetups.app.Value.name);
                    if (queueName.Length > 20)
                    {
                        queueName = queueName.Substring(0, 20);
                        Console.WriteLine("Shorten queue name: {0} assigned for app: {1}", queueName, appSetups.app.Value.name);
                    }
                    //if (isODBC)
                    //{
                    //    appSetups.overrides = new AppSetupOverrides()
                    //    {
                    //        buildpacks = new List<AppBuildpack>(),
                    //        env = new AppConfigvars()
                    //        {
                    //            MC_PROXY_APP = appSetups.app.Value.name,
                    //            JOB_QUEUE_NAME = queueName,
                    //            AMAZONREDSHIFTODBCINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.amazon_redshift_odbc_ini_path)) ? ConfigVars.Instance.amazon_redshift_odbc_ini_path : null,
                    //            LD_LIBRARY_PATH = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.ld_library_path)) ? ConfigVars.Instance.ld_library_path : null,
                    //            ODBCINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbc_ini_path)) ? ConfigVars.Instance.odbc_ini_path : null,
                    //            ODBCSYSINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbc_sys_ini_path)) ? ConfigVars.Instance.odbc_sys_ini_path : null,
                    //        }
                    //    };
                    //}
                    else
                    {
                        appSetups.overrides = new AppSetupOverrides()
                        {
                            buildpacks = new List<AppBuildpack>(),
                            env = new AppConfigvars()
                            {
                                DEDUP_PROXY_APP = appSetups.app.Value.name,
                                JOB_QUEUE_NAME = queueName,
                            }
                        };
                    }
                    //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbcBuildpackUrl))
                    //{
                    //    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    //    {
                    //        url = ConfigVars.Instance.odbcBuildpackUrl
                    //    });
                    //}
                    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    {
                        url = ConfigVars.Instance.deDupBuildpackUrl
                    });
                    //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.redshiftBuildpackUrl))
                    //{
                    //    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    //    {
                    //        url = ConfigVars.Instance.redshiftBuildpackUrl
                    //    });
                    //}
                    JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
                    {
                        ContractResolver = new DefaultContractResolver()
                    };
                    string jsonObject = JsonConvert.SerializeObject(appSetups, Formatting.Indented, jsonSerializerSettings);
                    Console.WriteLine("Appsetup: {0}", jsonObject);
                    request.AddParameter("application/json", jsonObject, ParameterType.RequestBody);

                    var client = mFactory.CreateHerokuRestClient();
                    IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                    Console.WriteLine("Appsetup-Build: {0}", httpResponse.Content);
                    if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted))
                    {
                        errorMessage = "succeeded";
                        appSetupInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppSetupInfo>(httpResponse);
                        Console.WriteLine("Appsetup response received");
                    }
                    else
                    {
                        errorMessage = "failed";
                    }
                }
                else
                {
                    errorMessage = "succeeded";
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                errorMessage = ex.Message;
            }

            return Task.FromResult(appInfo);
        }

        public static Task<AppSetupInfo> GetPublishedAppStatusInPrivateSpace(string buildId, string herokuAuthToken, out string errorMessage)
        {
            errorMessage = string.Empty;
            AppSetupInfo appSetupInfo = default(AppSetupInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("app-setups/{0}", buildId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted))
                {
                    appSetupInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppSetupInfo>(httpResponse);
                    Console.WriteLine("Appsetup response received");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                errorMessage = ex.Message;
            }

            return Task.FromResult(appSetupInfo);
        }

        public static AppInfo GetAppInfos(string name, string herokuAuthToken)
        {
            AppInfo appInfo = default(AppInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}", name);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppInfo>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appInfo;
        }
        public static async Task<AppInfo> GetAppInfo(string name, string herokuAuthToken)
        {
            AppInfo appInfo = default(AppInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}", name);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppInfo>(httpResponse);
                }
            }
            catch
            {
                throw;
            }

            return appInfo;
        }

        public static string GetHerokuAppName(string name, string herokuAuthToken)
        {
            string appName = string.Empty;

            try
            {
                var appInfo = GetAppInfo(name, herokuAuthToken);
                //if (!appInfo.IsNull())
                //{
                //    appName = appInfo.name;
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appName;
        }

        public static string GetHerokuAppRegion(string name, string herokuAuthToken)
        {
            string appName = string.Empty;

            try
            {
                IDictionary<string, string> appInfo = GetAppDetails(name, herokuAuthToken);
                if (appInfo != null && appInfo.ContainsKey("name"))
                {
                    appInfo.TryGetValue("name", out appName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appName;
        }

        public static void PostlogMessage(string message, string providerid)
        {
            //Task.Run(() =>{
            try
            {
                if (string.IsNullOrEmpty(providerid))
                    return;

                IDictionary<string, string> laddonInfo = GetAddonInfo(providerid);
                if (laddonInfo != null && laddonInfo.ContainsKey("log_input_url") && laddonInfo.ContainsKey("logplex_token") && laddonInfo.ContainsKey("name"))
                {
                    WebRequest myReq = WebRequest.Create(laddonInfo["log_input_url"]);
                    myReq.Method = "POST";
                    myReq.ContentType = "application/logplex-1";
                    myReq.Headers["Logplex-Msg-Count"] = "1";
                    myReq.Headers["Authorization"] = "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(string.Format("token:{0}", laddonInfo["logplex_token"])));
                    UTF8Encoding enc = new UTF8Encoding();
                    string data = LogplexLogFormat(laddonInfo["logplex_token"], laddonInfo["name"], laddonInfo["name"], message);
                    Stream dataStream = myReq.GetRequestStreamAsync().Result;
                    dataStream.Write(enc.GetBytes(data), 0, data.Length);

                    WebResponse wr = myReq.GetResponseAsync().Result;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }
            //});
        }

        public static IDictionary<string, string> GetAddonInfo(string resourceId)
        {
            IDictionary<string, string> addonInfo = null;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("vendor/apps/{0}", resourceId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(string.Format("{0}:{1}", ConfigVars.Instance.herokuAddonId, ConfigVars.Instance.herokuPassword))));
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    addonInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return addonInfo;
        }

        public static IDictionary<string, string> GetAddonInfo(string resourceId, string herokuAuthToken)
        {
            IDictionary<string, string> addonInfo = null;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("vendor/apps/{0}", resourceId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(string.Format("{0}:{1}", ConfigVars.Instance.herokuAddonId, ConfigVars.Instance.herokuPassword))));
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    addonInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return addonInfo;
        }

        public static dynamic GetAddonInfo(string appName, string herokuAuthToken, string addonName)
        {
            HttpStatusCode httpStatusCode = HttpStatusCode.Unauthorized;
            HerokuAddon addonInfo = default(HerokuAddon);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/addons", appName);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null)
                {
                    if (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created)
                    {
                        var addons = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<List<HerokuAddon>>(httpResponse);
                        if (addons != null && addons.Where(p => p.name.ToLower().Contains(ConfigVars.Instance.herokuAddonId.ToLower())).Count() > 0)
                        {
                            httpStatusCode = HttpStatusCode.Found;
                            addonInfo = addons.Where(p => p.name.ToLower().Contains(ConfigVars.Instance.herokuAddonId.ToLower())).FirstOrDefault();
                        }
                    }
                    else
                    {
                        httpStatusCode = httpResponse.StatusCode;
                    }
                }
                else
                {
                    httpStatusCode = HttpStatusCode.NotFound;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return new { HttpStatusCode = (int)httpStatusCode, Addon = addonInfo };
        }

        public static string LogplexLogFormat(string token, string hostName, string appName, string message)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("<190>1 ");
            sb.Append(string.Format("{0} ", DateTime.Now.ToString("yyyy-MM-ddTHH:MM:ss+00:00")));
            sb.Append(string.Format("{0} ", hostName));
            sb.Append(string.Format("{0} ", token));
            sb.Append(string.Format("{0} ", appName));
            //sb.Append(string.Format("{0} web ", appName));
            sb.Append("- ");
            sb.Append("- ");
            sb.Append(string.Format(" {0}", message));

            return string.Format("{0} {1}", sb.Length, sb.ToString());
        }

        public static string GetToken()
        {
            string tokenUrl = string.Empty;
            try
            {
                mFactory = new RequestFactory();
                var request = mFactory.CreateRequest();
                request.Resource = "/oauth/authorize";

                request.AddObject(new
                {
                    response_type = "code",
                    client_id = ConfigVars.Instance.ClientId
                });

                var client = mFactory.CreateClient();
                client.BaseUrl = ConfigVars.Instance.herokuAuthServerBaseUrl;
                tokenUrl = client.BuildUri(request).ToString();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }
            return tokenUrl;
        }

        public static async Task<HerokuAuthToken> GetHerokuAccessToken(string grantCode, AuthGrantType grantType)
        {
            HerokuAuthToken authToken = default(HerokuAuthToken);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "/oauth/token";
                request.Method = Method.POST;
                switch (grantType)
                {
                    case AuthGrantType.authorization_code:
                        request.AddObject(new
                        {
                            code = grantCode,
                            client_id = ConfigVars.Instance.ClientId,
                            client_secret = ConfigVars.Instance.ClientSecret,
                            grant_type = "authorization_code"
                        });
                        break;
                    case AuthGrantType.refresh_token:
                        request.AddObject(new
                        {
                            refresh_token = grantCode,
                            client_id = ConfigVars.Instance.ClientId,
                            client_secret = ConfigVars.Instance.ClientSecret,
                            grant_type = "refresh_token"
                        });
                        break;
                }

                var client = mFactory.CreateClient();
                client.BaseUrl = ConfigVars.Instance.herokuAuthServerBaseUrl;
                var httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    authToken = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<HerokuAuthToken>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return authToken;
        }

        public static async Task<HerokuAuthToken> GetAddonAccessToken(string grantCode, string grantType)
        {
            HerokuAuthToken authToken = default(HerokuAuthToken);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "/oauth/token";
                request.Method = Method.POST;
                request.AddObject(new
                {
                    code = grantCode,
                    client_secret = ConfigVars.Instance.addonClientSecret,
                    grant_type = grantType
                });

                var client = mFactory.CreateClient();
                client.BaseUrl = ConfigVars.Instance.herokuAuthServerBaseUrl;
                var httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    authToken = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<HerokuAuthToken>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return authToken;
        }

        public static HerokuAuthToken GetAddonAccessTokenSync(string grantCode, AuthGrantType grantType, AuthScope authScope = AuthScope.global)
        {
            HerokuAuthToken authToken = default(HerokuAuthToken);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "/oauth/token";
                request.Method = Method.POST;
                switch (grantType)
                {
                    case AuthGrantType.authorization_code:
                        request.AddObject(new
                        {
                            code = grantCode,
                            client_secret = ConfigVars.Instance.addonClientSecret,
                            grant_type = grantType.GetAuthGrantTypeDescription()
                            //, scope = authScope.GetAuthScopeDescription()
                        });
                        break;
                    case AuthGrantType.refresh_token:
                        request.AddObject(new
                        {
                            refresh_token = grantCode,
                            client_secret = ConfigVars.Instance.addonClientSecret,
                            grant_type = grantType.GetAuthGrantTypeDescription()
                        });
                        break;
                }

                IRestResponse httpResponse = null;
                var blocker = new AutoResetEvent(false);
                var client = mFactory.CreateClient();
                client.BaseUrl = ConfigVars.Instance.herokuAuthServerBaseUrl;
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    authToken = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<HerokuAuthToken>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return authToken;
        }

        public static async Task<HerokuAuthToken> GetAddonAccessToken(string grantCode, AuthGrantType grantType, AuthScope authScope = AuthScope.global)
        {
            HerokuAuthToken authToken = default(HerokuAuthToken);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "/oauth/token";
                request.Method = Method.POST;
                switch (grantType)
                {
                    case AuthGrantType.authorization_code:
                        request.AddObject(new
                        {
                            code = grantCode,
                            client_secret = ConfigVars.Instance.addonClientSecret,
                            grant_type = grantType.GetAuthGrantTypeDescription()
                            //, scope = authScope.GetAuthScopeDescription()
                        });
                        break;
                    case AuthGrantType.refresh_token:
                        request.AddObject(new
                        {
                            refresh_token = grantCode,
                            client_secret = ConfigVars.Instance.addonClientSecret,
                            grant_type = grantType.GetAuthGrantTypeDescription()
                        });
                        break;
                }

                var client = mFactory.CreateClient();
                client.BaseUrl = ConfigVars.Instance.herokuAuthServerBaseUrl;
                IRestResponse httpResponse = await client.RestExecuteAsync(request).ConfigureAwait(false);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    authToken = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<HerokuAuthToken>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return authToken;
        }
        public static async Task<OauthGrant> GetOauthGrant(string resourceId)
        {
            OauthGrant oauthGrant = default(OauthGrant);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("/vendor/resources/{0}/oauth-grant", resourceId);
                request.Method = Method.GET;
                request.AddHeader("Content-Type", "Content-Type");
                request.AddHeader("Authorization", "Basic " + Utilities.Base64Encode($"{ConfigVars.Instance.herokuAddonId}:{ConfigVars.Instance.herokuPassword}"));

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    oauthGrant = Newtonsoft.Json.Linq.JToken.Parse(httpResponse.Content).SelectToken("oauth_grant").ToObject<OauthGrant>();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return oauthGrant;
        }

        public static async Task AddUpdatePrivateAppConfigvars(string appName, string appId, string herokuAuthToken)
        {
            Console.WriteLine("Private App {0} config update starts", appId);
            try
            {
                var request = mFactory.CreateRequest();
                if (string.IsNullOrEmpty(appId))
                    request.Resource = $"apps/{appName}/config-vars";
                else
                    request.Resource = $"apps/{appId}/config-vars";
                request.Method = Method.PATCH;
                request.AddHeader("Authorization", $"Bearer {herokuAuthToken}");
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json; version=3");

                JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
                {
                    ContractResolver = new DefaultContractResolver()
                };

                var guidFromAppName = Utilities.ConvertToMd5HashGUID(appName);
                Console.WriteLine("Guid from app name: {0} => {1}", appName, guidFromAppName);
                var queueName = $"critical_{Math.Abs(guidFromAppName.ToInt())}".Trim();
                Console.WriteLine("Queue: {0} assigned for app: {1}", queueName, appName);
                if (queueName.Length > 20)
                {
                    queueName = queueName.Substring(0, 20);
                    Console.WriteLine("Shorten queue name: {0} assigned for app: {1}", queueName, appName);
                }

                string jsonConfigStr = string.Empty;
                var appConfigVars = new AppConfigvars()
                {
                    DEDUP_PROXY_APP = appName,
                    JOB_QUEUE_NAME = queueName
                };
                jsonConfigStr = JsonConvert.SerializeObject(appConfigVars, Formatting.Indented, jsonSerializerSettings);
                Console.WriteLine("Config: {0}", jsonConfigStr);
                request.AddParameter("application/json", jsonConfigStr, ParameterType.RequestBody);

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("AppConfig-Update-Status: {0}", httpResponse.Content);
                if (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted)
                {
                    Console.WriteLine("Addon config var(s) updated");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            Console.WriteLine("Private App {0} config update ended", appId);
        }
        public static async Task<DynoInfo> GetDynoInfo(string appNameOrId, string herokuAuthToken)
        {
            DynoInfo dynoInfo = default(DynoInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/dynos ", appNameOrId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    var dynos = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<List<DynoInfo>>(httpResponse);
                    if (dynos != null)
                    {
                        dynoInfo = dynos.FirstOrDefault();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return dynoInfo;
        }

        public static string GetHerokuAppLogUrl(bool isconnectorlog, string appName, string herokuAuthToken, bool isTail = false)
        {
            IDictionary<string, string> appLogInfo = null;
            string appLogs = string.Empty;
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/log-sessions", appName);
                request.Method = Method.POST;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                request.RequestFormat = DataFormat.Json;
                if (isconnectorlog)
                {
                    var reqBody = new
                    {
                        dyno = "web",
                        lines = 500,
                        source = "app",
                        tail = isTail
                    };
                    request.AddBody(reqBody);
                }
                //request.AddBody(JsonConvert.SerializeObject(reqBody));
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appLogInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                    if (appLogInfo != null && appLogInfo.Count > 0 && !string.IsNullOrEmpty(appLogInfo["logplex_url"]))
                    {
                        appLogs = appLogInfo["logplex_url"];
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appLogs;
        }

        public static string GetHerokuAppLogs(string log_plex_url, string appName)
        {
            string appLogs = string.Empty;
            try
            {
                if (!string.IsNullOrEmpty(log_plex_url))
                {
                    var requestlog = mFactory.CreateRequest();
                    requestlog.Method = Method.GET;
                    var blockerlog = new AutoResetEvent(false);

                    IRestResponse httpResponselog = null;
                    var clientlog = mFactory.CreateClient();
                    clientlog.BaseUrl = new Uri(log_plex_url);
                    clientlog.ExecuteAsync(requestlog, response =>
                    {
                        httpResponselog = response;
                        blockerlog.Set();
                    });
                    blockerlog.WaitOne();
                    if (httpResponselog != null && (httpResponselog.StatusCode == HttpStatusCode.OK || httpResponselog.StatusCode == HttpStatusCode.Created))
                    {
                        var logs = Regex.Split(httpResponselog.Content.ToString(), "\n", RegexOptions.IgnoreCase);
                        if (logs.Where(p => p.Contains("app[" + appName + "]")).Count() > 0)
                        {
                            appLogs = string.Join("\n", logs.Where(p => p.Contains("app[" + appName + "]")).ToArray());
                            appLogs = Regex.Replace(appLogs, @"\r\n?|\n", "<br>");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appLogs;
        }

        public static async Task<string> GetHerokuAppConfigVarByName(string appName, string ConfigvarName, string herokuAuthToken)
        {
            string configVal = string.Empty;
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/config-vars", appName);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("App config-vars: {0}", httpResponse.Content);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    var configVars = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                    if (configVars != null)
                    {
                        configVars.TryGetValue(ConfigvarName, out configVal);
                    }
                }
            }
            catch
            {
                throw;
            }

            return configVal;
        }

        //public static string GetHerokuAppConfigVarByName(string appName, string configVar, string herokuAuthToken)
        //{
        //    string configVal = string.Empty;
        //    try
        //    {
        //        IDictionary<string, string> configVars =await GetHerokuAppConfigVars(appName, herokuAuthToken);
        //        if (configVars != null)
        //            configVars.TryGetValue(configVar, out configVal);
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("Error:{0}", ex.Message);
        //        throw;
        //    }
        //    return configVal;
        //}

        public static List<AddonInfo> GetHerokuAppAddons(string appName, string herokuAuthToken)
        {
            List<AddonInfo> addons = new List<AddonInfo>();
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/addons", appName);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                Console.WriteLine("App addons: {0}", httpResponse.Content);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    addons = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<List<AddonInfo>>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return addons;
        }

        public static List<AppInfo> GetAppsInAccount(string herokuAuthToken)
        {
            List<AppInfo> appInfos = default(List<AppInfo>);
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "apps";
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var blocker = new AutoResetEvent(false);

                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK))
                {
                    appInfos = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<List<AppInfo>>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }
            return appInfos;
        }

        public static AppInfo CreatAppInPrivateSpace(string regionName, string stackName, string appName, string spaceName, string herokuAuthToken)
        {
            AppInfo appInfo = default(AppInfo);
            try
            {
                var appLst = GetAppsInAccount(herokuAuthToken);
                if (appLst != null && appLst.FirstOrDefault(p => p.name.ToLower().Trim() == string.Format("{0}-{1}", spaceName, appName)).IsNull() == false)
                    appInfo = appLst.FirstOrDefault(p => p.name.ToLower().Trim() == string.Format("{0}-{1}", spaceName, appName));
                if (appInfo.IsNull())
                {
                    var request = mFactory.CreateRequest();
                    request.Resource = "apps";
                    request.Method = Method.POST;
                    request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                    request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                    request.AddObject(new
                    {
                        name = string.Format("{0}-{1}", spaceName, appName),
                        region = regionName,
                        stack = stackName
                    });
                    var blocker = new AutoResetEvent(false);

                    IRestResponse httpResponse = null;
                    var client = mFactory.CreateClient();
                    client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                    client.ExecuteAsync(request, response =>
                    {
                        httpResponse = response;
                        blocker.Set();
                    });
                    blocker.WaitOne();

                    if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.Created))
                    {
                        appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppInfo>(httpResponse);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appInfo;
        }

        public static bool CheckDeDupAppExistOrNot(string appName, string spaceName, string herokuAuthToken)
        {
            var appLst = GetAppsInAccount(herokuAuthToken);
            if (appLst != null && appLst.FirstOrDefault(p => p.name.ToLower().Trim() == string.Format("{0}-{1}", spaceName, appName)).IsNull())
                return false;
            else
                return true;
        }


        public static Task<AppInfo> PublishDeDupAppInPrivateSpace(string resourceId, string regionName, string organizationName, string appName, string spaceName, string herokuAuthToken, ref AppSetupInfo appSetupInfo, out string errorMessage)//, bool isODBC)
        {
            AppInfo appInfo = default(AppInfo);
            errorMessage = string.Empty;
            try
            {
                appInfo = GetAppInfo(string.Format("{0}-{1}", spaceName, appName), herokuAuthToken).Result;
                if (appInfo.IsNull())
                {
                    var request = mFactory.CreateRequest();
                    request.Resource = "app-setups";
                    request.Method = Method.POST;
                    request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                    request.AddHeader("Content-Type", "application/json");
                    request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                    var appSetups = new AppSetups();
                    appSetups.source_blob = new AppSourceBlob()
                    {
                        url = ConfigVars.Instance.deDupBuildGitRepoUrl,
                        version = ConfigVars.Instance.deDupPvtAppVersion
                    };
                    appSetups.app = new AppSetupAppInfo()
                    {
                        locked = false,
                        name = string.Format("{0}-{1}", spaceName, appName),
                        organization = organizationName,
                        personal = false,
                        region = regionName,
                        space = spaceName,
                        stack = ConfigVars.Instance.dedupPvtAppStackVersion
                    };

                    //create queue from heroku app name
                    var guidFromAppName = Utilities.ConvertToMd5HashGUID(appSetups.app.Value.name);
                    Console.WriteLine("Guid from app name: {0} => {1}", appSetups.app.Value.name, guidFromAppName);
                    var queueName = $"critical_{Math.Abs(guidFromAppName.ToInt())}".Trim();
                    Console.WriteLine("Queue: {0} assigned for app: {1}", queueName, appSetups.app.Value.name);
                    if (queueName.Length > 20)
                    {
                        queueName = queueName.Substring(0, 20);
                        Console.WriteLine("Shorten queue name: {0} assigned for app: {1}", queueName, appSetups.app.Value.name);
                    }
                    //if (isODBC)
                    //{
                    //    appSetups.overrides = new AppSetupOverrides()
                    //    {
                    //        buildpacks = new List<AppBuildpack>(),
                    //        env = new AppConfigvars()
                    //        {
                    //            MC_PROXY_APP = appSetups.app.Value.name,
                    //            JOB_QUEUE_NAME = queueName,
                    //            AMAZONREDSHIFTODBCINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.amazon_redshift_odbc_ini_path)) ? ConfigVars.Instance.amazon_redshift_odbc_ini_path : null,
                    //            LD_LIBRARY_PATH = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.ld_library_path)) ? ConfigVars.Instance.ld_library_path : null,
                    //            ODBCINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbc_ini_path)) ? ConfigVars.Instance.odbc_ini_path : null,
                    //            ODBCSYSINI = (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbc_sys_ini_path)) ? ConfigVars.Instance.odbc_sys_ini_path : null,
                    //        }
                    //    };
                    //}
                    //else
                    {
                        appSetups.overrides = new AppSetupOverrides()
                        {
                            buildpacks = new List<AppBuildpack>(),
                            env = new AppConfigvars()
                            {
                                DEDUP_PROXY_APP = appSetups.app.Value.name,
                                JOB_QUEUE_NAME = queueName,
                            }
                        };
                    }
                    //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbcBuildpackUrl))
                    //{
                    //    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    //    {
                    //        url = ConfigVars.Instance.odbcBuildpackUrl
                    //    });
                    //}
                    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    {
                        url = ConfigVars.Instance.deDupBuildpackUrl
                    });
                    //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.redshiftBuildpackUrl))
                    //{
                    //    appSetups.overrides?.buildpacks.Add(new AppBuildpack
                    //    {
                    //        url = ConfigVars.Instance.redshiftBuildpackUrl
                    //    });
                    //}
                    JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
                    {
                        ContractResolver = new DefaultContractResolver()
                    };
                    string jsonObject = JsonConvert.SerializeObject(appSetups, Formatting.Indented, jsonSerializerSettings);
                    Console.WriteLine("Appsetup: {0}", jsonObject);
                    request.AddParameter("application/json", jsonObject, ParameterType.RequestBody);

                    var client = mFactory.CreateHerokuRestClient();
                    IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                    Console.WriteLine("Appsetup-Build: {0}", httpResponse.Content);
                    if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted))
                    {
                        errorMessage = "succeeded";
                        appSetupInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppSetupInfo>(httpResponse);
                        Console.WriteLine("Appsetup response received");
                    }
                    else
                    {
                        errorMessage = "failed";
                    }
                }
                else
                {
                    errorMessage = "succeeded";
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                errorMessage = ex.Message;
            }

            return Task.FromResult(appInfo);
        }

        public static void GetPublishedAppStatusInPrivateSpace(string buildId, string herokuAuthToken, out string buildStatus, out string failureMessage, out string successUrl)
        {
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("app-setups/{0}", buildId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted))
                {
                    var buildStat = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                    Console.WriteLine("Appsetup-Status-Response: {0}", buildStat);
                    if (buildStat != null && buildStat.Count() > 0)
                    {
                        buildStat.TryGetValue("status", out buildStatus);
                        buildStat.TryGetValue("failure_message", out failureMessage);
                        buildStat.TryGetValue("resolved_success_url", out successUrl);
                    }
                    else
                    {
                        buildStatus = null;
                        failureMessage = null;
                        successUrl = null;
                    }
                }
                else
                {
                    buildStatus = null;
                    failureMessage = null;
                    successUrl = null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                buildStatus = null;
                failureMessage = null;
                successUrl = null;
            }
        }

        public static async Task AddUpdateMainAppConfigByResourceId(string resourceId, string herokuAuthToken)
        {
            Console.WriteLine("Addon-id {0} config update starts", resourceId);
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = $"addons/{resourceId}/config";
                request.Method = Method.PATCH;
                request.AddHeader("Authorization", $"Bearer {herokuAuthToken}");
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json; version=3");

                var configVar = new
                {
                    config = new[] {
                        new { DEDUP_URL = string.Format("{0}/sso/auth/{1}", ConfigVars.Instance.deDupWebUrl, Utilities.EncryptText(resourceId).Replace('+', '-').Replace('/', '_')) }
                    }
                };
                string jsonObject = JsonConvert.SerializeObject(configVar, Formatting.Indented);
                Console.WriteLine("AppConfigVar: {0}", jsonObject);
                request.AddParameter("application/json", jsonObject, ParameterType.RequestBody);

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("AppConfig-Update-Status: {0}", httpResponse.Content);
                if (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted)
                {
                    Console.WriteLine("Addon config var(s) updated");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            Console.WriteLine("Addon config update ended");
        }

        public static VendorAppInfo GetVendorAppInfoByResourceId(string resourceId)
        {
            VendorAppInfo appInfo = default(VendorAppInfo);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("vendor/apps/{0}", resourceId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Basic " + Convert.ToBase64String(Encoding.ASCII.GetBytes(string.Format("{0}:{1}", ConfigVars.Instance.herokuAddonId, ConfigVars.Instance.herokuPassword))));
                request.AddHeader("Content-Type", "application/json");

                var blocker = new AutoResetEvent(false);
                IRestResponse httpResponse = null;
                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                client.ExecuteAsync(request, response =>
                {
                    httpResponse = response;
                    blocker.Set();
                });
                blocker.WaitOne();

                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<VendorAppInfo>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
                throw;
            }

            return appInfo;
        }

        public static async Task UpdateVendorAppConfigVarByResourceId(string resourceId, string herokuAuthToken)
        {
            Console.WriteLine("App config var update starts");
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("vendor/apps/{0}", resourceId);
                request.Method = Method.PUT;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Content-Type", "application/json");

                var configVar = new { config = new { DEDUP_URL = string.Format("{0}/sso/auth/{1}", ConfigVars.Instance.deDupWebUrl, resourceId) } };
                string jsonObject = JsonConvert.SerializeObject(configVar, Formatting.Indented);
                Console.WriteLine("AppConfigVar: {0}", jsonObject);
                request.AddParameter("application/json", jsonObject, ParameterType.RequestBody);

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("AppConfig-Update-Status: {0}", httpResponse.Content);
                if (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted)
                {
                    Console.WriteLine("App config var updated");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }
            Console.WriteLine("App config var update ended");
        }

        public static HerokuAppConfig GetHerokuAppConfig(string resourceId, string herokuAuthToken)
        {
            HerokuAppConfig appInfo = default(HerokuAppConfig);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "api/herokuapp/getappconfig";
                request.Method = Method.POST;
                request.AddHeader("Content-Type", "application/json");
                request.AddJsonBody(new AppViewModel()
                {
                    resourceId = resourceId,
                    herokuAuthToken = herokuAuthToken,
                });

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.deDupWebUrl);
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    appInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<HerokuAppConfig>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appInfo;
        }

        public static string GetHerokuAppEnvVarByName(string resourceId, string herokuAuthToken, string envVarName)
        {
            string envValue = string.Empty;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = "api/herokuapp/getconfigvar";
                request.Method = Method.POST;
                request.AddHeader("Content-Type", "application/json");
                request.AddJsonBody(new AppViewModel()
                {
                    resourceId = resourceId,
                    herokuAuthToken = herokuAuthToken,
                    envVarName = envVarName
                });

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.deDupWebUrl);
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                Console.WriteLine("App config-vars: {0}", httpResponse.Content);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    var configVars = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                    if (configVars != null && configVars.ContainsKey("value"))
                    {
                        configVars.TryGetValue("value", out envValue);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return envValue;
        }

        public static async Task<AppBuild> GetAppLatestBuild(string appId, string herokuAuthToken)
        {
            AppBuild appBuild = default(AppBuild);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/builds", appId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");
                request.AddHeader("Range", "created_at ..;order=desc,max=1;");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted
                    || httpResponse.StatusCode == HttpStatusCode.PartialContent))
                {
                    appBuild = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<List<AppBuild>>(httpResponse).FirstOrDefault();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appBuild;
        }

        public static void UpdateHerokuappStackVersion(string appId, string herokuAuthToken, string version)
        {
            Console.WriteLine("Stack Version update starts");
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = $"apps/{appId}";
                request.Method = Method.PATCH;
                request.AddHeader("Authorization", $"Bearer {herokuAuthToken}");
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json; version=3");

                JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
                {
                    ContractResolver = new DefaultContractResolver()
                };

                string jsonConfigStr = string.Empty;
                var stackVersionConfig = new
                {
                    build_stack = version
                    //maintenance = false,
                    //name = appName

                };
                jsonConfigStr = JsonConvert.SerializeObject(stackVersionConfig, Formatting.Indented, jsonSerializerSettings);
                Console.WriteLine("stackVersionConfig: {0}", jsonConfigStr);
                request.AddParameter("application/json", jsonConfigStr, ParameterType.RequestBody);

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = client.RestExecuteAsync(request).Result;
                Console.WriteLine("Stack versoin-Update-Status: {0}", httpResponse.Content);
                if (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted)
                {
                    Console.WriteLine("stack version updated");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }
            Console.WriteLine("Stack Version update ended");
        }

        public static async Task<Dictionary<string, string>> GetHerokuAppConfigVars(string appName, string herokuAuthToken)
        {
            Dictionary<string, string> configVars = null;
            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/config-vars", appName);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("App config-vars: {0}", httpResponse.Content);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    configVars = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<Dictionary<string, string>>(httpResponse);
                }
            }
            catch
            {
                throw;
            }

            return configVars;
        }

        public static async Task<AppBuild> CreateAppBuild(string appId, string herokuAuthToken, string appVersion)//, bool isODBC = false)
        {
            AppBuild appBuild = default(AppBuild);

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("apps/{0}/builds", appId);
                request.Method = Method.POST;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                CreateAppBuild createAppBuild = new CreateAppBuild();
                createAppBuild.source_blob = new AppSourceBlob()
                {
                    url = ConfigVars.Instance.deDupBuildGitRepoUrl,
                    version = appVersion
                };
                createAppBuild.buildpacks = new List<AppBuildpack>();
                //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.odbcBuildpackUrl))
                //{
                //    createAppBuild.buildpacks.Add(new AppBuildpack
                //    {
                //        url = ConfigVars.Instance.odbcBuildpackUrl
                //    });
                //}
                createAppBuild.buildpacks.Add(new AppBuildpack
                {
                    url = ConfigVars.Instance.deDupBuildpackUrl
                });
                //if (isODBC && !string.IsNullOrEmpty(ConfigVars.Instance.redshiftBuildpackUrl))
                //{
                //    createAppBuild.buildpacks.Add(new AppBuildpack
                //    {
                //        url = ConfigVars.Instance.redshiftBuildpackUrl
                //    });
                //}
                request.AddJsonBody(createAppBuild);

                var client = mFactory.CreateHerokuRestClient();
                IRestResponse httpResponse = await client.RestExecuteAsync(request);
                Console.WriteLine("App-Build: {0}", httpResponse.Content);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created || httpResponse.StatusCode == HttpStatusCode.Accepted))
                {
                    appBuild = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AppBuild>(httpResponse);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appBuild;
        }


        private static string ParseTokenResponse(string content, string key)
        {
            if (String.IsNullOrEmpty(content) || String.IsNullOrEmpty(key))
                return null;

            try
            {
                // response can be sent in JSON format
                var token = JObject.Parse(content).SelectToken(key);
                return token != null ? token.ToString() : null;
            }
            catch (JsonReaderException)
            {
                // or it can be in "query string" format (param1=val1&param2=val2)
                var collection = HttpUtility.ParseQueryString(content);
                return collection[key];
            }
        }

        public static async Task<string> GetHerokuAppIdByAddonId(string addonId, string herokuAuthToken)
        {
            string appId = string.Empty;

            try
            {
                var request = mFactory.CreateRequest();
                request.Resource = string.Format("addons/{0}", addonId);
                request.Method = Method.GET;
                request.AddHeader("Authorization", "Bearer " + herokuAuthToken);
                request.AddHeader("Accept", "application/vnd.heroku+json;version=3");

                var client = mFactory.CreateClient();
                client.BaseUrl = new Uri(ConfigVars.Instance.HerokuApiUrl);
                IRestResponse httpResponse = await client.RestExecuteAsync(request).ConfigureAwait(false);
                if (httpResponse != null && (httpResponse.StatusCode == HttpStatusCode.OK || httpResponse.StatusCode == HttpStatusCode.Created))
                {
                    var addonInfo = (new RestSharp.Deserializers.JsonDeserializer()).Deserialize<AddonInfo>(httpResponse);
                    if (!addonInfo.IsNull() && !addonInfo.app.IsNull())
                    {
                        appId = addonInfo.app.id;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error:{0}", ex.Message);
            }

            return appId;
        }
    }
}
