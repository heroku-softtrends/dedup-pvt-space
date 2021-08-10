using System;
using System.Collections.Generic;
using Dedup.ViewModels;
using System.Linq;
using Microsoft.AspNetCore.Hosting;
using Dapper;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

namespace Dedup.Common
{
    public sealed class ConfigVars
    {
        public string ClientId = string.Empty;
        public string ClientSecret = string.Empty;
        public string HerokuApiUrl = string.Empty;
        public Uri herokuAuthServerBaseUrl = null;
        public string herokuPassword = string.Empty;
        public string herokuAddonId = string.Empty;
        public string herokuSalt = string.Empty;
        public string herokuAddonAppName = string.Empty;
        public string connectionString = string.Empty;
        public string hangfireConnectionString = string.Empty;
        public string deDupWebUrl = string.Empty;
        public bool deDupPvtEdition = false;
        public List<PlanInfos> addonPlans = null;
        public int DEDUP_PAGE_SIZE = 10;
        public double pgMaxQuerySizeInMB = 3.5;
        public int deDup_workerCount = 2;
        public int DEDUP_CTID_PAGE_SIZE = 50;
        public string addonClientSecret = string.Empty;
        public string PROVISION_ERR_MSG = string.Empty;
        public string DFT_PROVISION_MSG = string.Empty;

        public string deDupBuildGitRepoUrl = string.Empty;
        public string dedupPvtAppStackVersion = string.Empty;
        public string deDupBuildpackUrl = string.Empty;
        public string deDupPvtAppVersion = string.Empty;

        public List<string> addonPrivatePlanLevels = null;
        


        private ConfigVars()
        {
            if (Utilities.HostingEnvironment.IsProduction() || Utilities.HostingEnvironment.IsStaging())
            {
                connectionString = ConnectionFactory.GetPGConnectionStringFromUrl(Environment.GetEnvironmentVariable("DATABASE_URL"));
                hangfireConnectionString = ConnectionFactory.GetPGConnectionStringFromUrl(Environment.GetEnvironmentVariable("HEROKU_POSTGRESQL_JADE_URL"));
            }
            else if (Utilities.HostingEnvironment.IsDevelopment())
            {
                Environment.SetEnvironmentVariable("DATABASE_URL", "postgres://jzcwqchdiysgxq:60a1c2e22667b739e85c6bee37280ee69a5172d0e47d80ca1dcedf8055c5a835@ec2-52-0-67-144.compute-1.amazonaws.com:5432/d151gqddb2gq5i");
                Environment.SetEnvironmentVariable("HEROKU_POSTGRESQL_PUCE_URL", "postgres://cdarwuyikfcrbn:b739edf3fd53a60a6800a331de5970f944adb90f8f3887853d70398d3dd91781@ec2-3-227-44-84.compute-1.amazonaws.com:5432/d8t7i7ajuvgf7f");
                connectionString = ConnectionFactory.GetPGConnectionStringFromUrl(Environment.GetEnvironmentVariable("DATABASE_URL"));
                hangfireConnectionString = ConnectionFactory.GetPGConnectionStringFromUrl(Environment.GetEnvironmentVariable("HEROKU_POSTGRESQL_PUCE_URL"));

            }
            LoadDeDupConfigsByType();
        }

        public static ConfigVars Instance { get { return ConfigVarInstance.Instance; } }

        private class ConfigVarInstance
        {
            static ConfigVarInstance()
            {
            }
            internal static readonly ConfigVars Instance = new ConfigVars();
        }

        public void LoadDeDupConfigsByType(LoadConfigType type = LoadConfigType.ALL)
        {
            try
            {
                using (ConnectionFactory connectionFactory = new ConnectionFactory(connectionString))
                {
                    switch (type)
                    {
                        case LoadConfigType.PLAN:
                            addonPlans = connectionFactory.DbConnection.Query<PlanInfos>("SELECT * FROM \"dedup-settings\".\"planinfos\";").ToList();
                            break;
                        case LoadConfigType.CONFIGVARS:
                            var configvars = connectionFactory.DbConnection.Query<dynamic>("SELECT * FROM \"dedup-settings\".\"configvars\";").FirstOrDefault();
                            SetConfigvars(configvars);
                            break;
                        default:
                            var queryResult = connectionFactory.DbConnection.QueryMultiple("SELECT * FROM \"dedup-settings\".\"configvars\";SELECT * FROM \"dedup-settings\".\"planinfos\";");
                            if (queryResult != null)
                            {
                                SetConfigvars(queryResult.Read<dynamic>().FirstOrDefault());
                                addonPlans = queryResult.Read<PlanInfos>().ToList();
                            }
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                throw;
            }
        }

        public Task LoadDeDupConfigsByTypeAsync(LoadConfigType type = LoadConfigType.ALL)
        {
            try
            {
                using (ConnectionFactory connectionFactory = new ConnectionFactory(connectionString))
                {
                    switch (type)
                    {
                        case LoadConfigType.PLAN:
                            addonPlans = connectionFactory.DbConnection.Query<PlanInfos>("SELECT * FROM \"dedup-settings\".\"planinfos\";").ToList();
                            break;
                        case LoadConfigType.CONFIGVARS:
                            var configvars = connectionFactory.DbConnection.Query<dynamic>("SELECT * FROM \"dedup-settings\".\"configvars\";").FirstOrDefault();
                            SetConfigvars(configvars);
                            break;
                        default:
                            var queryResult = connectionFactory.DbConnection.QueryMultiple("SELECT * FROM \"dedup-settings\".\"configvars\";SELECT * FROM \"dedup-settings\".\"planinfos\";");
                            if (queryResult != null)
                            {
                                SetConfigvars(queryResult.Read<dynamic>().FirstOrDefault());
                                addonPlans = queryResult.Read<PlanInfos>().ToList();
                            }
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                throw;
            }
            return Task.CompletedTask;
        }

        private void SetConfigvars(dynamic configvars)
        {
            try
            {
                if (configvars != null)
                {
                    herokuAddonAppName = configvars.public_addon_name.Trim();
                    deDupWebUrl = configvars.public_addon_url.Trim();
                    herokuAddonId = configvars.heroku_user.Trim();
                    herokuPassword = configvars.heroku_password.Trim();
                    herokuSalt = configvars.heroku_salt.Trim();
                    HerokuApiUrl = configvars.heroku_api_url.Trim();
                    Uri.TryCreate(configvars.heroku_auth_url.Trim(), UriKind.Absolute, out herokuAuthServerBaseUrl);
                    addonClientSecret = configvars.addon_client_secret.Trim();
                    DFT_PROVISION_MSG = configvars.provision_success_message.Trim();
                    PROVISION_ERR_MSG = configvars.provision_error_message.Trim();
                    DEDUP_PAGE_SIZE = (int)configvars.dedup_page_size;
                    deDup_workerCount = (int)configvars.dedup_worker_count;
                    pgMaxQuerySizeInMB = (double)configvars.pg_max_query_size_in_mb;
                    DEDUP_CTID_PAGE_SIZE= (int)configvars.dedup_ctid_page_size;
                   // Int32.TryParse(configvars.dedup_ctid_page_size.Trim(),out DEDUP_CTID_PAGE_SIZE);
                    ClientId = configvars.heroku_clientid.Trim();
                    ClientSecret = configvars.heroku_clientsecret.Trim();
                    //deDupBuildGitRepoUrl = configvars.private_addon_git_repository.Trim();
                    //dedupPvtAppStackVersion = configvars.private_addon_heroku_stack.Trim();
                    //deDupBuildpackUrl = configvars.private_addon_buildpack.Trim();
                    //deDupPvtAppVersion = configvars.private_addon_version.Trim();
                }
            }
            catch
            {
                throw;
            }
        }
    }
}
