using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Dedup.Common;
using Dedup.Data;
using Dedup.HangfireFilters;
using Dedup.Repositories;
using Hangfire;
using Hangfire.PostgreSql;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json.Serialization;
using Pioneer.Pagination;
using Microsoft.AspNetCore.ResponseCompression;

namespace Dedup
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
			//telemetry off
            Environment.SetEnvironmentVariable("DOTNET_CLI_TELEMETRY_OPTOUT", "1");
			//gc allow large objects
			Environment.SetEnvironmentVariable("COMPlus_gcAllowVeryLargeObjects", "1");
			
            //db setting
            services.AddDbContext<DeDupContext>(options => options.UseNpgsql(ConfigVars.Instance.connectionString, b => b.MigrationsAssembly("Dedup")));
            services.Configure<CookiePolicyOptions>(options =>
            {
                // This lambda determines whether user consent for non-essential cookies is needed for a given request.
                options.CheckConsentNeeded = context => true;
                options.MinimumSameSitePolicy = SameSiteMode.None;
            });
            //set form options
            services.Configure<FormOptions>(options =>
            {
                options.ValueCountLimit = 2048; //default 1024
                options.ValueLengthLimit = int.MaxValue; //not recommended value
                options.MultipartBodyLengthLimit = long.MaxValue; //not recommended value
            });
            services.AddAuthentication(options =>
            {
                options.DefaultScheme = Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME;
                options.DefaultSignInScheme = Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME;
                options.DefaultSignOutScheme = Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME;
            })
           .AddCookie(Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_SCHEME, options =>
           {
               options.Cookie.Name = Dedup.Common.Constants.DEFAULT_AUTH_COOKIE_NAME;
               options.Cookie.HttpOnly = true;
               options.LoginPath = new PathString("/login/herokuauth/");
               options.AccessDeniedPath = new PathString("/home/unauthorized/");
               options.SlidingExpiration = true;
               options.ExpireTimeSpan = TimeSpan.FromDays(30);
           });
		   
            //set hangfire settings
            services.AddHangfire(configuration =>
            {
                //configuration.UseStorage(new PostgreSqlStorage(ConfigVars.Instance.hangfireConnectionString));
            });
            services.AddCors();
            services.AddResponseCompression(options =>
            {
                options.EnableForHttps = true;
                options.MimeTypes = ResponseCompressionDefaults.MimeTypes.Concat(new[] { "image/svg+xml", "application/atom+xml" });
                options.Providers.Add<BrotliCompressionProvider>();
                options.Providers.Add<GzipCompressionProvider>();
            });
            services.AddControllersWithViews().AddNewtonsoftJson(options =>
            {
                //Use the default
                options.SerializerSettings.ContractResolver = new DefaultContractResolver();
            }).SetCompatibilityVersion(CompatibilityVersion.Version_3_0);
            services.AddDistributedMemoryCache();
            services.AddSession(opts =>
            {
                opts.IdleTimeout = TimeSpan.FromHours(5);
            });

            //Register dependency injections
            services.AddSingleton<IActionContextAccessor, ActionContextAccessor>();
            services.AddScoped<IPaginatedMetaService, PaginatedMetaService>();
            services.AddScoped<IRequestFactory, RequestFactory>();
            services.AddTransient<IResourcesRepository, ResourcesRepository>();
            services.AddTransient<IDeDupSettingsRepository, DeDupSettingsRepository>();
            services.AddTransient<IConnectorsRepository, ConnectorsRepository>();
            services.AddTransient<IAuthTokenRepository, AuthTokenRepository>();
            services.AddTransient<IPartnerAuthTokenRepository, PartnerAuthTokenRepository>();
            services.AddTransient<ISyncRepository, SyncRepository>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, IServiceProvider serviceProvider, IHostApplicationLifetime appLifetime)
        {
            Utilities.AppServiceProvider = serviceProvider;
            Utilities.HostingEnvironment = env;
			Console.WriteLine($"GCSettings.IsServerGC:{System.Runtime.GCSettings.IsServerGC}");
            Console.WriteLine($"GCSettings.LargeObjectHeapCompactionMode:{System.Runtime.GCSettings.LargeObjectHeapCompactionMode}");
            Console.WriteLine($"GCSettings.LatencyMode:{System.Runtime.GCSettings.LatencyMode}");
            Console.WriteLine($"GCMemoryInfo.HeapSizeBytes:{GC.GetGCMemoryInfo().HeapSizeBytes}");
            Console.WriteLine($"GCMemoryInfo.HighMemoryLoadThresholdBytes:{GC.GetGCMemoryInfo().HighMemoryLoadThresholdBytes}");
            Console.WriteLine($"GCMemoryInfo.TotalAvailableMemoryBytes:{GC.GetGCMemoryInfo().TotalAvailableMemoryBytes}");
            Console.WriteLine($"GCMemoryInfo.MemoryLoadBytes:{GC.GetGCMemoryInfo().MemoryLoadBytes}");
            Console.WriteLine($"GCMemoryInfo.FragmentedBytes:{GC.GetGCMemoryInfo().FragmentedBytes}");
			
			app.UseMiddleware<HttpFilters.GCMiddleware>();
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                var culture = CultureInfo.CreateSpecificCulture("en-US");
                var dateformat = new DateTimeFormatInfo
                {
                    ShortDatePattern = "MM/dd/yyyy",
                    LongDatePattern = "MM/dd/yyyy hh:mm:ss tt"
                };
                culture.DateTimeFormat = dateformat;
                CultureInfo.DefaultThreadCurrentUICulture = culture;
                CultureInfo.DefaultThreadCurrentCulture = culture;
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
                app.UseHsts();
                app.UseHttpsRedirection();
            }
            app.UseResponseCompression();
            app.UseStaticFiles();
            app.UseRouting();
            // global cors policy
            app.UseCors(x => x
                .AllowAnyMethod()
                .AllowAnyHeader()
                .SetIsOriginAllowed(origin => true) // allow any origin
                .AllowCredentials()); // allow credentials
            app.UseAuthentication();
            app.UseAuthorization();
            app.UseSession();
            app.UseCookiePolicy();

            //hangfire settings
            Hangfire.Logging.LogProvider.SetCurrentLogProvider(null);
            var pgoptions = new PostgreSqlStorageOptions
            {
                InvisibilityTimeout = TimeSpan.FromHours(20) // default value
            };
            pgoptions.SchemaName = ConfigVars.Instance.herokuAddonAppName;
            GlobalConfiguration.Configuration.UsePostgreSqlStorage(ConfigVars.Instance.hangfireConnectionString, pgoptions);
            //set hangfire dashboard url
            app.UseHangfireDashboard("/scheduledjobs", new DashboardOptions()
            {
                Authorization = new[] { new HangFireAuthorizationFilter() },
                DisplayStorageConnectionString = false
            });
            Console.WriteLine(string.Format("Environment:{0} - ProcessorCount:{1}", (env.IsDevelopment() ? "Development" : "Production"), Environment.ProcessorCount));
            //WorkerCount = Math.Min(Environment.ProcessorCount * 5, 20)
            var serverOptions = new BackgroundJobServerOptions()
            {
                WorkerCount = ConfigVars.Instance.deDup_workerCount,
                Activator = null,
                FilterProvider = null,
                ServerTimeout = TimeSpan.FromMinutes(1),
                ServerCheckInterval = TimeSpan.FromMinutes(1),
                SchedulePollingInterval = TimeSpan.FromMinutes(1),
                TimeZoneResolver = app.ApplicationServices.GetService<ITimeZoneResolver>(),
                ServerName = $"{ConfigVars.Instance.herokuAddonAppName}_{Math.Abs(Guid.NewGuid().ToInt())}",
                Queues = new[] { Constants.JOB_QUEUE_NAME.Trim(), "default" }
            };
            Console.WriteLine("Production_Setup_Count:{0}", serverOptions.WorkerCount);
            //assign hangfire server options(background jobs)
            app.UseHangfireServer(serverOptions);

            try
            {
                //Remove Duplicte HangFire Server
                Console.WriteLine("Restarted sync after Heroku Dyno Restart, no action needed on your part");
                var servers = JobStorage.Current.GetMonitoringApi().Servers().Where(p => p.Name.Trim().StartsWith(ConfigVars.Instance.herokuAddonAppName.Trim(), StringComparison.OrdinalIgnoreCase)).OrderByDescending(p => p.Heartbeat).ToArray();
                if (servers != null && servers.Length > 0)
                {
                    using (var connection = JobStorage.Current.GetConnection())
                    {
                        for (int i = 0; i < servers.Length; ++i)
                        {
                            if (i == 0)
                            {
                                continue;
                            }
                            connection.RemoveServer(servers[i].Name);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: [0}", ex.Message);
            }
            app.Use(next => async context =>
            {
                var request = context.Request;
                if (request.IsHttps || // Handles https straight to the server 
                    request.Headers["X-Forwarded-Proto"] == request.Scheme || // Handles an IIS or Azure passthrough
                    request.Host.ToString().StartsWith("localhost", StringComparison.CurrentCultureIgnoreCase) || // Ignore for localhost
                    request.Headers["X-Forwarded-Proto"].ToString().Contains(request.Scheme)) // X-Forwarded-Proto can have multiple values if there are multiple proxies 
                {
                    await next(context);
                }
                else
                {
                    //context.Features.Get<IHttpMaxRequestBodySizeFeature>().MaxRequestBodySize = null;
                    string newUrl = $"https://{request.Host}{request.Path}{request.QueryString}";
                    context.Response.Redirect(newUrl, true);
                }
            });
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
            });

            ////Only one local or production
            //try
            //{
            //    using (var serviceScope = app.ApplicationServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
            //    {
            //        serviceScope.ServiceProvider.GetService<DeDupContext>().Database.Migrate();
            //    }
            //}
            //catch (Exception e)
            //{
            //    var msg = e.Message;
            //    var stacktrace = e.StackTrace;
            //}

            try
            {
                //schedule dedup process if they were not scheduled
              // Dedup.Services.JobScheduler.Instance.ResetScheduledJobs();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message, ex);
            }
        }
    }
}
