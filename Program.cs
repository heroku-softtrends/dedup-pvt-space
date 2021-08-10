using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Dedup
{
    public class Program
    {
        public static void Main(string[] args)
        {
			//host app
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.ConfigureLogging((hostingContext, logging) =>
                     {
                         logging.ClearProviders();
                         logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                         logging.AddFilter("Microsoft", LogLevel.None)
                                .AddFilter("System", LogLevel.None)
                                .AddFilter("Hangfire", LogLevel.None)
                                .AddConsole(options => options.IncludeScopes = true);
                         logging.AddFilter("Microsoft", LogLevel.None)
                                .AddFilter("System", LogLevel.None)
                                .AddFilter("Hangfire", LogLevel.None)
                                .AddDebug();

                     }).UseStartup<Startup>();
                });
    }
}
