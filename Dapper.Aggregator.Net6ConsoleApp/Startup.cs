using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator.Net6ConsoleApp
{
    public class Startup
    {
        public IServiceProvider Ensure()
        {
            IServiceCollection services = new ServiceCollection();

            ConfigureServices(services);

            return services.BuildServiceProvider();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            System.Data.Common.DbProviderFactories.RegisterFactory("NpgSql", NpgsqlFactory.Instance);

            IConfiguration configuration = new ConfigurationBuilder()
                 .SetBasePath(Directory.GetCurrentDirectory())
                 .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                 .AddJsonFile($"appsettings.Development.json", optional: true)
                 .AddEnvironmentVariables()
                 .Build();
            services.AddSingleton(configuration);

            services.AddLogging();

            services.AddTransient<DbConnectionFactory>();


            var listener = new ActivityListener
            {
                ShouldListenTo = _ => true,
                SampleUsingParentId = (ref ActivityCreationOptions<string> activityOptions) => ActivitySamplingResult.AllData,
                Sample = (ref ActivityCreationOptions<ActivityContext> context) => ActivitySamplingResult.AllData,

                ActivityStarted = activity => Console.WriteLine($"{activity.ParentId}:{activity.Id} - Start"),
                ActivityStopped = activity => Console.WriteLine($"{activity.ParentId}:{activity.Id} - Stop "),


                //ActivityStarted = activity => Console.WriteLine($"{activity.ParentId}:{activity.Id} - Start - {Environment.NewLine}{String.Join(Environment.NewLine, activity.Tags.Select(x => $"{x.Key}:{x.Value}"))}"),
                //ActivityStopped = activity => Console.WriteLine($"{activity.ParentId}:{activity.Id} - Stop - {Environment.NewLine}{String.Join(Environment.NewLine, activity.Tags.Select(x => $"{x.Key}:{x.Value}"))}"),
            };
            ActivitySource.AddActivityListener(listener);
        }

    }
}
