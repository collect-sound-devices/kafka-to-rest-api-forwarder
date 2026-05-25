using NLog;
using NLog.Extensions.Logging;
using KafkaToRestApiForwarder;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureLogging((context, logging) =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.Trace);

        LogManager.Setup()
            .LoadConfigurationFromSection(context.Configuration.GetSection("NLog"));

        logging.AddNLog();
    })
    .ConfigureServices((context, services) =>
    {
        var config = context.Configuration;

        services.Configure<KafkaServerSettings>(config.GetSection("Kafka:Service"));
        services.Configure<KafkaMessageDeliverySettings>(config.GetSection("Kafka:MessageDelivery"));
        services.Configure<ApiBaseUrlSettings>(config.GetSection("ApiBaseUrl"));
        services.Configure<GitHubCodespaceSettings>(config.GetSection("GitHubCodespace"));

        services.AddSingleton<CryptService>();
        services.AddHttpClient();
        services.AddSingleton<GitHubCodespaceAwaker>();
    });

await builder.Build().RunAsync();
