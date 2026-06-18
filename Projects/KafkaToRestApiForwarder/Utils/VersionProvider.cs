using System.Reflection;
using System.Runtime.InteropServices;

namespace KafkaToRestApiForwarder.Utils;

internal class VersionProvider : IVersionProvider
{
    public VersionProvider()
        : this(ReadVersionFromAssembly(), GetRuntimeDescription(), GetAppName())
    {
    }

    private VersionProvider((string? CodeVersion, string? LastCommitDate) version, string runtime, string appName)
    {
        AppName = appName;
        Runtime = runtime;
        CodeVersion = version.CodeVersion ?? "UnknownVersion";
        LastCommitDate = version.LastCommitDate ?? "UnknownDate";
    }

    public string AppName { get; }
    public string Runtime { get; }
    public string CodeVersion { get; }
    public string LastCommitDate { get; }

    private static (string? Version, string? LastCommitDate) ReadVersionFromAssembly()
    {
        var assembly = typeof(VersionProvider).Assembly;
        var versionAttribute = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
        var lastCommitDateAttribute = assembly.GetCustomAttribute<AssemblyDescriptionAttribute>();
        return (versionAttribute?.InformationalVersion, lastCommitDateAttribute?.Description);
    }

    private static string GetRuntimeDescription()
    {
        var os = OperatingSystem.IsWindows() ? "Windows" :
            OperatingSystem.IsLinux() ? "Linux" :
            "Unknown OS";

        var cloud = DetectCloudEnvironment();

        var dotnetVersion = RuntimeInformation.FrameworkDescription;

        return $"{dotnetVersion} on {os}{(string.IsNullOrEmpty(cloud) ? "" : $" ({cloud})")} ";
    }

    private static string DetectCloudEnvironment()
    {
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WEBSITE_SITE_NAME")))
        {
            return "Microsoft Azure";
        }

        // ReSharper disable once ConvertIfStatementToReturnStatement
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CODESPACES")))
        {
            return "GitHub Codespace";
        }

        return string.Empty;
    }

    private static string GetAppName()
    {
        return typeof(VersionProvider).Assembly.GetName().Name ?? "UnknownAppName";
    }
}
