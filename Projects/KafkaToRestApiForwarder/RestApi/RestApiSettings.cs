namespace KafkaToRestApiForwarder.RestApi;

// Bound options classes must have public read-write properties.
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
public record ApiBaseUrlSettings
{
    public string Target { get; init; } = "Azure";
    public string Codespace { get; init; } = string.Empty;
    public string Azure { get; init; } = string.Empty;
    public string Local { get; init; } = string.Empty;
}

public record GitHubCodespaceSettings
{
    public string StartUrl { get; init; } = string.Empty;
    public string CodespaceName { get; init; } = string.Empty;
    public int TimeoutSeconds { get; init; } = 30;
}
// ReSharper restore AutoPropertyCanBeMadeGetOnly.Global
