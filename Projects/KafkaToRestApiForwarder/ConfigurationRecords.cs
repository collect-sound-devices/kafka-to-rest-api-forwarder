namespace KafkaToRestApiForwarder;

// Bound options classes must public read-write properties
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
public record KafkaConsumerSettings
{
    public string BootstrapServers { get; init; } = "localhost:29092";
    public string Topic { get; init; } = "audio-device-events";
    public string ConsumerGroupId { get; init; } = "audio-device-api-forwarder";
    public string DeadLetterTopic { get; init; } = "audio-device-events.failed";
    public bool AutoOffsetResetEarliest { get; init; } = true;
}

public record KafkaMessageDeliverySettings
{
    public int MaxRetryAttempts { get; init; } = 5;
    public int RetryDelayInSeconds { get; init; } = 10;
    // Debouncing interval for VolumeRenderChanged/VolumeCaptureChanged events (milliseconds)
    public int VolumeChangeEventDebouncingWindowInMilliseconds { get; init; } = 400;
}

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
    public string Token { get; init; } = string.Empty;
    public int TimeoutSeconds { get; init; } = 30;
}
// ReSharper restore AutoPropertyCanBeMadeGetOnly.Global
