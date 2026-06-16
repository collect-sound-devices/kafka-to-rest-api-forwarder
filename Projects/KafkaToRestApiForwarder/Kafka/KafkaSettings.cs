namespace KafkaToRestApiForwarder.Kafka;

// Bound options classes must have public read-write properties.
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global
public record KafkaConsumerSettings
{
    public string BootstrapServers { get; init; } = "localhost:29092";
    public string Topic { get; init; } = "audio-device-events";
    public string ConsumerGroupId { get; init; } = "audio-device-api-forwarder";
    public string DeadLetterTopic { get; init; } = "audio-device-events.failed";
    public bool AutoOffsetResetEarliest { get; init; } = true;
    public int IdleLogIntervalInSeconds { get; init; } = 30;
    // Debouncing interval for VolumeRenderChanged/VolumeCaptureChanged events (milliseconds)
    public int VolumeChangeEventDebouncingWindowInMilliseconds { get; init; } = 400;
}

public record KafkaMessageDeliverySettings
{
    public int MaxRetryAttempts { get; init; } = 5;
    public int RetryDelayInSeconds { get; init; } = 10;
}
// ReSharper restore AutoPropertyCanBeMadeGetOnly.Global
