namespace KafkaToRestApiForwarder;

public readonly record struct ForwardingResult(bool Success, string? ErrorReason);
