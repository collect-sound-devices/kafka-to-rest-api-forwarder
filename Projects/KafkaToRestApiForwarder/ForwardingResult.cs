namespace KafkaToRestApiForwarder;

public sealed record ForwardingResult(bool Success, string? ErrorReason);
