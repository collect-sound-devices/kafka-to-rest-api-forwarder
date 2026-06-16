using System.Text.Json.Nodes;
using KafkaToRestApiForwarder.Contracts;

namespace KafkaToRestApiForwarder;

public sealed record ForwardingMessage(
    string? HttpMethod,
    string? UrlSuffix,
    DeviceEventType DeviceEventType,
    DateTime UpdateDate,
    JsonObject Payload);
