using System.Text.Json.Nodes;
using KafkaToRestApiForwarder.Contracts;

namespace KafkaToRestApiForwarder;

public sealed record ForwardingMessage(
    string Body,
    string? HttpMethod,
    string? UrlSuffix,
    MessageFields.DeviceEventType DeviceEventType,
    DateTime UpdateDate,
    JsonObject Payload);
