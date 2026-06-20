using System.Text.Json.Nodes;
using Confluent.Kafka;
using KafkaToRestApiForwarder.Contracts;

namespace KafkaToRestApiForwarder;

public sealed record ForwardingMessage(
    string? HttpMethod,
    string? UrlSuffix,
    DeviceEventType DeviceEventType,
    DateTime UpdateDate,
    JsonObject Payload,
    ConsumeResult<string, string> ConsumeResult) : IHasUpdateDate;
