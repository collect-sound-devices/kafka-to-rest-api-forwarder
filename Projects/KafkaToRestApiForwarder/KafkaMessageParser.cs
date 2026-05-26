using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using static KafkaToRestApiForwarder.Contracts.MessageFields;

namespace KafkaToRestApiForwarder;

[SuppressMessage("Performance", "CA1873:Avoid potentially expensive logging")]
public sealed class KafkaMessageParser(ILogger<KafkaMessageParser> logger)
{
    public ForwardingMessage Parse(string body)
    {
        var payload = JsonNode.Parse(body)?.AsObject()
            ?? throw new JsonException("Kafka message payload must be a JSON object.");

        var deviceEventType = GetDeviceEventType(payload);
        var updateDate = GetUpdateDate(payload);
        logger.LogInformation("Parsed Kafka event payload of tyoe: {DeviceEventType}, generated at: {UpdateDate}.", deviceEventType, updateDate);

        return new ForwardingMessage(
            body,
            payload[HttpRequest]?.GetValue<string>(),
            payload[UrlSuffix]?.GetValue<string>(),
            deviceEventType,
            updateDate,
            payload);
    }

    private static DeviceEventType GetDeviceEventType(JsonObject payload)
    {
        var deviceMessageTypeAsInt = payload[DeviceMessageType]?.GetValue<int?>();
        return deviceMessageTypeAsInt.HasValue
            ? (DeviceEventType)deviceMessageTypeAsInt.Value
            : DeviceEventType.Confirmed;
    }

    private static DateTime GetUpdateDate(JsonObject payload)
    {
        return ParseToUtc(payload[UpdateDate]?.GetValue<string>());
    }

    private static DateTime ParseToUtc(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return DateTime.MinValue;
        if (DateTimeOffset.TryParse(input, out var dto)) return dto.UtcDateTime;
        if (DateTime.TryParse(input, out var dt)) return dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : dt;

        return DateTime.MinValue;
    }
}
