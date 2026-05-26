using System.Text.Json;
using KafkaToRestApiForwarder.Contracts;

namespace KafkaToRestApiForwarder.Tests;

public class KafkaMessageParserTests
{
    private readonly KafkaMessageParser _sut = new();

    [Test]
    public void Parse_WhenMessageIsValid_ReturnsForwardingMessage()
    {
        const string body = """
                            {
                              "httpRequest": "POST",
                              "urlSuffix": "/manual",
                              "deviceMessageType": 1,
                              "updateDate": "2026-05-26T10:00:00Z",
                              "name": "Manual Kafka Test Device"
                            }
                            """;

        var message = _sut.Parse(body);

        Assert.That(message.Body, Is.EqualTo(body));
        Assert.That(message.HttpMethod, Is.EqualTo("POST"));
        Assert.That(message.UrlSuffix, Is.EqualTo("/manual"));
        Assert.That(message.DeviceEventType, Is.EqualTo(MessageFields.DeviceEventType.Discovered));
        Assert.That(message.UpdateDate, Is.EqualTo(new DateTime(2026, 5, 26, 10, 0, 0, DateTimeKind.Utc)));
        Assert.That(message.Payload["name"]?.GetValue<string>(), Is.EqualTo("Manual Kafka Test Device"));
    }

    [Test]
    public void Parse_WhenDeviceMessageTypeIsMissing_DefaultsToConfirmed()
    {
        const string body = """
                            {
                              "httpRequest": "PUT",
                              "urlSuffix": "/devices/1",
                              "updateDate": "2026-05-26T10:00:00Z"
                            }
                            """;

        var message = _sut.Parse(body);

        Assert.That(message.DeviceEventType, Is.EqualTo(MessageFields.DeviceEventType.Confirmed));
    }

    [Test]
    public void Parse_WhenUpdateDateContainsOffset_ConvertsToUtc()
    {
        const string body = """
                            {
                              "httpRequest": "POST",
                              "urlSuffix": "",
                              "deviceMessageType": 1,
                              "updateDate": "2026-05-26T12:00:00+02:00"
                            }
                            """;

        var message = _sut.Parse(body);

        Assert.That(message.UpdateDate, Is.EqualTo(new DateTime(2026, 5, 26, 10, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public void Parse_WhenUpdateDateIsMissing_ReturnsMinValue()
    {
        const string body = """
                            {
                              "httpRequest": "POST",
                              "urlSuffix": "",
                              "deviceMessageType": 1
                            }
                            """;

        var message = _sut.Parse(body);

        Assert.That(message.UpdateDate, Is.EqualTo(DateTime.MinValue));
    }

    [Test]
    public void Parse_WhenJsonIsInvalid_ThrowsJsonException()
    {
        Assert.That(() => _sut.Parse("{"), Throws.InstanceOf<JsonException>());
    }

    [Test]
    public void Parse_WhenJsonIsNotObject_ThrowsInvalidOperationException()
    {
        Assert.Throws<InvalidOperationException>(() => _sut.Parse("[]"));
    }
}
