using System.Text.Json;
using KafkaToRestApiForwarder.Contracts;
using KafkaToRestApiForwarder.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace KafkaToRestApiForwarder.Tests;

public class KafkaMessageParserTests
{
    private readonly KafkaMessageParser _sut = new(Substitute.For<ILogger<KafkaMessageParser>>());

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

        var (httpMethod, urlSuffix, deviceEventType, updateDate, payload) = _sut.Parse(body);

        using (Assert.EnterMultipleScope())
        {
            Assert.That(httpMethod, Is.EqualTo("POST"));
            Assert.That(urlSuffix, Is.EqualTo("/manual"));
            Assert.That(deviceEventType, Is.EqualTo(DeviceEventType.Discovered));
            Assert.That(updateDate, Is.EqualTo(new DateTime(2026, 5, 26, 10, 0, 0, DateTimeKind.Utc)));
            Assert.That(payload["name"]?.GetValue<string>(), Is.EqualTo("Manual Kafka Test Device"));
        }
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

        var (_, _, deviceEventType, _, _) = _sut.Parse(body);

        Assert.That(deviceEventType, Is.EqualTo(DeviceEventType.Confirmed));
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

        var (_, _, _, updateDate, _) = _sut.Parse(body);

        Assert.That(updateDate, Is.EqualTo(new DateTime(2026, 5, 26, 10, 0, 0, DateTimeKind.Utc)));
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

        var (_, _, _, updateDate, _) = _sut.Parse(body);

        Assert.That(updateDate, Is.EqualTo(DateTime.MinValue));
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
