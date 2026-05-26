using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using static KafkaToRestApiForwarder.Contracts.MessageFields;

namespace KafkaToRestApiForwarder;

public class KafkaConsumerService : BackgroundService
{
    private static readonly string[] _validTargets =
    [
        nameof(ApiBaseUrlSettings.Azure), nameof(ApiBaseUrlSettings.Local), nameof(ApiBaseUrlSettings.Codespace)
    ];

    private static readonly string _validTargetsAsString = string.Join(
        ", ",
        Array.ConvertAll(_validTargets, v => $"\"{v}\""));

    private readonly string _apiEndpoint;
    private readonly string _apiTarget;
    private readonly AutoOffsetReset _autoOffsetReset;
    private readonly string _bootstrapServers;
    private readonly GitHubCodespaceAwaker _codespaceAwaker;
    private readonly string _consumerGroupId;
    private readonly string _deadLetterTopic;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly int _maxRetryAttempts;
    private readonly TimeSpan _retryDelay;
    private readonly string _topic;

    public KafkaConsumerService(
        IOptions<KafkaConsumerSettings> kafkaConsumerSettings,
        IOptions<KafkaMessageDeliverySettings> kafkaMessageDeliverySettings,
        IOptions<ApiBaseUrlSettings> apiSettings,
        GitHubCodespaceAwaker codespaceAwaker,
        IHttpClientFactory httpClientFactory,
        ILogger<KafkaConsumerService> logger)
    {
        var consumerSettings = kafkaConsumerSettings.Value;
        var deliverySettings = kafkaMessageDeliverySettings.Value;

        _bootstrapServers = consumerSettings.BootstrapServers;
        _topic = consumerSettings.Topic;
        _consumerGroupId = consumerSettings.ConsumerGroupId;
        _deadLetterTopic = consumerSettings.DeadLetterTopic;
        _autoOffsetReset = consumerSettings.AutoOffsetResetEarliest
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest;

        _maxRetryAttempts = Math.Max(1, deliverySettings.MaxRetryAttempts);
        _retryDelay = TimeSpan.FromSeconds(Math.Max(0, deliverySettings.RetryDelayInSeconds));

        _codespaceAwaker = codespaceAwaker;
        _httpClientFactory = httpClientFactory;
        _logger = logger;

        _apiTarget = apiSettings.Value.Target;
        if (Array.IndexOf(_validTargets, _apiTarget) < 0)
        {
            _logger.LogWarning(
                "Service initializing: Unknown Target REST API \"{ApiTarget}\". The possible values are {PossibleTargets}. Setting it to default value \"{Default}\"",
                _apiTarget, _validTargetsAsString, nameof(ApiBaseUrlSettings.Azure));

            _apiTarget = nameof(ApiBaseUrlSettings.Azure);
        }

        _apiEndpoint = _apiTarget switch
        {
            nameof(ApiBaseUrlSettings.Azure) => apiSettings.Value.Azure,
            nameof(ApiBaseUrlSettings.Codespace) => apiSettings.Value.Codespace,
            nameof(ApiBaseUrlSettings.Local) => apiSettings.Value.Local,
            _ => apiSettings.Value.Azure
        };

        _logger.LogInformation(
            "Consumer service parameters initialized: BootstrapServers \"{BootstrapServers}\" Topic \"{Topic}\" ConsumerGroupId \"{ConsumerGroupId}\" DeadLetterTopic \"{DeadLetterTopic}\" Target REST API \"{ApiTarget}\" MaxRetryAttempts {MaxAttempts} RetryDelaySeconds {RetryDelay}",
            _bootstrapServers, _topic, _consumerGroupId, _deadLetterTopic, _apiTarget, _maxRetryAttempts,
            _retryDelay.TotalSeconds);
    }

    protected override async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _consumerGroupId,
            AutoOffsetReset = _autoOffsetReset,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) => _logger.LogError("Kafka consumer error: {Reason}", error.Reason))
            .Build();
        using var deadLetterProducer = new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler((_, error) => _logger.LogError("Kafka producer error: {Reason}", error.Reason))
            .Build();

        consumer.Subscribe(_topic);
        _logger.LogInformation("Kafka consumer started. Topic \"{Topic}\"", _topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                if (consumeResult == null)
                {
                    continue;
                }

                await ProcessConsumeResultAsync(consumer, deadLetterProducer, consumeResult, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal host shutdown.
        }
        finally
        {
            _logger.LogInformation("Closing Kafka consumer.");
            consumer.Close();
        }
    }

    private async Task ProcessConsumeResultAsync(
        IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        PendingMessage pendingMessage;
        try
        {
            pendingMessage = ParsePendingMessage(consumeResult);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            var reason = $"Invalid Kafka message payload: {ex.Message}";
            _logger.LogError(ex, "{Reason}. Moving message to dead-letter topic.", reason);
            await PublishToDeadLetterTopicAsync(deadLetterProducer, consumeResult, reason, cancellationToken);
            CommitProcessedMessage(consumer, consumeResult);
            return;
        }

        for (var attempt = 1; attempt <= _maxRetryAttempts; attempt++)
        {
            ProcessingResult result;
            try
            {
                result = await ProcessMessageAsync(pendingMessage, attempt, cancellationToken);
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException)
            {
                var reason = $"Unexpected Kafka message processing failure: {ex.Message}";
                _logger.LogError(ex, "{Reason}. Moving message to dead-letter topic.", reason);
                await PublishToDeadLetterTopicAsync(deadLetterProducer, consumeResult, reason, cancellationToken);
                CommitProcessedMessage(consumer, consumeResult);
                return;
            }

            if (result.Success)
            {
                CommitProcessedMessage(consumer, consumeResult);
                _logger.LogInformation(
                    "Message processed successfully. TopicPartitionOffset {TopicPartitionOffset} Attempt {Attempt}",
                    consumeResult.TopicPartitionOffset, attempt);
                return;
            }

            if (attempt < _maxRetryAttempts)
            {
                _logger.LogWarning(
                    "Attempt {Attempt}/{MaxAttempts} failed. Retrying in {DelaySeconds}s. Reason: {Reason}",
                    attempt, _maxRetryAttempts, _retryDelay.TotalSeconds, result.ErrorReason);
                await Task.Delay(_retryDelay, cancellationToken);
                continue;
            }

            await PublishToDeadLetterTopicAsync(deadLetterProducer, consumeResult, result.ErrorReason, cancellationToken);
            CommitProcessedMessage(consumer, consumeResult);
            _logger.LogError(
                "Attempt {Attempt} (max) failed. Message published to dead-letter topic \"{DeadLetterTopic}\". Reason: {Reason}",
                attempt, _deadLetterTopic, result.ErrorReason);
        }
    }

    private PendingMessage ParsePendingMessage(ConsumeResult<string, string> consumeResult)
    {
        var eventMessage = JsonNode.Parse(consumeResult.Message.Value)!.AsObject();

        var deviceMessageTypeAsInt = eventMessage[DeviceMessageType]?.GetValue<int?>();
        var updateDateAsString = eventMessage[UpdateDate]?.GetValue<string>();
        var updateDate = ParseToUtc(updateDateAsString);
        var deviceEventType = deviceMessageTypeAsInt.HasValue
            ? (DeviceEventType)deviceMessageTypeAsInt.Value
            : DeviceEventType.Confirmed;

        var httpRequest = eventMessage[HttpRequest]?.GetValue<string>();
        var urlSuffix = eventMessage[UrlSuffix]?.GetValue<string>();

        var logPayload = JsonNode.Parse(consumeResult.Message.Value)!.AsObject();
        logPayload.Remove(HttpRequest);
        logPayload.Remove(UrlSuffix);

        _logger.LogInformation(
            "Received Kafka message: TopicPartitionOffset {TopicPartitionOffset}, device event type {DeviceEventType}, HTTP request \"{Method}\", suffix \"{Suffix}\", payload:\n{Payload}",
            consumeResult.TopicPartitionOffset, deviceEventType, httpRequest, urlSuffix,
            logPayload.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));

        return new PendingMessage(
            consumeResult.Message.Value,
            httpRequest,
            urlSuffix,
            updateDate);
    }

    private async Task<ProcessingResult> ProcessMessageAsync(
        PendingMessage message,
        int attempt,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processing Kafka message (Attempt {Attempt}/{MaxAttempts}): \"{Method}\", suffix \"{Suffix}\". UpdateDate: {UpdateDate:o}",
            attempt, _maxRetryAttempts, message.HttpMethod, message.UrlSuffix, message.UpdateDate);

        var eventMessage = JsonNode.Parse(message.Body)!.AsObject();
        return await SendToApiAsync(message.HttpMethod, message.UrlSuffix, eventMessage, cancellationToken);
    }

    private async Task<ProcessingResult> SendToApiAsync(
        string? httpMethod,
        string? urlSuffix,
        JsonObject payload,
        CancellationToken cancellationToken)
    {
        if (urlSuffix == null)
        {
            return new ProcessingResult(false, "urlSuffix is null");
        }

        if (string.IsNullOrWhiteSpace(httpMethod))
        {
            return new ProcessingResult(false, "httpMethod is null or empty");
        }

        try
        {
            using var httpClient = _httpClientFactory.CreateClient();
            using var jsonContent = new StringContent(payload.ToJsonString(), Encoding.UTF8, "application/json");

            var response = httpMethod.ToUpperInvariant() == "PUT"
                ? await httpClient.PutAsync(_apiEndpoint + urlSuffix, jsonContent, cancellationToken)
                : await httpClient.PostAsync(_apiEndpoint + urlSuffix, jsonContent, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                var reason = $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}";
                throw new Exception(reason);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Caught {ExceptionType} exception: {Message}.", ex.GetType().Name, ex.Message);
            if (_apiTarget == nameof(ApiBaseUrlSettings.Codespace))
            {
                await _codespaceAwaker.Awake(cancellationToken);
            }

            return new ProcessingResult(false, $"Exception: {ex.Message}");
        }

        return new ProcessingResult(true, null);
    }

    private async Task PublishToDeadLetterTopicAsync(
        IProducer<string, string> producer,
        ConsumeResult<string, string> consumeResult,
        string? reason,
        CancellationToken cancellationToken)
    {
        var envelope = new JsonObject
        {
            ["failedAt"] = DateTimeOffset.UtcNow,
            ["reason"] = reason,
            ["originalTopic"] = consumeResult.Topic,
            ["originalPartition"] = consumeResult.Partition.Value,
            ["originalOffset"] = consumeResult.Offset.Value,
            ["payload"] = TryParsePayload(consumeResult.Message.Value)
        };

        await producer.ProduceAsync(
            _deadLetterTopic,
            new Message<string, string>
            {
                Key = consumeResult.Message.Key,
                Value = envelope.ToJsonString()
            },
            cancellationToken);
    }

    private void CommitProcessedMessage(IConsumer<string, string> consumer, ConsumeResult<string, string> consumeResult)
    {
        try
        {
            consumer.StoreOffset(consumeResult);
            consumer.Commit(consumeResult);
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Failed to commit Kafka offset {TopicPartitionOffset}", consumeResult.TopicPartitionOffset);
            throw;
        }
    }

    private static JsonNode? TryParsePayload(string payload)
    {
        try
        {
            return JsonNode.Parse(payload);
        }
        catch (JsonException)
        {
            return JsonValue.Create(payload);
        }
    }

    private static DateTime ParseToUtc(string? input)
    {
        if (string.IsNullOrWhiteSpace(input)) return DateTime.MinValue;
        if (DateTimeOffset.TryParse(input, out var dto)) return dto.UtcDateTime;
        if (DateTime.TryParse(input, out var dt)) return dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : dt;

        return DateTime.MinValue;
    }

    private sealed record PendingMessage(
        string Body,
        string? HttpMethod,
        string? UrlSuffix,
        DateTime UpdateDate
    );

    private readonly record struct ProcessingResult(bool Success, string? ErrorReason);
}
