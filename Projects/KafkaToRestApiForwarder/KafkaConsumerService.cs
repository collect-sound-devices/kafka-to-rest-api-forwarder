using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using static KafkaToRestApiForwarder.Contracts.MessageFields;

namespace KafkaToRestApiForwarder;

[SuppressMessage("Performance", "CA1873:Avoid potentially expensive logging")]
public class KafkaConsumerService : BackgroundService
{
    private readonly AutoOffsetReset _autoOffsetReset;
    private readonly string _bootstrapServers;
    private readonly string _consumerGroupId;
    private readonly string _deadLetterTopic;
    private readonly TimeSpan _idleLogInterval;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly KafkaMessageParser _messageParser;
    private readonly int _maxRetryAttempts;
    private readonly IRestApiForwarder _restApiForwarder;
    private readonly TimeSpan _retryDelay;
    private readonly string _topic;

    public KafkaConsumerService(
        IOptions<KafkaConsumerSettings> kafkaConsumerSettings,
        IOptions<KafkaMessageDeliverySettings> kafkaMessageDeliverySettings,
        KafkaMessageParser messageParser,
        IRestApiForwarder restApiForwarder,
        ILogger<KafkaConsumerService> logger)
    {
        var consumerSettings = kafkaConsumerSettings.Value;
        var deliverySettings = kafkaMessageDeliverySettings.Value;

        _bootstrapServers = consumerSettings.BootstrapServers;
        _topic = consumerSettings.Topic;
        _consumerGroupId = consumerSettings.ConsumerGroupId;
        _deadLetterTopic = consumerSettings.DeadLetterTopic;
        _idleLogInterval = TimeSpan.FromSeconds(Math.Max(5, consumerSettings.IdleLogIntervalInSeconds));
        _autoOffsetReset = consumerSettings.AutoOffsetResetEarliest
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest;

        _maxRetryAttempts = Math.Max(1, deliverySettings.MaxRetryAttempts);
        _retryDelay = TimeSpan.FromSeconds(Math.Max(0, deliverySettings.RetryDelayInSeconds));

        _messageParser = messageParser;
        _restApiForwarder = restApiForwarder;
        _logger = logger;

        _logger.LogInformation(
            "Consumer service parameters initialized: BootstrapServers \"{BootstrapServers}\" Topic \"{Topic}\" ConsumerGroupId \"{ConsumerGroupId}\" DeadLetterTopic \"{DeadLetterTopic}\" MaxRetryAttempts {MaxAttempts} RetryDelaySeconds {RetryDelay}",
            _bootstrapServers, _topic, _consumerGroupId, _deadLetterTopic, _maxRetryAttempts,
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

        await CreateTopicsIfMissingAsync(cancellationToken);

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
            var nextIdleLogAt = DateTimeOffset.UtcNow + _idleLogInterval;

            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult;
                try
                {
                    consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    _logger.LogWarning(
                        ex,
                        "Subscribed topic \"{Topic}\" is not available yet. Retrying.",
                        _topic);
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    continue;
                }

                if (consumeResult == null)
                {
                    if (DateTimeOffset.UtcNow >= nextIdleLogAt)
                    {
                        _logger.LogInformation(
                            "Kafka consumer is idle. Waiting for messages on topic \"{Topic}\" as group \"{ConsumerGroupId}\".",
                            _topic, _consumerGroupId);
                        nextIdleLogAt = DateTimeOffset.UtcNow + _idleLogInterval;
                    }

                    continue;
                }

                nextIdleLogAt = DateTimeOffset.UtcNow + _idleLogInterval;
                await ProcessConsumerResultAsync(consumer, deadLetterProducer, consumeResult, cancellationToken);
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

    private async Task CreateTopicsIfMissingAsync(CancellationToken cancellationToken)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _bootstrapServers
        }).Build();

        var topics = new[]
        {
            new TopicSpecification { Name = _topic, NumPartitions = 3, ReplicationFactor = 1 },
            new TopicSpecification { Name = _deadLetterTopic, NumPartitions = 3, ReplicationFactor = 1 }
        };

        try
        {
            await adminClient.CreateTopicsAsync(topics);
            _logger.LogInformation("Kafka topics created or already scheduled for creation: {Topics}",
                string.Join(", ", topics.Select(topic => topic.Name)));
        }
        catch (CreateTopicsException ex)
        {
            var realErrors = ex.Results
                .Where(result => result.Error.Code != ErrorCode.TopicAlreadyExists)
                .ToArray();

            if (realErrors.Length > 0)
            {
                throw;
            }

            _logger.LogInformation("Kafka topics already exist: {Topics}",
                string.Join(", ", ex.Results.Select(result => result.Topic)));
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private async Task ProcessConsumerResultAsync(
        IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        ForwardingMessage forwardingMessage;
        try
        {
            forwardingMessage = ParsePendingMessage(consumeResult);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            var reason = $"Invalid Kafka message payload: {ex.Message}";
            _logger.LogError(ex, "{Reason}. Moving message to dead-letter topic.", reason);
            await DeadLetterAndCommitAsync(consumer, deadLetterProducer, consumeResult, reason, cancellationToken);
            return;
        }

        ForwardingResult result;
        try
        {
            result = await ForwardWithRetriesAsync(forwardingMessage, cancellationToken);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            var reason = $"Unexpected Kafka message processing failure: {ex.Message}";
            _logger.LogError(ex, "{Reason}. Moving message to dead-letter topic.", reason);
            await DeadLetterAndCommitAsync(consumer, deadLetterProducer, consumeResult, reason, cancellationToken);
            return;
        }

        if (result.Success)
        {
            CommitProcessedMessage(consumer, consumeResult);
            _logger.LogInformation(
                "Kafka message commited successfully om TopicPartitionOffset {TopicPartitionOffset}.",
                consumeResult.TopicPartitionOffset);
            return;
        }

        _logger.LogError(
            "All attempt to deliver a Kafka message to REST API server failed. Moving message to dead-letter topic. Reason: {Reason}.", result.ErrorReason);
        await DeadLetterAndCommitAsync(consumer, deadLetterProducer, consumeResult, result.ErrorReason, cancellationToken);
    }

    private async Task<ForwardingResult> ForwardWithRetriesAsync(
        ForwardingMessage forwardingMessage,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= _maxRetryAttempts; attempt++)
        {
            var result = await ProcessMessageAsync(forwardingMessage, attempt, cancellationToken);

            if (result.Success || attempt >= _maxRetryAttempts)
            {
                return result;
            }

            _logger.LogWarning(
                "Attempt {Attempt}/{MaxAttempts} failed. Retrying in {DelaySeconds}s. Reason: {Reason}",
                attempt, _maxRetryAttempts, _retryDelay.TotalSeconds, result.ErrorReason);
            await Task.Delay(_retryDelay, cancellationToken);
        }

        throw new InvalidOperationException("Retry loop completed without a forwarding result.");
    }

    private async Task DeadLetterAndCommitAsync(
        IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        ConsumeResult<string, string> consumeResult,
        string? reason,
        CancellationToken cancellationToken)
    {
        await PublishToDeadLetterTopicAsync(deadLetterProducer, consumeResult, reason, cancellationToken);
        CommitProcessedMessage(consumer, consumeResult);
    }

    private ForwardingMessage ParsePendingMessage(ConsumeResult<string, string> consumeResult)
    {
        var forwardingMessage = _messageParser.Parse(consumeResult.Message.Value);

        var logPayload = JsonNode.Parse(consumeResult.Message.Value)!.AsObject();
        logPayload.Remove(HttpRequest);
        logPayload.Remove(UrlSuffix);

        _logger.LogInformation(
            "Received Kafka message: TopicPartitionOffset {TopicPartitionOffset}, device event type {DeviceEventType}, HTTP request \"{Method}\", suffix \"{Suffix}\", payload:\n{Payload}",
            consumeResult.TopicPartitionOffset, forwardingMessage.DeviceEventType, forwardingMessage.HttpMethod, forwardingMessage.UrlSuffix,
            logPayload.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));

        return forwardingMessage;
    }

    private async Task<ForwardingResult> ProcessMessageAsync(
        ForwardingMessage message,
        int attempt,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processing Kafka message (Attempt {Attempt}/{MaxAttempts}): \"{Method}\", suffix \"{Suffix}\". UpdateDate: {UpdateDate:o}",
            attempt, _maxRetryAttempts, message.HttpMethod, message.UrlSuffix, message.UpdateDate);

        return await _restApiForwarder.ForwardAsync(message, cancellationToken);
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

}
