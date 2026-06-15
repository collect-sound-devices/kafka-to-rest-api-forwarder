using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaToRestApiForwarder.RestApi;
using Microsoft.Extensions.Options;
using static KafkaToRestApiForwarder.Contracts.MessagePayloadFields;

namespace KafkaToRestApiForwarder.Kafka;

[SuppressMessage("Performance", "CA1873:Avoid potentially expensive logging")]
public class KafkaConsumerService : BackgroundService
{
    private readonly AutoOffsetReset _autoOffsetReset;

    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _consumerGroupId;
    private readonly string _deadLetterTopic;
    private readonly TimeSpan _idleLogInterval;
    private readonly int _maxRetryAttempts;
    private readonly TimeSpan _retryDelay;

    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly KafkaMessageParser _messageParser;

    private readonly IRestApiForwarder _restApiForwarder;

    public KafkaConsumerService(
        IOptions<KafkaConsumerSettings> kafkaConsumerSettings,
        IOptions<KafkaMessageDeliverySettings> kafkaMessageDeliverySettings,
        KafkaMessageParser messageParser,
        IRestApiForwarder restApiForwarder,
        ILogger<KafkaConsumerService> logger)
    {
        // Kafka consumer settings
        var consumerSettings = kafkaConsumerSettings.Value;
        _bootstrapServers = consumerSettings.BootstrapServers;
        _topic = consumerSettings.Topic;
        _consumerGroupId = consumerSettings.ConsumerGroupId;
        _deadLetterTopic = consumerSettings.DeadLetterTopic;
        _autoOffsetReset = consumerSettings.AutoOffsetResetEarliest
            ? AutoOffsetReset.Earliest
            : AutoOffsetReset.Latest;
        _idleLogInterval = TimeSpan.FromSeconds(
            Math.Max(5, consumerSettings.IdleLogIntervalInSeconds));

        // Kafka message delivery settings
        var deliverySettings = kafkaMessageDeliverySettings.Value;
        _maxRetryAttempts = Math.Max(1, deliverySettings.MaxRetryAttempts);
        _retryDelay = TimeSpan.FromSeconds(
            Math.Max(0, deliverySettings.RetryDelayInSeconds));

        _messageParser = messageParser;
        _restApiForwarder = restApiForwarder;
        _logger = logger;

        _logger.LogInformation(
            """
            Consumer service parameters initialized:
              BootstrapServers "{BootstrapServers}"
              Topic "{Topic}"
              ConsumerGroupId "{ConsumerGroupId}"
              DeadLetterTopic "{DeadLetterTopic}"
              MaxRetryAttempts {MaxAttempts}
              RetryDelaySeconds {RetryDelay}
            """,
            _bootstrapServers,
            _topic,
            _consumerGroupId,
            _deadLetterTopic,
            _maxRetryAttempts,
            _retryDelay.TotalSeconds);
    }

    // ReSharper disable CognitiveComplexity
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _consumerGroupId,
            AutoOffsetReset = _autoOffsetReset,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };

        var deadLetterProducerConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers
        };

        // Ensure topics exist before starting to consume messages
        await CreateTopicsIfMissingAsync(stoppingToken);
        _logger.LogInformation("Kafka topics verified/created successfully.");


        // Create consumer and producer instances
        using var consumer = CreateConsumer(consumerConfig);
        using var deadLetterProducer = CreateDeadLetterProducer(deadLetterProducerConfig);
        _logger.LogInformation("Kafka consumer and dead-letter producer instances created successfully.");

        // Subscribe to the main topic
        consumer.Subscribe(_topic);
        _logger.LogInformation("Kafka consumer started. Topic \"{Topic}\"", _topic);

        try
        {
            var nextIdleLogAt = DateTimeOffset.UtcNow + _idleLogInterval;

            while (!stoppingToken.IsCancellationRequested)
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
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
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
                _logger.LogInformation("Kafka consumer received a message on offset: {TopicPartitionOffset}. Processing it...", consumeResult.TopicPartitionOffset);
                await ProcessConsumerResultAsync(consumer, deadLetterProducer, consumeResult, stoppingToken);

                nextIdleLogAt = DateTimeOffset.UtcNow + _idleLogInterval;
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
    // ReSharper restore CognitiveComplexity

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

    private IConsumer<string, string> CreateConsumer(ConsumerConfig consumerConfig)
    {
        return new ConsumerBuilder<string, string>(consumerConfig)
            .SetErrorHandler((_, error) =>
                _logger.LogError("Kafka consumer error: {Reason}", error.Reason))
            .Build();
    }

    private IProducer<string, string> CreateDeadLetterProducer(ProducerConfig producerConfig)
    {
        return new ProducerBuilder<string, string>(producerConfig)
            .SetErrorHandler((_, error) =>
                _logger.LogError("Kafka producer error: {Reason}", error.Reason))
            .Build();
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
            _logger.LogInformation(
                """
                Forwarding the Kafka message (Attempt {Attempt}/{MaxAttempts}) to the REST API:
                \"{Method}\", suffix \"{Suffix}\". UpdateDate: {UpdateDate:o}
                """,
                attempt, _maxRetryAttempts,
                forwardingMessage.HttpMethod, forwardingMessage.UrlSuffix, forwardingMessage.UpdateDate);

            var result = await _restApiForwarder.ForwardAsync(forwardingMessage, cancellationToken);

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
