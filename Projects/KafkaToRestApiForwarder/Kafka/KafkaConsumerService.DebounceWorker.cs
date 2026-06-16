using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaToRestApiForwarder.Kafka;

public partial class KafkaConsumerService
{
    private DebounceWorker? _captureDebouncer;
    private DebounceWorker? _renderDebouncer;
    private TimeSpan _volumeDebounceWindow;


    private void InitializeDebouncers((string render, string capture) names, IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        TimeSpan volumeChangeEventDebouncingWindow,
        CancellationToken cancellationToken
        )
    {
        _volumeDebounceWindow = volumeChangeEventDebouncingWindow;
        _renderDebouncer = CreateDebouncer(names.render, consumer, deadLetterProducer, cancellationToken);
        _captureDebouncer = CreateDebouncer(names.capture, consumer, deadLetterProducer, cancellationToken);
    }

    private void WaitForStopDebouncers()
    {
        _renderDebouncer?.WaitForStop();
        _captureDebouncer?.WaitForStop();
    }


    private DebounceWorker CreateDebouncer(string eventName, IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        CancellationToken cancellationToken)
    {
        return new DebounceWorker(
            eventName,
            _volumeDebounceWindow,
            (consumeResult, ct) => ProcessDebouncedMessageAsync(eventName, consumer, deadLetterProducer, consumeResult, ct),
            (consumeResult) => IgnoreDebouncedMessage(eventName, consumer, consumeResult),
            _logger,
            _messageParser,
            cancellationToken);
    }

    private async Task ProcessDebouncedMessageAsync(string eventName, IConsumer<string, string> consumer, IProducer<string, string> deadLetterProducer,
        ConsumeResult<string, string> consumeResult, CancellationToken ct)
    {
        _logger.LogInformation(
            "Debouncing chosen {message.} message to be PROCESSED",
            eventName);
        await TryForwardOrPublishToDeadLetterAsync(deadLetterProducer, consumeResult, ct);
        CommitProcessedMessage(consumer, consumeResult);
    }

    private void IgnoreDebouncedMessage(string eventName, IConsumer<string, string> consumer, ConsumeResult<string, string> consumeResult)
    {
        _logger.LogInformation(
            "Debouncing chosen {EventName} message to be IGNORED",
            eventName);
        CommitProcessedMessage(consumer, consumeResult);
    }

    private sealed class DebounceWorker
    {
        private readonly string _name;
        private readonly TimeSpan _window;
        private readonly Func<ConsumeResult<string, string>, CancellationToken, Task> _processMessageAsync;
        private readonly Action<ConsumeResult<string, string>> _ignoreMessageAsync;
        private readonly ILogger _logger;
        private readonly KafkaMessageParser _messageParser;

        private readonly Channel<ConsumeResult<string, string>> _queue =
            Channel.CreateUnbounded<ConsumeResult<string, string>>(new UnboundedChannelOptions
                { SingleReader = true, SingleWriter = false });

        private readonly CancellationToken _stopToken;
        private readonly Task _workerTask;

        public DebounceWorker(string name, TimeSpan window,
            Func<ConsumeResult<string, string>, CancellationToken, Task> processMessageAsync,
            Action<ConsumeResult<string, string>> ignoreMessageAsync,
            ILogger logger,
            KafkaMessageParser messageParser,
            CancellationToken stopToken)
        {
            _name = name;
            _window = window;
            _processMessageAsync = processMessageAsync;
            _ignoreMessageAsync = ignoreMessageAsync;
            _logger = logger;
            _messageParser = messageParser;
            _stopToken = stopToken;
            _workerTask = Task.Run(RunAsync, stopToken);
        }

        public void WaitForStop()
        {
            _workerTask.Wait(CancellationToken.None);
        }

        public ValueTask EnqueueAsync(ConsumeResult<string, string> message)
        {
            return _queue.Writer.WriteAsync(message, _stopToken);
        }
        private static async Task<ConsumeResult<string, string>?> GetNextMessageAsync(
            ConsumeResult<string, string>? nextMessage,
            ChannelReader<ConsumeResult<string, string>> reader,
            CancellationToken cancellationToken)
        {
            if (nextMessage != null)
            {
                return nextMessage;
            }
            try
            {
                return await reader.ReadAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return null; // Signal to break the loop
            }
        }

        private ConsumeResult<string, string>? ProcessDebounceWindow(
                ConsumeResult<string, string> current,
                ChannelReader<ConsumeResult<string, string>> reader)
        {
            var latest = current;
            var latestMessage = _messageParser.Parse(latest.Message.Value);
            ConsumeResult<string, string>? next = null;
            while (reader.TryRead(out var nextRead))
            {
                var nextMessage = _messageParser.Parse(nextRead.Message.Value);
                if ((nextMessage.UpdateDate - latestMessage.UpdateDate) <= _window) // within the time window?
                {
                    try
                    {
                        _ignoreMessageAsync(latest);
                    }
                    catch
                    {
                        // Ignored
                    }

                    latest = nextRead; // keep the most recent within the window
                    continue;
                }

                // Next is outside the window; keep it for next iteration
                next = nextRead;
                break;
            }

            return next;
        }
        private async Task ProcessMessageSafelyAsync(ConsumeResult<string, string> message)
        {
            try
            {
                await _processMessageAsync(message, _stopToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{Name}] Error or stop while processing debounced message.", _name);
                try
                {
                    _ignoreMessageAsync(message);
                }
                catch
                {
                    // Ignored
                }
            }
        }
        private async Task RunAsync()
        {
            var reader = _queue.Reader;
            ConsumeResult<string, string>? nextMessage = null;
            try
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    var currentMessage = await GetNextMessageAsync(nextMessage, reader, _stopToken);
                    if (currentMessage == null)
                    {
                        break; // Exit the loop if cancellation is requested
                    }
                    // Process messages within the debounce window
                    nextMessage = ProcessDebounceWindow(currentMessage, reader);
                    // Process the chosen latest message
                    await ProcessMessageSafelyAsync(currentMessage);
                }
            }
            catch (OperationCanceledException)
            {
                // Gracefully exit on cancellation
            }
        }
    }
}