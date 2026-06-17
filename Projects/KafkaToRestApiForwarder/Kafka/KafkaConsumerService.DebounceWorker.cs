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
            (message, ct) => ProcessDebouncedMessageAsync(eventName, consumer, deadLetterProducer, message, ct),
            (message) => IgnoreDebouncedMessage(eventName, consumer, message),
            _logger,
            cancellationToken);
    }

    private async Task ProcessDebouncedMessageAsync(string eventName, IConsumer<string, string> consumer, IProducer<string, string> deadLetterProducer,
        ForwardingMessage message, CancellationToken ct)
    {
        _logger.LogInformation(
            "Debouncing chosen {message} message to be PROCESSED",
            eventName);
        await TryForwardOrPublishToDeadLetterAsync(deadLetterProducer, message, ct);
        CommitProcessedMessage(consumer, message.ConsumeResult);
    }

    private void IgnoreDebouncedMessage(string eventName, IConsumer<string, string> consumer, ForwardingMessage message)
    {
        _logger.LogInformation(
            "Debouncing chosen {EventName} message to be IGNORED",
            eventName);
        CommitProcessedMessage(consumer, message.ConsumeResult);
    }

    private sealed class DebounceWorker
    {
        private readonly string _name;
        private readonly TimeSpan _window;
        private readonly Func<ForwardingMessage, CancellationToken, Task> _processMessageAsync;
        private readonly Action<ForwardingMessage> _ignoreMessageAsync;
        private readonly ILogger _logger;

        private readonly Channel<ForwardingMessage> _queue =
            Channel.CreateUnbounded<ForwardingMessage>(new UnboundedChannelOptions
                { SingleReader = true, SingleWriter = false });

        private readonly CancellationToken _stopToken;
        private readonly Task _workerTask;

        public DebounceWorker(string name, TimeSpan window,
            Func<ForwardingMessage, CancellationToken, Task> processMessageAsync,
            Action<ForwardingMessage> ignoreMessageAsync,
            ILogger logger,
            CancellationToken stopToken)
        {
            _name = name;
            _window = window;
            _processMessageAsync = processMessageAsync;
            _ignoreMessageAsync = ignoreMessageAsync;
            _logger = logger;
            _stopToken = stopToken;
            _workerTask = Task.Run(RunAsync, stopToken);
        }

        public void WaitForStop()
        {
            _workerTask.Wait(CancellationToken.None);
        }

        public ValueTask EnqueueAsync(ForwardingMessage message)
        {
            return _queue.Writer.WriteAsync(message, _stopToken);
        }
        private static async Task<ForwardingMessage?> GetNextMessageAsync(
            ForwardingMessage? nextMessage,
            ChannelReader<ForwardingMessage> reader,
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

        private ForwardingMessage? ProcessDebounceWindow(
                ForwardingMessage currentMessage,
                ChannelReader<ForwardingMessage> reader)
        {
            var latestMessage = currentMessage;
            ForwardingMessage? nextMessage = null;
            while (reader.TryRead(out var next))
            {
                if ((next.UpdateDate - latestMessage.UpdateDate) <= _window) // within the time window?
                {
                    try
                    {
                        _ignoreMessageAsync(latestMessage);
                    }
                    catch
                    {
                        // Ignored
                    }
                    
                    latestMessage = next; // keep the most recent within the window
                    continue;
                }
                // Next is outside the window; keep it for next iteration
                nextMessage = next;
                break;
            }
            return nextMessage;
        }
        private async Task ProcessMessageSafelyAsync(ForwardingMessage message)
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
            ForwardingMessage? nextMessage = null;
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