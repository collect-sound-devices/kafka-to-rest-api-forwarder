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
            (message, ct) => ForwardDebouncedMessageAsync(eventName, consumer, deadLetterProducer, message, ct),
            (message) => IgnoreDebouncedMessage(eventName, consumer, message),
            _logger,
            cancellationToken);
    }

    private async Task ForwardDebouncedMessageAsync(string eventName, IConsumer<string, string> consumer, IProducer<string, string> deadLetterProducer,
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
        private readonly TimeSpan _debounceWindow;
        private readonly Func<ForwardingMessage, CancellationToken, Task> _forwardMessageAsync;
        private readonly Action<ForwardingMessage> _ignoreMessageAsync;
        private readonly ILogger _logger;

        private readonly Channel<ForwardingMessage> _queue =
            Channel.CreateUnbounded<ForwardingMessage>(new UnboundedChannelOptions
            { SingleReader = true, SingleWriter = false });

        private readonly CancellationToken _stopToken;
        private readonly Task _workerTask;

        public DebounceWorker(string name, TimeSpan window,
            Func<ForwardingMessage, CancellationToken, Task> forwardMessageAsync,
            Action<ForwardingMessage> ignoreMessageAsync,
            ILogger logger,
            CancellationToken stopToken)
        {
            _name = name;
            _debounceWindow = window;
            _forwardMessageAsync = forwardMessageAsync;
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

        private (ForwardingMessage messageToForward, ForwardingMessage? firstMessageAfterWindow) ChooseMessageToForward(
            ForwardingMessage firstMessageInWindow,
            ChannelReader<ForwardingMessage> reader)
        {
            var messageToForward = firstMessageInWindow;
            ForwardingMessage? firstMessageAfterWindow = null;
            while (reader.TryRead(out var candidateMessage))
            {
                if ((candidateMessage.UpdateDate - messageToForward.UpdateDate) <= _debounceWindow) // within the time window?
                {
                    try
                    {
                        _ignoreMessageAsync(messageToForward);
                    }
                    catch
                    {
                        // Ignored
                    }

                    messageToForward = candidateMessage; // keep the most recent within the window
                    continue;
                }

                // The candidateMessage is now outside the window; keep it for next iteration
                firstMessageAfterWindow = candidateMessage;
                break;
            }

            return (messageToForward, firstMessageAfterWindow);
        }

        private async Task RunAsync()
        {
            var reader = _queue.Reader;
            ForwardingMessage? firstMessageAfterWindow = null;
            try
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    ForwardingMessage firstMessageInWindow;
                    if (firstMessageAfterWindow != null)
                    {
                        firstMessageInWindow = firstMessageAfterWindow;
                    }
                    else
                    {
                        try
                        {
                            firstMessageInWindow = await reader.ReadAsync(_stopToken);
                        }
                        catch (OperationCanceledException)
                        {
                            break; // Exit the loop if cancellation is requested
                        }
                    }

                    (var messageToForward, firstMessageAfterWindow) =
                        ChooseMessageToForward(firstMessageInWindow, reader);

                    // Process the chosen latest message
                    try
                    {
                        await _forwardMessageAsync(messageToForward, _stopToken);
                    }
                    catch
                    {
                        // Ignored
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Just exit on cancellation
            }
        }
    }
}
