using Confluent.Kafka;
using System.Threading.Channels;

namespace KafkaToRestApiForwarder.Kafka;

public partial class KafkaConsumerService
{
    private DebounceWorker<ForwardingMessage>? _captureDebouncer;
    private DebounceWorker<ForwardingMessage>? _renderDebouncer;
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


    private DebounceWorker<ForwardingMessage> CreateDebouncer(string eventName, IConsumer<string, string> consumer,
        IProducer<string, string> deadLetterProducer,
        CancellationToken cancellationToken)
    {
        return new DebounceWorker<ForwardingMessage>(_volumeDebounceWindow,
            (message, ct) => ForwardDebouncedMessageAsync(eventName, consumer, deadLetterProducer, message, ct),
            (message, ct) => IgnoreDebouncedMessageAsync(eventName, consumer, message),
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

    private Task IgnoreDebouncedMessageAsync(string eventName, IConsumer<string, string> consumer, ForwardingMessage message)
    {
        _logger.LogInformation(
            "Debouncing chosen {EventName} message to be IGNORED",
            eventName);
        CommitProcessedMessage(consumer, message.ConsumeResult);
        return Task.CompletedTask;
    }

    private sealed class DebounceWorker<TMessage> where TMessage : class, IHasUpdateDate
    {
        private readonly TimeSpan _debounceWindow;
        private readonly Func<TMessage, CancellationToken, Task> _forwardMessageAsync;
        private readonly Func<TMessage, CancellationToken, Task> _ignoreMessageAsync;

        private readonly Channel<TMessage> _queue =
            Channel.CreateUnbounded<TMessage>(new UnboundedChannelOptions
            { SingleReader = true, SingleWriter = false });

        private readonly CancellationToken _stopToken;
        private readonly Task _workerTask;

        public DebounceWorker(TimeSpan window,
            Func<TMessage, CancellationToken, Task> forwardMessageAsync,
            Func<TMessage, CancellationToken, Task> ignoreMessageAsync,
            CancellationToken stopToken)
        {
            _debounceWindow = window;
            _forwardMessageAsync = forwardMessageAsync;
            _ignoreMessageAsync = ignoreMessageAsync;
            _stopToken = stopToken;
            _workerTask = Task.Run(RunAsync, stopToken);
        }

        public void WaitForStop()
        {
            _workerTask.Wait(CancellationToken.None);
        }

        public ValueTask EnqueueAsync(TMessage message)
        {
            return _queue.Writer.WriteAsync(message, _stopToken);
        }

        private async Task<(TMessage messageToForward, TMessage? firstMessageAfterWindow)> ChooseMessageToForwardAsync(
            TMessage firstMessageInWindow,
            ChannelReader<TMessage> reader)
        {
            var messageToForward = firstMessageInWindow;
            TMessage? firstMessageAfterWindow = null;
            while (reader.TryRead(out var candidateMessage))
            {
                if ((candidateMessage.UpdateDate - messageToForward.UpdateDate) <= _debounceWindow) // within the time window?
                {
                    try
                    {
                        await _ignoreMessageAsync(messageToForward, _stopToken);
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

        // ReSharper disable CognitiveComplexity
        private async Task RunAsync()
        {
            var reader = _queue.Reader;
            TMessage? firstMessageAfterWindow = null;
            try
            {
                while (!_stopToken.IsCancellationRequested)
                {
                    TMessage firstMessageInWindow;
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
                        await ChooseMessageToForwardAsync(firstMessageInWindow, reader);

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
        // ReSharper restore CognitiveComplexity
    }
}
