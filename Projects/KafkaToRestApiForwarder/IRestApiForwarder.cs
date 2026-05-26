namespace KafkaToRestApiForwarder;

public interface IRestApiForwarder
{
    Task<ForwardingResult> ForwardAsync(ForwardingMessage message, CancellationToken cancellationToken);
}
