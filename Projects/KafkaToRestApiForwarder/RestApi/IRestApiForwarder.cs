namespace KafkaToRestApiForwarder.RestApi;

public interface IRestApiForwarder
{
    Task<ForwardingResult> ForwardAsync(ForwardingMessage message, CancellationToken cancellationToken);
}
