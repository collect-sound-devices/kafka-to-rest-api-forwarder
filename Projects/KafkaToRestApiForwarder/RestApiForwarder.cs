using System.Diagnostics.CodeAnalysis;
using System.Text;
using Microsoft.Extensions.Options;

namespace KafkaToRestApiForwarder;

[SuppressMessage("Performance", "CA1873:Avoid potentially expensive logging")]
public sealed class RestApiForwarder : IRestApiForwarder
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
    private readonly GitHubCodespaceAwaker _codespaceAwaker;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<RestApiForwarder> _logger;

    public RestApiForwarder(
        IOptions<ApiBaseUrlSettings> apiSettings,
        GitHubCodespaceAwaker codespaceAwaker,
        IHttpClientFactory httpClientFactory,
        ILogger<RestApiForwarder> logger)
    {
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

        _logger.LogInformation("REST API forwarder initialized. Target REST API \"{ApiTarget}\"", _apiTarget);
    }

    public async Task<ForwardingResult> ForwardAsync(
        ForwardingMessage message,
        CancellationToken cancellationToken)
    {
        if (message.UrlSuffix == null)
        {
            return new ForwardingResult(false, "urlSuffix is null");
        }

        if (string.IsNullOrWhiteSpace(message.HttpMethod))
        {
            return new ForwardingResult(false, "httpMethod is null or empty");
        }

        try
        {
            using var httpClient = _httpClientFactory.CreateClient();
            using var jsonContent = new StringContent(message.Payload.ToJsonString(), Encoding.UTF8, "application/json");

            var response = message.HttpMethod.ToUpperInvariant() == "PUT"
                ? await httpClient.PutAsync(_apiEndpoint + message.UrlSuffix, jsonContent, cancellationToken)
                : await httpClient.PostAsync(_apiEndpoint + message.UrlSuffix, jsonContent, cancellationToken);

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

            return new ForwardingResult(false, $"Exception: {ex.Message}");
        }

        return new ForwardingResult(true, null);
    }
}
