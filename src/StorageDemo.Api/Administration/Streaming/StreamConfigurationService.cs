using Microsoft.Extensions.Options;
using StorageDemo.Api.Controllers;
using StorageDemo.Api.Streaming;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Administration.Streaming;

/// <summary>The operator-facing stream configuration boundary.</summary>
public interface IStreamConfigurationService
{
    bool Enabled { get; }

    bool RequiresToken { get; }

    bool Authorize(string token);

    Task<IReadOnlyList<SourceRow>> ListAsync(string? browserHost, CancellationToken cancellationToken);

    Task<StreamCommandResult> SaveAsync(SourceEditModel edit, CancellationToken cancellationToken);

    Task<StreamCommandResult> DeleteAsync(string name, CancellationToken cancellationToken);

    Task<StreamDetectionResult> ConfigureDetectionAsync(
        string name,
        bool enabled,
        int rate,
        string? model,
        IReadOnlyList<string> labels,
        CancellationToken cancellationToken);
}

public sealed record StreamCommandResult(bool Succeeded, string? Error = null)
{
    public static StreamCommandResult Success() => new(true);

    public static StreamCommandResult Failure(string error) => new(false, error);
}

public sealed record StreamDetectionResult(bool Succeeded, LiveStream? Stream = null, string? Error = null);

public sealed class SourceEditModel
{
    public string Name { get; set; } = string.Empty;

    public bool IsPull { get; set; } = true;

    public string Url { get; set; } = string.Empty;

    public bool Enabled { get; set; } = true;

    public List<ForwardEditModel> Forwards { get; set; } = [];

    public static SourceEditModel From(LiveSource? source)
        => source is null
            ? new SourceEditModel()
            : new SourceEditModel
            {
                Name = source.Name,
                IsPull = source.IsPull,
                Url = source.Url ?? string.Empty,
                Enabled = source.Enabled,
                Forwards = [.. source.Forwards.Select(ForwardEditModel.From)],
            };
}

public sealed class ForwardEditModel
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");

    public string Url { get; set; } = string.Empty;

    public bool Enabled { get; set; } = true;

    public static ForwardEditModel From(ForwardTarget target)
        => new() { Id = target.Id, Url = target.Url, Enabled = target.Enabled };
}

public sealed class StreamConfigurationService(
    ILiveSourceStore sources,
    ILiveStreamService live,
    LivePeerProxy peers,
    IOptions<LiveOptions> options) : IStreamConfigurationService
{
    private readonly LiveOptions _options = options.Value;
    private readonly ThroughputMeter _meter = new();

    public bool Enabled => _options.Enabled;

    public bool RequiresToken => !string.IsNullOrEmpty(_options.Token);

    public bool Authorize(string token)
    {
        if (!RequiresToken)
        {
            return true;
        }

        var provided = System.Text.Encoding.UTF8.GetBytes(token);
        var expected = System.Text.Encoding.UTF8.GetBytes(_options.Token!);

        return System.Security.Cryptography.CryptographicOperations.FixedTimeEquals(provided, expected);
    }

    public async Task<IReadOnlyList<SourceRow>> ListAsync(
        string? browserHost,
        CancellationToken cancellationToken)
    {
        var configured = await sources.ListAsync(cancellationToken);
        var streams = (await live.StreamsAsync(cancellationToken))
            .ToDictionary(stream => stream.Name, StringComparer.Ordinal);
        var sampledAt = DateTimeOffset.UtcNow;

        return [.. configured
            .OrderBy(source => source.Name, StringComparer.OrdinalIgnoreCase)
            .Select(source =>
            {
                var stream = streams.GetValueOrDefault(source.Name);

                return StreamingRows.Build(
                    source,
                    stream,
                    _options,
                    browserHost,
                    stream is null ? null : _meter.Sample(source.Name, stream.Bytes, sampledAt));
            })];
    }

    public async Task<StreamCommandResult> SaveAsync(
        SourceEditModel edit,
        CancellationToken cancellationToken)
    {
        var name = edit.Name.Trim();
        var url = edit.Url.Trim();

        if (!StreamName.TryParse(name, out var parsed, out var rejection) || parsed != name)
        {
            return StreamCommandResult.Failure(
                rejection.Length > 0 ? rejection : "That is not a usable stream name.");
        }

        var source = new LiveSource(
            name,
            edit.IsPull ? url : null,
            edit.Enabled,
            [.. edit.Forwards.Select(forward => new ForwardTarget(
                string.IsNullOrWhiteSpace(forward.Id) ? Guid.NewGuid().ToString("N") : forward.Id,
                forward.Url.Trim(),
                forward.Enabled))],
            DateTimeOffset.UtcNow);

        if (LiveSourceRules.Refuse(source, _options.AllowedSchemes) is { } refused)
        {
            return StreamCommandResult.Failure(refused);
        }

        try
        {
            await sources.SaveAsync(source, cancellationToken);
            return StreamCommandResult.Success();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return StreamCommandResult.Failure($"Not saved: {ex.Message}");
        }
    }

    public async Task<StreamCommandResult> DeleteAsync(
        string name,
        CancellationToken cancellationToken)
    {
        try
        {
            await sources.RemoveAsync(name, cancellationToken);
            return StreamCommandResult.Success();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return StreamCommandResult.Failure($"Not deleted: {ex.Message}");
        }
    }

    public async Task<StreamDetectionResult> ConfigureDetectionAsync(
        string name,
        bool enabled,
        int rate,
        string? model,
        IReadOnlyList<string> labels,
        CancellationToken cancellationToken)
    {
        try
        {
            model = DetectionModels.Normalize(model);
            labels = CocoClasses.Normalize(labels);
            var known = await live.GetAsync(name, cancellationToken);
            if (known is null)
            {
                return new(false, Error: "The stream is no longer on air.");
            }

            var stream = live.Owns(name)
                ? await live.SetDetectionAsync(name, enabled, rate, model, labels, cancellationToken)
                : await peers.FetchAsync<LiveStream>(
                    known,
                    $"api/live/detect/{name}",
                    _options.Token,
                    cancellationToken,
                    HttpMethod.Put,
                    new DetectRequest(enabled, rate, model, labels));

            return stream is null
                ? new(false, Error: "The stream owner did not accept the change.")
                : new(true, stream);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new(false, Error: ex.Message);
        }
    }
}
