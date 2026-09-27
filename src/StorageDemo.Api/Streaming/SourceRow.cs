using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Streaming;

/// <summary>What an operator sees in the state column, which is three answers rather than two.</summary>
public enum SourceState
{
    NotOnAir,
    Live,
    Interrupted,
}

/// <summary>
/// One line of the sources table: the configuration, whatever is on air under its name, and the few
/// figures derived from the pair of them.
/// </summary>
/// <param name="LocalOutput">
/// The SRT address a player pulls this feed from, which is the answer to the question an operator
/// asks most often.
/// </param>
/// <param name="BitsPerSecond">
/// Null when no honest figure exists yet - the first refresh has nothing to subtract from, and a
/// counter that went backwards means the stream restarted rather than that throughput was negative.
/// </param>
public sealed record SourceRow(
    LiveSource Source,
    LiveStream? Stream,
    SourceState State,
    string LocalOutput,
    double? BitsPerSecond)
{
    private IReadOnlyList<ForwardStatus> Statuses => Stream?.Forwards ?? [];

    public int ForwardsConfigured => Source.Forwards.Count;

    public int ForwardsConnected => Statuses.Count(status => status.Connected);

    /// <summary>
    /// Every configured forward beside what it is actually doing, status null when the owning
    /// replica is reporting nothing for it.
    /// </summary>
    public IEnumerable<(ForwardTarget Target, ForwardStatus? Status)> Detail
        => Source.Forwards.Select(target =>
            (target, Statuses.FirstOrDefault(status => status.Id == target.Id)));
}

/// <summary>
/// The page's arithmetic, kept out of the component so it can be exercised without a renderer.
/// </summary>
public static class StreamingRows
{
    /// <summary>Where a player pulls this source from this service.</summary>
    public static string LocalOutput(LiveOptions options, string? host, string name)
    {
        var address = string.IsNullOrWhiteSpace(options.PublicConsumptionUrl)
            ? $"srt://{(string.IsNullOrWhiteSpace(host) ? "localhost" : host)}:{options.ConsumptionPort}"
            : options.PublicConsumptionUrl.TrimEnd('/');

        return $"{address}?streamid={Uri.EscapeDataString(name)}";
    }

    /// <summary>
    /// The same scheme rule the coordinator applies before it opens anything, run in the browser so
    /// a typo costs a red line under the field rather than a round trip and a stack trace.
    /// </summary>
    public static bool SchemeAllowed(string? url, IReadOnlyList<string> allowed)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            return false;
        }

        var separator = url.IndexOf("://", StringComparison.Ordinal);

        // No scheme at all is "file" here for the same reason it is in the coordinator: an
        // unqualified path is one libav would happily read off the disk.
        var scheme = separator > 0 ? url[..separator] : "file";

        return allowed.Contains(scheme, StringComparer.OrdinalIgnoreCase);
    }

    public static SourceRow Build(
        LiveSource source,
        LiveStream? stream,
        LiveOptions options,
        string? host,
        double? bitsPerSecond = null)
        => new(
            source,
            stream,
            stream is null
                ? SourceState.NotOnAir
                : stream.State == LiveStreamState.Interrupted ? SourceState.Interrupted : SourceState.Live,
            LocalOutput(options, host, source.Name),
            bitsPerSecond);
}

/// <summary>
/// Turns the stream's running byte total into a rate, by remembering what it was last time.
/// </summary>
public sealed class ThroughputMeter
{
    private readonly Dictionary<string, (long Bytes, DateTimeOffset At)> _last = new(StringComparer.Ordinal);

    /// <summary>Null until there is a previous sample to subtract, and after a counter reset.</summary>
    public double? Sample(string name, long bytes, DateTimeOffset at)
    {
        var known = _last.TryGetValue(name, out var previous);
        _last[name] = (bytes, at);

        if (!known)
        {
            return null;
        }

        var seconds = (at - previous.At).TotalSeconds;

        // A total that went backwards is a stream that ended and came back under the same name,
        // which is the ordinary case for a reconnecting encoder.
        return seconds > 0 && bytes >= previous.Bytes
            ? (bytes - previous.Bytes) * 8 / seconds
            : null;
    }
}
