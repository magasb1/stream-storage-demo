using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One forward, as the reconcile pass needs to see it: where it is pointed, whether it has stopped,
/// and when it last tried.
/// </summary>
public readonly record struct RunningForward(string Url, bool Finished, DateTimeOffset LastAttemptAt);

/// <summary>What has to change for the forwards of one stream to match what was configured.</summary>
public static class ForwardPlan
{
    /// <param name="desired">
    /// The forwards that should be running, already filtered: enabled targets of an enabled source,
    /// and empty when the source is off or absent.
    /// </param>
    /// <param name="running">What is actually running, by forward id.</param>
    /// <param name="retry">How long a failed forward waits before it is tried again.</param>
    public static (IReadOnlyList<ForwardTarget> Start, IReadOnlyList<string> Stop) Decide(
        IReadOnlyList<ForwardTarget> desired,
        IReadOnlyDictionary<string, RunningForward> running,
        DateTimeOffset now,
        TimeSpan retry)
    {
        var start = new List<ForwardTarget>();
        var stop = new List<string>();

        foreach (var (id, actual) in running)
        {
            var wanted = desired.FirstOrDefault(target => target.Id == id);

            if (wanted is null)
            {
                stop.Add(id);

                continue;
            }

            if (!string.Equals(wanted.Url, actual.Url, StringComparison.Ordinal))
            {
                stop.Add(id);
                start.Add(wanted);

                continue;
            }

            // A failed forward is retried here and nowhere else.
            if (actual.Finished && now - actual.LastAttemptAt >= retry)
            {
                stop.Add(id);
                start.Add(wanted);
            }
        }

        foreach (var wanted in desired)
        {
            if (!running.ContainsKey(wanted.Id))
            {
                start.Add(wanted);
            }
        }

        return (start, stop);
    }
}

/// <summary>A live copy of one stream, pushed to somewhere else.</summary>
public sealed class StreamForwarder : IDisposable
{
    private readonly StreamHub _hub;
    private readonly LiveOptions _options;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stop = new();

    public StreamForwarder(string id, string url, StreamHub hub, LiveOptions options, ILogger logger)
    {
        Id = id;
        Url = url;
        _hub = hub;
        _options = options;
        _logger = logger;

        // Stamped at construction rather than when the thread gets going, because the retry window
        // is measured from the attempt and a thread that never starts is still an attempt.
        LastAttemptAt = DateTimeOffset.UtcNow;
    }

    private SrtSocketStream? _srt;

    public string Id { get; }

    public string Url { get; }

    public bool Connected { get; private set; }

    /// <summary>Counted at the writer, so it is bytes that left rather than bytes muxed.</summary>
    public long Bytes { get; private set; }

    public DateTimeOffset? ConnectedAt { get; private set; }

    public string? Error { get; private set; }

    /// <summary>Set once this attempt is over, however it ended.</summary>
    public bool Finished { get; private set; }

    public DateTimeOffset LastAttemptAt { get; private set; }

    public Task? Running { get; private set; }

    /// <summary>
    /// Read once per heartbeat by the reconcile pass, same as a source's own <c>Describe</c> reads
    /// <c>Transport.Health</c> - which matters here for the same reason it does there: libsrt
    /// resets the interval counters <see cref="SrtSocketStream.SendHealth"/> reads, so a second
    /// call in the same beat would see the remainder of this one's window.
    /// </summary>
    public ForwardStatus Status
    {
        get
        {
            var health = _srt?.SendHealth();

            return new ForwardStatus(
                Id,
                Url,
                Connected,
                Bytes,
                ConnectedAt,
                Error,
                health?.Lost ?? 0,
                health?.Dropped ?? 0,
                health?.Link);
        }
    }

    /// <summary>Runs the forward on a thread of its own until it is stopped or it fails.</summary>
    public void Start(CancellationToken lifetime)
    {
        LastAttemptAt = DateTimeOffset.UtcNow;

        Running = Task.Factory.StartNew(() => Run(lifetime), TaskCreationOptions.LongRunning);
    }

    /// <summary>Records that this forward was never attempted, and why, without starting a thread.</summary>
    public void Refuse(string why)
    {
        Error = why;
        Finished = true;
    }

    private void Run(CancellationToken lifetime)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token, lifetime);

        try
        {
            var layout = _hub.Layout
                ?? throw new InvalidOperationException(
                    $"'{_hub.Name}' has not been demultiplexed yet, so there is nothing to forward.");

            // SkipToLive, and deliberately not Fail.
            using var subscription = _hub.Subscribe(
                _options.ViewerQueuePackets,
                OverflowPolicy.SkipToLive,
                streamIndexes: [],
                preroll: 0);

            var isSrt = Url.StartsWith("srt://", StringComparison.OrdinalIgnoreCase);

            // SRT dials or waits through the same direct-libsrt stack the ingest port uses, so this
            // forward's own connection can be asked the same questions a push source's already can
            // - bandwidth, round trip time, retransmits - rather than the bytes-only view libav's
            // SRT protocol handler offers, because libav never exposes the socket underneath it.
            using Stream writer = isSrt
                ? _srt = SrtEgress.Open(Url, _options.SrtLatencyMs, linked.Token)
                : new AvioWriter(Url);

            ConnectedAt = DateTimeOffset.UtcNow;
            Connected = true;

            _logger.LogInformation("Forwarding '{Name}' to {Url} as {Forward}", _hub.Name, Url, Id);

            // Disposed before the writer, which the declaration order gives: the trailer has to
            // reach the far end while the transport is still open.
            using var muxer = new PacketMuxer(writer, layout, Container(Url));

            Pump(subscription, muxer, (IWireWriter)writer, linked.Token);

            Error ??= "the stream ended";
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            Error = ex.Message;

            // Information, not error: a far end that is down is an ordinary condition for a forward
            // and the reconcile pass will keep trying.
            _logger.LogInformation(
                "The forward of '{Name}' to {Url} stopped: {Reason}",
                _hub.Name,
                Url,
                ex.Message);
        }
        finally
        {
            Connected = false;
            Finished = true;

            // The transport is already disposed by the using declaration above by the time this
            // runs, so nothing here closes a live socket - it only stops Status asking a dead one
            // for its last read, which SendHealth would answer null for anyway.
            _srt = null;
        }
    }

    /// <summary>
    /// Reads the queue on this thread rather than awaiting it, so the whole forward stays on the
    /// one dedicated thread the blocking open already required.
    /// </summary>
    private void Pump(
        PacketSubscription subscription,
        PacketMuxer muxer,
        IWireWriter writer,
        CancellationToken cancellationToken)
    {
        var packets = subscription.Packets;

        while (packets.WaitToReadAsync(cancellationToken).AsTask().GetAwaiter().GetResult())
        {
            while (packets.TryRead(out var packet))
            {
                muxer.Write(packet);

                Bytes = writer.Written;

                if (muxer.Fault is { } fault)
                {
                    throw fault;
                }
            }
        }
    }

    /// <summary>Which container the scheme wants.</summary>
    private static string Container(string url)
        => url.StartsWith("rtp://", StringComparison.OrdinalIgnoreCase) ? "rtp_mpegts" : "mpegts";

    /// <summary>
    /// ponytail: cancellation is noticed between packets and never during the open, so stopping a
    /// forward that is still waiting for a listener to be pulled leaves that thread parked until
    /// the process ends.
    /// </summary>
    public void Stop() => _stop.Cancel();

    public void Dispose()
    {
        // Prompt: the thread is signalled and not waited for.
        try
        {
            Stop();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
