using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One forward, as the reconcile pass needs to see it: where it is pointed, whether it has stopped,
/// and when it last tried. Enough to decide, and nothing that would make the decision need a thread
/// or a socket - which is what lets <see cref="ForwardPlan"/> be tested without either.
/// </summary>
public readonly record struct RunningForward(string Url, bool Finished, DateTimeOffset LastAttemptAt);

/// <summary>
/// What has to change for the forwards of one stream to match what was configured.
///
/// Pulled out of the heartbeat as a pure function because this is the only part of forwarding with
/// interesting behaviour and the least testable surroundings: the alternative is proving "a
/// disabled forward stops" by standing up a hub, a socket and a far end.
/// </summary>
public static class ForwardPlan
{
    /// <param name="desired">
    /// The forwards that should be running, already filtered: enabled targets of an enabled source,
    /// and empty when the source is off or absent. Filtering here instead would mean this function
    /// had to know that a disabled source silences its forwards, which is the store's rule.
    /// </param>
    /// <param name="running">What is actually running, by forward id.</param>
    /// <param name="retry">How long a failed forward waits before it is tried again.</param>
    /// <returns>
    /// What to start and what to stop. A forward that has been redialled or has failed and is due
    /// appears in both, in that order: there is one socket per forward id, so the old one goes
    /// before the new one opens.
    /// </returns>
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
                // Removed, switched off, or its source was switched off. All three are the operator
                // saying stop, and none of them is distinguishable from here or needs to be.
                stop.Add(id);

                continue;
            }

            if (!string.Equals(wanted.Url, actual.Url, StringComparison.Ordinal))
            {
                // Edited in place. The id is stable across edits precisely so this is a redial
                // rather than a forward disappearing and an unrelated one appearing.
                stop.Add(id);
                start.Add(wanted);

                continue;
            }

            // A failed forward is retried here and nowhere else. The forwarder deliberately does
            // not retry itself: two places deciding whether a forward should be running is how you
            // get one that reconnects after the operator disabled it.
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

/// <summary>
/// A live copy of one stream, pushed to somewhere else.
///
/// A cousin of <see cref="StreamRecorder"/> and much the smaller one, because nothing is stored and
/// nothing becomes a document: it subscribes to a hub, muxes into a URL, and reports what happened.
/// Everything that makes a recording complicated - parts, uploads, a document that has to be
/// truthful about what it contains - has no meaning for bytes leaving the building.
///
/// It never retries. The reconcile pass in <see cref="LiveStreamCoordinator"/> owns that, so there
/// is exactly one place that decides whether a forward should be running; <see cref="Finished"/>
/// and <see cref="LastAttemptAt"/> exist for it to decide with.
/// </summary>
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

    /// <summary>Set once this attempt is over, however it ended. The reconcile pass retries it.</summary>
    public bool Finished { get; private set; }

    public DateTimeOffset LastAttemptAt { get; private set; }

    public Task? Running { get; private set; }

    /// <summary>
    /// Read once per heartbeat by the reconcile pass, same as a source's own <c>Describe</c> reads
    /// <c>Transport.Health</c> - which matters here for the same reason it does there: libsrt
    /// resets the interval counters <see cref="SrtSocketStream.SendHealth"/> reads, so a second
    /// call in the same beat would see the remainder of this one's window. Null for a UDP or RTP
    /// target, which opened through <see cref="AvioWriter"/> and has no socket here to ask.
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

    /// <summary>
    /// Runs the forward on a thread of its own until it is stopped or it fails.
    ///
    /// Long-running and genuinely dedicated, like the feed and the recorder, because opening the
    /// transport blocks: a listening SRT forward waits for somebody to pull it and may wait forever,
    /// and a caller waits out a handshake against a far end that may be down. None of that may
    /// happen on a pool thread.
    /// </summary>
    /// <param name="lifetime">The stream's lifetime, so a stream that ends takes its forwards with it.</param>
    public void Start(CancellationToken lifetime)
    {
        LastAttemptAt = DateTimeOffset.UtcNow;

        Running = Task.Factory.StartNew(() => Run(lifetime), TaskCreationOptions.LongRunning);
    }

    /// <summary>
    /// Records that this forward was never attempted, and why, without starting a thread.
    ///
    /// For a URL the allowlist refuses. It is written onto the forward rather than only logged
    /// because the operator who typed the URL is the one who has to see why nothing is arriving,
    /// and a forward that is simply not running looks exactly like one nobody asked for.
    /// </summary>
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

            // SkipToLive, and deliberately not Fail. A forward is a live copy, so falling behind
            // costs a skip exactly as it does for a viewer. The recorder fails instead because a
            // hole in a file that claims to be a recording is worse than no recording; a forward
            // makes no such promise to anybody, and the far end is watching rather than archiving.
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
            // UDP and RTP have no such statistics to gain either way, so they keep the one
            // unchanged path that already serves every protocol libav can write.
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

            // The hub closed under it. Recorded like a failure because to an operator watching a
            // row go quiet the difference is invisible, and silence explains nothing.
            Error ??= "the stream ended";
        }
        catch (OperationCanceledException)
        {
            // Stopped, or the stream ended. Neither is worth a word.
        }
        catch (Exception ex)
        {
            Error = ex.Message;

            // Information, not error: a far end that is down is an ordinary condition for a forward
            // and the reconcile pass will keep trying. A thousand streams forwarding to one gateway
            // that restarts would otherwise fill a log with a thousand errors a minute.
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
                    // The far end went away. The muxer swallowed the exception to keep libav's
                    // callback from unwinding through native code, so it is raised again here.
                    throw fault;
                }
            }
        }
    }

    /// <summary>
    /// Which container the scheme wants. Plain <c>rtp</c> in libav carries a single elementary
    /// stream and refuses a transport stream outright, so MPEG-TS over RTP is a muxer of its own;
    /// udp and srt take the same transport stream every other consumer here writes.
    /// </summary>
    private static string Container(string url)
        => url.StartsWith("rtp://", StringComparison.OrdinalIgnoreCase) ? "rtp_mpegts" : "mpegts";

    /// <remarks>
    /// ponytail: cancellation is noticed between packets and never during the open, so stopping a
    /// forward that is still waiting for a listener to be pulled leaves that thread parked until the
    /// process ends. It costs one thread per abandoned listening forward, which nothing in this
    /// deployment creates in numbers. An AVIOInterruptCB on the write context would cut it short,
    /// the same fix <see cref="StreamDemuxer"/> has open for the same reason.
    /// </remarks>
    public void Stop() => _stop.Cancel();

    public void Dispose()
    {
        // Prompt: the thread is signalled and not waited for. A forward blocked in a send to a peer
        // that has stopped reading would otherwise hold up the stream's disposal, and everything it
        // owns is released by its own finally rather than by whoever asked it to stop.
        //
        // The source itself is not disposed, following StreamRecorder: the running thread holds a
        // token linked to it, and disposing it underneath that is a race for no gain.
        try
        {
            Stop();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
