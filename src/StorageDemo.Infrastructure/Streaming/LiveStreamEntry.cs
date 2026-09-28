using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One stream this replica owns: its hub, the decoder and harvester behind its preview, whatever
/// connection is currently feeding it, and any recording that is running.
///
/// The hub outlives the connection, which is the whole point of the interrupted state. A feed that
/// stops leaves everything here alive for the grace period, and a reconnect under the same name
/// attaches a new demultiplexer to this same hub rather than creating a second stream.
/// </summary>
public sealed class LiveStreamEntry : IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly ILogger _logger;

    public LiveStreamEntry(StreamHub hub, Harvester harvester, ILogger logger, bool manual, string? manualUrl)
    {
        _logger = logger;
        Hub = hub;
        Harvester = harvester;
        Decoder = new FrameDecoder(hub, logger);
        Manual = manual;
        ManualUrl = manualUrl;

        PreviewSubscription = SubscribePreview(Decoder, harvester);
        Decoding = Decoder.RunAsync(Lifetime.Token);

        Klv = new KlvExtractor(hub, logger);
        Extracting = Klv.RunAsync(Lifetime.Token);
    }

    /// <summary>The harvester's handler takes a libav frame, so binding it needs an unsafe context.</summary>
    private static unsafe IDisposable SubscribePreview(FrameDecoder decoder, Harvester harvester)
        => decoder.Subscribe(DecodeRate.Keyframes, harvester.OnFrame);

    public StreamHub Hub { get; }

    public string Name => Hub.Name;

    public Harvester Harvester { get; }

    public FrameDecoder Decoder { get; }

    /// <summary>Ends the stream: the decoder, the current feed and any recording.</summary>
    public CancellationTokenSource Lifetime { get; } = new();

    public Task Decoding { get; }

    /// <summary>The always-attached packet subscriber on the KLV index, the metadata twin of the harvester.</summary>
    public KlvExtractor Klv { get; }

    public Task Extracting { get; }

    /// <summary>The detection toggle, the worker's lease and the VMTI ring, the carriage half of detection.</summary>
    public StreamDetection Detection { get; } = new();

    private StaticSensor? _sensor;
    private CancellationTokenSource? _synthesising;
    private Task? _synthesised;

    /// <summary>
    /// Starts, replaces or stops the publisher that synthesises this stream's ST 0601, to match the
    /// configuration as it is now.
    ///
    /// Idempotent, which is what lets the heartbeat call it every beat: the sensor is a record, so
    /// an unchanged configuration compares equal and nothing is torn down. A changed one replaces
    /// the publisher rather than editing it, because the sets it writes are built from the sensor
    /// it was given and a half-applied change would be a stream reporting one position and one
    /// bearing from different configurations.
    ///
    /// Only the packets are started and stopped here. The track they go on is part of the layout
    /// and cannot appear under a running feed - see <see cref="StreamHub.Synthesise"/> - so a newly
    /// configured camera publishes nothing until its stream reconnects, and this call is harmless
    /// in the meantime.
    /// </summary>
    public async Task SynthesiseAsync(StaticSensor? sensor)
    {
        CancellationTokenSource? previous;
        Task? previousTask;

        lock (_gate)
        {
            if (_sensor == sensor)
            {
                return;
            }

            previous = _synthesising;
            previousTask = _synthesised;

            _sensor = sensor;
            _synthesising = sensor is null ? null : CancellationTokenSource.CreateLinkedTokenSource(Lifetime.Token);
            _synthesised = _synthesising is null
                ? null
                : new StaticSensorPublisher(Hub, sensor!, _logger).RunAsync(_synthesising.Token);
        }

        if (previous is not null)
        {
            await previous.CancelAsync();
        }

        if (previousTask is not null)
        {
            // Bounded: the publisher's only await is on its own timer, which the cancellation
            // above completes at once, so this returns as soon as the tick in flight finishes -
            // and that tick is an encode of about a hundred bytes and one turn of the hub's lock.
            await previousTask;
        }

        previous?.Dispose();
    }

    private IDisposable PreviewSubscription { get; }

    /// <summary>
    /// When the stream began, which is not when this entry was built: a stream that moves to
    /// another replica is the same stream resuming, and its start time has to move with it or the
    /// name is the only thing that survived. Set from the registry by <c>Resumes</c> under the
    /// claim lock, before anything reads it.
    /// </summary>
    public DateTimeOffset StartedAt { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>Adopts the start time and the detection state of the stream this entry is taking over.</summary>
    public void Resumes(LiveStream shared)
    {
        StartedAt = shared.StartedAt;
        Detection.Adopt(shared);
    }

    public bool Manual { get; }

    public string? ManualUrl { get; }

    /// <summary>Tells one connection attempt from the next in a log. Nothing looks a stream up by it.</summary>
    public string? ConnectionId { get; private set; }

    /// <summary>Cancels only the current connection, leaving the hub and the recording alive.</summary>
    public CancellationTokenSource? Feed { get; private set; }

    public Task? Feeding { get; private set; }

    /// <summary>
    /// The socket the current feed is arriving on, so the heartbeat can ask libsrt how that
    /// connection is actually doing rather than inferring it from a byte count. Null for a pulled
    /// stream, which libav dials and which therefore has no socket this service holds.
    /// </summary>
    public SrtSocketStream? Transport { get; private set; }

    /// <summary>
    /// The forwards running for this stream, by forward id.
    ///
    /// Here, on the entry, and not in a structure of their own. A forward only exists where the
    /// bytes are, so hanging it off the stream's local entry makes it follow the stream's owner for
    /// free: a name that moves to another pod is claimed there and reconciled there, and the pod
    /// that lost it tears its copies down in <see cref="DisposeAsync"/>. No forward lease, no second
    /// thing to keep honest, and no window where two pods are pushing the same stream to the same
    /// far end - which is the failure a separate lease would have been invented to prevent.
    /// </summary>
    public ConcurrentDictionary<string, StreamForwarder> Forwards { get; } = new(StringComparer.Ordinal);

    public StreamRecorder? Recorder { get; private set; }

    public Task<Guid?>? Recording { get; private set; }

    /// <summary>Set when this replica has been displaced and is standing down.</summary>
    public bool StandingDown { get; set; }

    public bool FeedRunning => Feeding is { IsCompleted: false };

    private int _viewers;

    /// <summary>
    /// How many players are pulling this stream right now, on whichever port and however they
    /// reached this replica - directly on the consumption port, or relayed here from a replica
    /// that does not own the stream, since a relay ends up calling the same <c>Serve</c> that a
    /// direct connection does.
    ///
    /// Counted here rather than read off <see cref="StreamHub"/>'s subscriber list, which also
    /// holds the recorder, the harvester, the KLV extractor and every forward: a wall of a
    /// thousand tiles asking "who is actually watching this" wants none of those, and teaching the
    /// hub to tell them apart would be a second concept for one number. <see cref="ViewerJoined"/>
    /// and <see cref="ViewerLeft"/> are the only two callers, one per connection's lifetime.
    /// </summary>
    public int Viewers => Volatile.Read(ref _viewers);

    public void ViewerJoined() => Interlocked.Increment(ref _viewers);

    public void ViewerLeft() => Interlocked.Decrement(ref _viewers);

    private long _meteredPackets;
    private long _meteredBytes;
    private long _meteredKlv;
    private long _meteredKlvRejected;

    /// <summary>
    /// What this stream has carried since the meter last asked, and remembers that it was asked.
    ///
    /// The hub and the extractor keep running totals because that is what a reconnect can carry
    /// across; a counter wants the interval. Taking the difference here rather than in
    /// <see cref="LiveMetrics"/> is what makes the meter's figures exact however often the
    /// heartbeat describes a stream: the second call in one beat reports nothing because nothing
    /// arrived between them.
    ///
    /// Under the entry's own lock, because the heartbeat is not the only thread that describes a
    /// stream - a claim and a manual creation both do - and two of them reading the same totals
    /// would count an interval twice.
    /// </summary>
    /// <param name="lost">Packets libsrt reported missing over its own interval, which it has already cleared.</param>
    /// <param name="dropped">Packets that arrived too late, over that same interval.</param>
    public StreamFeed TakeFeed(int lost, int dropped)
    {
        lock (_gate)
        {
            var packets = Hub.Packets;
            var bytes = Hub.Bytes;
            var klv = Klv.Packets;
            var rejected = Klv.Rejected;

            var feed = new StreamFeed(
                packets - _meteredPackets,
                bytes - _meteredBytes,
                klv - _meteredKlv,
                rejected - _meteredKlvRejected,
                lost,
                dropped);

            _meteredPackets = packets;
            _meteredBytes = bytes;
            _meteredKlv = klv;
            _meteredKlvRejected = rejected;

            return feed;
        }
    }

    /// <summary>
    /// Hands the entry a new connection, cancelling whatever was feeding it.
    ///
    /// The cancellation is a guard rather than a take-over. A live name is locked now, so a second
    /// connection only reaches here once the feed it replaces has already stopped and there is
    /// nothing left to cancel. It stays because the alternative to a no-op cancel is two feeds
    /// writing into one hub, and that would be discovered as corrupted output.
    /// </summary>
    public async Task<CancellationToken> TakeOverAsync(string connectionId)
    {
        CancellationTokenSource? previous;
        Task? previousTask;

        lock (_gate)
        {
            previous = Feed;
            previousTask = Feeding;

            Feed = CancellationTokenSource.CreateLinkedTokenSource(Lifetime.Token);
            ConnectionId = connectionId;
        }

        if (previous is not null)
        {
            await previous.CancelAsync();
        }

        if (previousTask is not null)
        {
            // Bounded: the old demultiplexer notices cancellation between reads, so it can be
            // waiting out a socket timeout. Nothing here depends on it having finished.
            await Task.WhenAny(previousTask, Task.Delay(TimeSpan.FromSeconds(10)));
        }

        previous?.Dispose();

        lock (_gate)
        {
            return Feed!.Token;
        }
    }

    public void Feeds(Task feeding, SrtSocketStream? transport = null)
    {
        lock (_gate)
        {
            Feeding = feeding;
            Transport = transport;
        }
    }

    public void Records(StreamRecorder recorder, Task<Guid?> running)
    {
        lock (_gate)
        {
            Recorder = recorder;
            Recording = running;
        }
    }

    /// <summary>Forgets a finished recording, so the next trigger starts a new one.</summary>
    public void RecordingEnded()
    {
        lock (_gate)
        {
            Recorder = null;
            Recording = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        Recorder?.Stop();

        // Before the lifetime is cancelled, so a far end is told the copy has ended by a trailer
        // rather than by a socket going quiet. Not waited for: a forward is a copy, and nothing
        // downstream of this service is owed a tidy close at the cost of holding up a failover.
        foreach (var forwarder in Forwards.Values)
        {
            forwarder.Dispose();
        }

        Forwards.Clear();

        await Lifetime.CancelAsync();

        if (Feeding is not null)
        {
            await Task.WhenAny(Feeding, Task.Delay(TimeSpan.FromSeconds(10)));
        }

        // The recording is awaited rather than abandoned: it still has a file to upload, and that
        // is what turns it into the document somebody asked for.
        if (Recording is not null)
        {
            await Task.WhenAny(Recording, Task.Delay(TimeSpan.FromSeconds(60)));
        }

        Hub.Close();

        await Task.WhenAny(Task.WhenAll(Decoding, Extracting), Task.Delay(TimeSpan.FromSeconds(10)));

        if (_synthesised is not null)
        {
            // Already cancelled with the lifetime above; awaited so the token source below is not
            // disposed underneath the tick still holding it.
            await Task.WhenAny(_synthesised, Task.Delay(TimeSpan.FromSeconds(10)));
        }

        _synthesising?.Dispose();

        PreviewSubscription.Dispose();
        Decoder.Dispose();
        Feed?.Dispose();
        Lifetime.Dispose();
        Hub.Dispose();
    }
}
