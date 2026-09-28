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
    private readonly TimeProvider _time;

    public LiveStreamEntry(
        StreamHub hub,
        Harvester harvester,
        ILogger logger,
        bool manual,
        string? manualUrl,
        TimeProvider? time = null)
    {
        _logger = logger;
        _time = time ?? TimeProvider.System;
        _sampledAt = _time.GetTimestamp();
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
    /// an unchanged configuration compares equal and nothing is torn down - unless the publisher
    /// has stopped, in which case the next beat starts it again. A changed one replaces
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
            // The completed check is not redundant with the comparison beside it. The publisher
            // catches everything and logs, so that a configuration the encoder refuses costs the
            // stream its metadata rather than its life - but on the comparison alone that cost is
            // permanent, because the configuration has not changed and never will. Not a
            // reconnect, not a re-adopted layout, nothing restarts it. A completed task with a
            // sensor still configured is a publisher that has stopped and should not have, so it
            // is built again.
            if (_sensor == sensor && _synthesised is not { IsCompleted: true })
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

    private int _misdeclarationReported;

    /// <summary>
    /// True the first time it is asked on this connection, false ever after: the guard behind the
    /// one line that says this stream is sending faster than it declared.
    ///
    /// Once per stream per connection is the whole design of it. That a sender is misdeclaring is
    /// worth an operator's attention - an H.264 encoder's VUI timing is wrong in the wild by
    /// accident often enough that this is a real diagnosis rather than an accusation - but the
    /// comparison is made for every viewer that attaches and every rollback that is resolved, so a
    /// line per occurrence would be a line per viewer per reconnect on the busiest path here.
    /// Reset in <see cref="TakeOverAsync"/> because a reconnect is a new encoder session and may
    /// well declare something else.
    /// </summary>
    public bool ReportsMisdeclaredRate() => Interlocked.Exchange(ref _misdeclarationReported, 1) == 0;

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
    /// The shortest window a rate is taken over. <see cref="TakeFeed"/> is called once a beat by
    /// the heartbeat, and <see cref="LiveStreamCoordinator.Beat"/> is two seconds, so an ordinary
    /// sample clears this comfortably. What it exists for is the other callers: a claim and a
    /// manual creation both describe a stream too, and either can land a fraction of a second
    /// after a beat. A handful of packets over a hundredth of a second is not a rate, and read as
    /// one it would be hundreds a second on a stream sending twenty-five.
    /// </summary>
    private static readonly TimeSpan ShortestSample = TimeSpan.FromSeconds(1);

    /// <summary>The hub's packet total when the rate below was last taken, with its timestamp.</summary>
    private long _sampledPackets;
    private long _sampledAt;

    private double _observedPacketsPerSecond;

    /// <summary>
    /// What has actually been arriving, in packets a second, over the last window of at least
    /// <see cref="ShortestSample"/>; zero before the first window closes.
    ///
    /// Arrivals only, and that is the part that has to be carried by whoever reads it.
    /// <see cref="StreamHub.PublishAtLiveEdge"/> does not touch <see cref="StreamHub.Packets"/> -
    /// so that a packet this replica produced cannot move <c>LastPacketAt</c> and keep a dead
    /// camera alive - so this figure omits every synthetic track by construction.
    /// <see cref="StreamLayout.SyntheticPacketsPerSecond"/> is what adds them back, and
    /// <c>LiveStreamCoordinator</c>'s effective rate is where the two are put together.
    ///
    /// Zero rather than null before the first sample, and zero again on a stream whose feed has
    /// stopped, because the only consumer takes the larger of this and the declaration: a stream
    /// that has not been measured yet, or is no longer arriving, falls back to what the sender
    /// said without any of the three having to ask which case it is in.
    /// </summary>
    public double ObservedPacketsPerSecond => Volatile.Read(ref _observedPacketsPerSecond);

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
    ///
    /// It is also where <see cref="ObservedPacketsPerSecond"/> is taken, because this is already
    /// the one place a packet total is read on a schedule. That sample keeps a window of its own
    /// rather than reusing the meter's, for the reason given where it is taken.
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

            // Measured against its own mark rather than against the meter's, and that is not
            // fussiness. The meter's interval ends wherever the last caller left it, so a claim a
            // tenth of a second after a beat leaves the next beat differencing nine tenths of a
            // second of packets over an interval this would have called a whole one - a rate about
            // a tenth low, on the one figure that exists because a low rate cannot be detected.
            // Keeping the window's own start here means the sample is exact whoever else called.
            var elapsed = _time.GetElapsedTime(_sampledAt);

            if (elapsed >= ShortestSample)
            {
                Volatile.Write(ref _observedPacketsPerSecond, (packets - _sampledPackets) / elapsed.TotalSeconds);

                _sampledPackets = packets;
                _sampledAt = _time.GetTimestamp();
            }

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

            // A new connection is a new encoder session, which may declare a different rate or the
            // same one truthfully, so it gets its own chance to be reported. See
            // ReportsMisdeclaredRate.
            Interlocked.Exchange(ref _misdeclarationReported, 0);
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
