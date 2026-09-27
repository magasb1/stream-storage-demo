using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One stream this replica owns: its hub, the decoder and harvester behind its preview, whatever
/// connection is currently feeding it, and any recording that is running.
/// </summary>
public sealed class LiveStreamEntry : IAsyncDisposable
{
    private readonly Lock _gate = new();

    public LiveStreamEntry(StreamHub hub, Harvester harvester, ILogger logger, bool manual, string? manualUrl)
    {
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

    /// <summary>
    /// The always-attached packet subscriber on the KLV index, the metadata twin of the harvester.
    /// </summary>
    public KlvExtractor Klv { get; }

    public Task Extracting { get; }

    /// <summary>
    /// The detection toggle, the worker's lease and the VMTI ring, the carriage half of detection.
    /// </summary>
    public StreamDetection Detection { get; } = new();

    private IDisposable PreviewSubscription { get; }

    /// <summary>
    /// When the stream began, which is not when this entry was built: a stream that moves to
    /// another replica is the same stream resuming, and its start time has to move with it or the
    /// name is the only thing that survived.
    /// </summary>
    public DateTimeOffset StartedAt { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Adopts the start time and the detection state of the stream this entry is taking over.
    /// </summary>
    public void Resumes(LiveStream shared)
    {
        StartedAt = shared.StartedAt;
        Detection.Adopt(shared);
    }

    public bool Manual { get; }

    public string? ManualUrl { get; }

    /// <summary>Tells one connection attempt from the next in a log.</summary>
    public string? ConnectionId { get; private set; }

    /// <summary>Cancels only the current connection, leaving the hub and the recording alive.</summary>
    public CancellationTokenSource? Feed { get; private set; }

    public Task? Feeding { get; private set; }

    /// <summary>
    /// The socket the current feed is arriving on, so the heartbeat can ask libsrt how that
    /// connection is actually doing rather than inferring it from a byte count.
    /// </summary>
    public SrtSocketStream? Transport { get; private set; }

    /// <summary>The forwards running for this stream, by forward id.</summary>
    public ConcurrentDictionary<string, StreamForwarder> Forwards { get; } = new(StringComparer.Ordinal);

    public StreamRecorder? Recorder { get; private set; }

    public Task<Guid?>? Recording { get; private set; }

    /// <summary>Set when this replica has been displaced and is standing down.</summary>
    public bool StandingDown { get; set; }

    public bool FeedRunning => Feeding is { IsCompleted: false };

    private int _viewers;

    /// <summary>
    /// How many players are pulling this stream right now, on whichever port and however they
    /// reached this replica - directly on the consumption port, or relayed here from a replica that
    /// does not own the stream, since a relay ends up calling the same <c>Serve</c> that a direct
    /// connection does.
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
    /// </summary>
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

    /// <summary>Hands the entry a new connection, cancelling whatever was feeding it.</summary>
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
            // waiting out a socket timeout.
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
        // rather than by a socket going quiet.
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

        PreviewSubscription.Dispose();
        Decoder.Dispose();
        Feed?.Dispose();
        Lifetime.Dispose();
        Hub.Dispose();
    }
}
