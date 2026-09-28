using System.Collections.Concurrent;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Coordination;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// Owns every stream this replica holds, and keeps the shared registry telling the truth about
/// them.
///
/// The split that matters: the connection and everything hanging off it are local, because they
/// cannot be anything else, while what a caller asks about is in the registry, because a caller
/// may reach any replica. A request that needs the actual bytes is forwarded to the owner; the
/// rest are answered from the registry by whoever received them.
///
/// A stream never becomes a document by itself. Documents come only from snapshots and recordings
/// someone asked for, which is what makes unattended ingest safe to leave running.
/// </summary>
public sealed class LiveStreamCoordinator(
    StreamDemuxer demuxer,
    ILiveStreamRegistry registry,
    ILiveSourceStore sources,
    IDistributedLock coordination,
    IMediaAnalyzer analyzer,
    IServiceScopeFactory scopeFactory,
    IOptions<LiveOptions> options,
    IOptions<MediaOptions> mediaOptions,
    LiveMetrics metrics,
    ILogger<LiveStreamCoordinator> logger) : ILiveStreamService, IAsyncDisposable
{
    /// <summary>
    /// How often the registry is refreshed and the local streams reconsidered. Short next to the
    /// grace period, so an interruption is noticed well inside it, and it is also the unit
    /// <see cref="LiveStreamStaleness.OwnerAlive"/> counts in.
    /// </summary>
    public static readonly TimeSpan Beat = TimeSpan.FromSeconds(2);

    private readonly ConcurrentDictionary<string, LiveStreamEntry> _local = new(StringComparer.Ordinal);

    /// <summary>
    /// How far past its declaration a stream has to be measured before it is said to be
    /// misdeclaring. Only the log line reads it; the queue is sized from the larger of the two
    /// whatever this says.
    ///
    /// It exists because an observation is a count over a wall-clock window, so an honest stream's
    /// measured rate straddles its declaration and lands above it about half the time. This
    /// repository's own pinned case is the demonstration: 25 fps video beside 48 kHz AAC in
    /// 1024-sample frames declares 71.875 packets a second, and over a two-second beat the stream
    /// delivers 50 video packets and either 93 or 94 audio ones - 71.5 or 72. Strictly greater,
    /// 72 accuses it. Because <see cref="Effective"/> runs on every viewer attach and every
    /// rollback resolved, the once-per-connection flag would be spent on the first high window,
    /// which for any stream with viewers is a near certainty: a line reading as an accusation,
    /// once per connection, for most healthy streams, at a thousand streams a pod. That teaches an
    /// operator to ignore the one line that matters.
    ///
    /// A quarter, because the case this exists for is fifty times the declaration and the band
    /// costs it nothing; anything inside a quarter is a queue a little deeper or shallower in
    /// seconds than asked for, which is what <see cref="StreamLayout.PacketsPerSecond"/> already
    /// says it tolerates.
    /// </summary>
    private const double MisdeclaredBeyond = 1.25;

    /// <summary>
    /// The registry as it looked at the last heartbeat, which is the only form the handshake can
    /// read. See <see cref="AdmitPublisher"/>.
    /// </summary>
    private readonly ConcurrentDictionary<string, LiveStream> _known = new(StringComparer.Ordinal);

    /// <summary>
    /// When this replica last tried to pick up each configured source. A source whose URL the
    /// allowlist refuses, or whose camera is switched off, would otherwise be dialled on every beat
    /// for as long as it stays that way, by every replica at once.
    /// </summary>
    private readonly ConcurrentDictionary<string, DateTimeOffset> _attempted = new(StringComparer.Ordinal);

    private readonly LiveOptions _options = options.Value;
    private readonly CancellationTokenSource _shutdown = new();

    private bool _disposed;

    public string Owner { get; } = options.Value.NodeName is { Length: > 0 } name
        ? name
        : Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName;

    public IReadOnlyList<string> Transports
    {
        get
        {
            var inputs = FfmpegLibrary.InputProtocols();

            return [.. FfmpegLibrary.OutputProtocols().Where(inputs.Contains)];
        }
    }

    private TimeSpan Grace => TimeSpan.FromSeconds(_options.GracePeriodSeconds);

    public async Task<IReadOnlyList<LiveStream>> StreamsAsync(CancellationToken cancellationToken = default)
    {
        var streams = await registry.ListAsync(cancellationToken);

        // A stream whose owner has stopped heartbeating is gone, and after that it leaves no
        // trace: the registry is a picture of what is live now, and the documents a stream
        // produced are what outlives it.
        return [.. streams.Where(stream => !LiveStreamStaleness.IsGone(stream, Grace))];
    }

    public async Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        var stream = await registry.GetAsync(name, cancellationToken);

        return stream is null || LiveStreamStaleness.IsGone(stream, Grace) ? null : stream;
    }

    public bool Owns(string name) => _local.ContainsKey(name);

    public byte[]? Preview(string name)
        => _local.TryGetValue(name, out var entry) ? entry.Harvester.Preview : null;

    public KlvSample? Klv(string name)
        => _local.TryGetValue(name, out var entry) ? entry.Klv.Latest : null;

    public VmtiSample? Detections(string name)
        => _local.TryGetValue(name, out var entry) ? entry.Detection.Latest : null;

    public bool PostDetections(string name, VmtiSample sample)
    {
        if (!_local.TryGetValue(name, out var entry))
        {
            return false;
        }

        entry.Detection.Post(sample);

        metrics.Detected();

        return true;
    }

    public Task<LiveStream?> SetDetectionAsync(
        string name,
        bool enabled,
        int rate,
        string? model = null,
        IReadOnlyList<string>? labels = null,
        CancellationToken cancellationToken = default)
    {
        if (rate is < 0 or > 60)
        {
            throw new ArgumentOutOfRangeException(nameof(rate), "Detection rate must be between 0 and 60 per second.");
        }

        var normalizedModel = DetectionModels.Normalize(model);
        var normalizedLabels = CocoClasses.Normalize(labels);

        return Publish(
            name,
            entry => entry.Detection.Set(enabled, rate, normalizedModel, normalizedLabels),
            cancellationToken);
    }

    public Task<LiveStream?> ClaimDetectorAsync(string name, string worker, CancellationToken cancellationToken = default)
        => Publish(
            name,
            entry =>
            {
                if (!entry.Detection.TryClaim(worker))
                {
                    throw new InvalidOperationException($"'{name}' is held by worker {entry.Detection.Worker}.");
                }
            },
            cancellationToken);

    public async Task<bool> ReleaseDetectorAsync(string name, string worker, CancellationToken cancellationToken = default)
    {
        var released = false;

        await Publish(name, entry => released = entry.Detection.Release(worker), cancellationToken);

        return released;
    }

    /// <summary>
    /// Changes something about a local stream and publishes it at once rather than on the next
    /// beat, because the caller is answered with the stream as it now stands and a worker lists
    /// the registry rather than asking the owner. Null when the stream is not here.
    /// </summary>
    private async Task<LiveStream?> Publish(string name, Action<LiveStreamEntry> change, CancellationToken cancellationToken)
    {
        if (!_local.TryGetValue(name, out var entry))
        {
            return null;
        }

        change(entry);

        var stream = Describe(entry, Interrupted(entry, Silence(entry)) ? LiveStreamState.Interrupted : LiveStreamState.Live);

        await registry.UpsertAsync(stream, cancellationToken);

        return stream;
    }

    /// <summary>
    /// Whether a publisher presenting this name may connect, answered on libsrt's receiver thread.
    ///
    /// A name is held while its owner is alive and its feed is live, and nobody else may publish it.
    /// Free means the feed is interrupted, the owner has stopped heartbeating, or nothing owns the
    /// name at all; a free name is admitted and, if it was interrupted, resumes the same stream.
    ///
    /// Answered from <see cref="_known"/> and not from the registry, because this runs on the thread
    /// carrying every packet for every socket on the ingest port: one Redis round trip here stalls
    /// packet processing for the whole port. The cache is therefore up to one beat stale, which
    /// leaves a window where two replicas each admit the same name. <see cref="ClaimAsync"/> closes
    /// it.
    ///
    /// A full replica also refuses here, which is the other half of the same decision and is why it
    /// is made in one place: both answers are about whether this name may connect right now, both
    /// are read off state only this replica has, and both have to be given before a connection
    /// exists. <see cref="LiveOptions.MaxStreams"/> is the limit, zero meaning none, and a name
    /// already held here is admitted whatever the count - an encoder reconnecting after a blip is a
    /// stream this replica is already responsible for, and refusing it would strand it.
    ///
    /// Only the ingest port asks. A viewer is not a publisher and is never refused by this rule.
    /// </summary>
    public int? AdmitPublisher(Admission admission)
    {
        if (_known.TryGetValue(admission.Name, out var held)
            && held.State == LiveStreamState.Live
            && LiveStreamStaleness.OwnerAlive(held, Beat))
        {
            return Srt.SRT_REJX_CONFLICT;
        }

        // ponytail: a count, for a ceiling that is really a joint budget of sockets and packet
        // rate. A replica holding ten streams at fifteen megabits is past the knee the baseline
        // measured while one holding a hundred and fifty at half a megabit is not, and this cannot
        // tell them apart, so the number has to be set per deployment from the expected bitrate.
        // The honest signal exists - live.udp.receive.errors is the collapse itself, zero on a
        // quiet pod and thirteen thousand a second on a broken one - but it arrives after the pod
        // is already failing, and a handshake has to answer before that. Refuse on the kernel
        // counter's recent trend instead of on a count when something is willing to own a
        // hysteresis rule that does not flap at the knee.
        return _options.MaxStreams > 0
            && _local.Count >= _options.MaxStreams
            && !_local.ContainsKey(admission.Name)
                ? Srt.SRT_REJX_OVERLOAD
                : null;
    }

    /// <summary>
    /// Takes an accepted socket off the accept thread. Everything real happens on another thread,
    /// because every millisecond spent here is a millisecond the ingest port is not listening.
    ///
    /// A <see cref="SrtSocketStream"/> and not yet an <see cref="AvioReader"/>: the reader's
    /// context is freed by the demultiplexer and by nothing else, so it is built only where
    /// <c>demuxer.Run</c> is certain to be called. Everything up to that point carries the stream.
    /// </summary>
    public void OnAccepted(AcceptedSocket socket)
    {
        var name = socket.Name;
        var transport = new SrtSocketStream(socket.Release(), writable: false);
        var connectionId = Guid.NewGuid().ToString("N")[..8];

        _ = Task.Run(() => AttachAsync(name, transport, connectionId));
    }

    private async Task AttachAsync(string name, SrtSocketStream transport, string connectionId)
    {
        LiveStreamEntry? entry = null;

        try
        {
            entry = _local.GetOrAdd(name, Create);

            if (!await ClaimAsync(entry, cancellationToken: CancellationToken.None))
            {
                // The handshake cache said the name was free and the registry says otherwise, which
                // is the one-beat window two replicas can both admit in. Closing the socket is all
                // this side can do: the connection already exists, so there is no rejection code
                // left to send, and the encoder sees a drop and retries.
                logger.LogInformation(
                    "'{Name}' is held elsewhere, so the connection accepted here is being closed",
                    name);

                transport.Dispose();

                await DiscardAsync(entry);

                return;
            }

            // Read before the demultiplexer starts, because the layout is built from the first
            // packet and the hub reads this when it adopts it. One store read per connection, on a
            // store that says of itself it is read-heavy and rarely written. Without it a fixed
            // camera configured while it was away would come back without its metadata track and
            // the heartbeat would have nothing to do but declare it pending forever.
            entry.Hub.Synthesise(Synthetic(await sources.GetAsync(name, CancellationToken.None)));

            var feed = await entry.TakeOverAsync(connectionId);
            var running = entry;

            entry.Feeds(
                Task.Factory.StartNew(() => Feed(running, transport, feed), TaskCreationOptions.LongRunning),
                transport);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Attaching '{Name}' failed", name);

            transport.Dispose();
        }
    }

    /// <summary>
    /// Wraps the socket as a libav transport and demultiplexes it until the feed ends.
    ///
    /// The reader is built here, a line before the call that consumes it, and never earlier. Only
    /// <see cref="StreamDemuxer.Run(AVIOContext*, StreamHub, CancellationToken)"/> frees an
    /// <c>AVIOContext</c>, so one allocated on a path that can still bail - a claim lost while
    /// attaching - would be leaked. Disposing the reader afterwards closes the socket and nothing
    /// else, the context being already gone.
    /// </summary>
    private unsafe void Feed(LiveStreamEntry entry, Stream transport, CancellationToken feed)
    {
        using var reader = new AvioReader(transport);

        var outcome = demuxer.Run(reader.Context, entry.Hub, feed);

        logger.LogInformation(
            "The feed for '{Name}' ({Connection}) ended: {Outcome}",
            entry.Name,
            entry.ConnectionId,
            outcome);
    }

    private LiveStreamEntry Create(string name) => Create(name, manual: false, manualUrl: null);

    private LiveStreamEntry Create(string name, bool manual, string? manualUrl)
    {
        var hub = new StreamHub(name, _options, logger, metrics);

        var harvester = new Harvester(
            mediaOptions.Value.ThumbnailSize,
            mediaOptions.Value.ThumbnailQuality,
            TimeSpan.FromSeconds(_options.PreviewIntervalSeconds));

        logger.LogInformation("Stream '{Name}' is now on air", name);

        return new LiveStreamEntry(hub, harvester, logger, manual, manualUrl);
    }

    /// <summary>
    /// Records this replica as the owner of the name, or refuses because somebody else still holds
    /// it.
    ///
    /// A live name is locked. While the owner is alive and its feed is live nobody else may publish
    /// that name, and this is the authoritative half of that rule: the handshake answers from a
    /// cache that is up to one beat old, so two replicas can both admit the same name, and exactly
    /// one of them gets past here. A name whose feed is interrupted, or whose owner has stopped
    /// heartbeating, is free and is taken - the replica losing it stands down on its next heartbeat.
    ///
    /// The design on record said the opposite, that the newest connection wins, on the grounds that
    /// an encoder actively pushing bytes is more real than a socket that has not noticed its peer is
    /// gone. The repository owner reversed it, and the cost is stated rather than hidden: a dead
    /// pod's names stay held for about three beats, and an encoder that reconnects before its old
    /// socket has timed out is refused until the feed timeout declares the old one interrupted.
    ///
    /// The claim is the owner field of the registry entry, and the distributed lock serialises the
    /// moment of taking it rather than being held for the stream's life. See the map: the design
    /// said the claim was held on the lock and renewed by the heartbeat, and the lock this
    /// repository has can neither be taken over nor renewed. Nothing waits on it.
    /// </summary>
    private async Task<bool> ClaimAsync(LiveStreamEntry entry, CancellationToken cancellationToken)
    {
        var gate = await coordination.TryAcquireAsync(
            $"live-claim:{entry.Name}",
            TimeSpan.FromSeconds(10),
            cancellationToken);

        try
        {
            var existing = await registry.GetAsync(entry.Name, cancellationToken);

            if (existing is not null
                && existing.Owner != Owner
                && existing.State == LiveStreamState.Live
                && LiveStreamStaleness.OwnerAlive(existing, Beat))
            {
                logger.LogInformation(
                    "'{Name}' is live on {Owner}, so it is not taken here",
                    entry.Name,
                    existing.Owner);

                metrics.Claimed("refused");

                return false;
            }

            var resumed = false;

            if (existing is not null && !LiveStreamStaleness.IsGone(existing, Grace))
            {
                resumed = true;

                // The same stream resuming, so it keeps the start time it has always had. Without
                // this a stream that moved replicas looked identical to a new one with the same
                // name: same registry entry, but a start time that jumped to the moment the new
                // owner built its entry. docs/replica-failover.md claims the start time survives a
                // move; on one host it did, because the entry was reused, and across two pods it
                // did not. Observed on k3s; see .scratch/scale-to-1000/cross-pod.md.
                entry.Resumes(existing);

                if (existing.Owner != Owner)
                {
                    logger.LogInformation(
                        "Taking '{Name}' over from {Previous}, which will stand down on its next heartbeat",
                        entry.Name,
                        existing.Owner);
                }
            }

            await registry.UpsertAsync(Describe(entry, LiveStreamState.Live), cancellationToken);

            // A move and a reconnect are both "resumed", which is the distinction that matters to
            // anyone reading this: a rolling update should show a resume for every stream it moved
            // and no new stream at all.
            metrics.Claimed(resumed ? "resumed" : "taken");

            return true;
        }
        finally
        {
            if (gate is not null)
            {
                await gate.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Throws away an entry this replica turned out not to own. The hub and harvester were built by
    /// <see cref="Create"/> before the claim was asked for, and without this they would sit here
    /// unowned and unfed until the heartbeat noticed.
    /// </summary>
    private async Task DiscardAsync(LiveStreamEntry entry)
    {
        if (_local.TryRemove(new KeyValuePair<string, LiveStreamEntry>(entry.Name, entry)))
        {
            await entry.DisposeAsync();
        }
    }

    public async Task<LiveStream> CreateManualAsync(
        string name,
        string url,
        CancellationToken cancellationToken = default)
    {
        var entry = await PullAsync(name, url, cancellationToken);

        if (entry is null)
        {
            // The same lock an encoder meets at the handshake. A pulled stream has no handshake to
            // be refused at, so the refusal is the answer to the request that asked for it - which
            // is exactly why the reconcile pass calls the shared path below rather than this one:
            // losing a claim is a request failing here and an ordinary beat there.
            throw new InvalidOperationException($"'{name}' is already live on another replica.");
        }

        return Describe(entry, LiveStreamState.Live);
    }

    /// <summary>
    /// Claims a name and opens a pulled input on it, for a protocol that cannot name itself.
    ///
    /// Both callers go through here, rather than the reconcile pass calling
    /// <see cref="CreateManualAsync"/>, because they disagree about one thing only and it is the
    /// return type: a person asking for a stream needs to be told the name was taken, and a replica
    /// reconciling a thousand sources against a dozen peers expects to lose most of the time and
    /// must not raise an exception each time it does. Everything else - the name check, the
    /// allowlist, the claim, the demultiplexer on its own thread - is shared, which is the point.
    /// </summary>
    /// <returns>The running entry, or null when another replica holds the name.</returns>
    private async Task<LiveStreamEntry?> PullAsync(string name, string url, CancellationToken cancellationToken)
    {
        if (!StreamName.TryParse(name, out var parsed, out var rejection))
        {
            throw new ArgumentException(rejection, nameof(name));
        }

        RequireAllowed(url);

        // One namespace and one claim. A manual stream is simply one that claimed its name early,
        // and an encoder presenting that name is the same conflict as any other.
        var entry = _local.GetOrAdd(parsed, _ => Create(parsed, manual: true, manualUrl: url));

        if (!await ClaimAsync(entry, cancellationToken))
        {
            await DiscardAsync(entry);

            return null;
        }

        // The pulled twin of the read in AttachAsync, and for the same reason. A manual stream
        // created straight through the endpoint has no configured row at all, which reads as
        // nothing to synthesise.
        entry.Hub.Synthesise(Synthetic(await sources.GetAsync(parsed, cancellationToken)));

        var feed = await entry.TakeOverAsync(Guid.NewGuid().ToString("N")[..8]);
        var running = entry;

        entry.Feeds(Task.Factory.StartNew(
            () => demuxer.Run(url, _options.ManualInputOptions, running.Hub, feed),
            TaskCreationOptions.LongRunning));

        return entry;
    }

    public async Task<Guid?> SnapshotAsync(
        string name,
        DetectionReference? detection = null,
        CancellationToken cancellationToken = default)
    {
        if (!_local.TryGetValue(name, out var entry))
        {
            return null;
        }

        var (bytes, note) = await CaptureAsync(entry, cancellationToken);

        if (bytes is null)
        {
            metrics.Snapshotted("none");

            return null;
        }

        // The note is set only where the harvester's older, smaller picture had to stand in, so it
        // is also the honest answer to "how good was this snapshot".
        metrics.Snapshotted(note is null ? "stored" : "preview");

        var takenAt = DateTimeOffset.UtcNow;

        var metadata = new Dictionary<string, string>
        {
            ["Live stream"] = name,
            ["Captured"] = takenAt.ToString("u"),
        };

        if (note is not null)
        {
            metadata["Snapshot"] = note;
        }

        // A stored picture must not lose the marking the stream carried.
        if (entry.Klv.Classification is { } marking)
        {
            metadata["Classification"] = marking;
        }

        // Provenance, not a second trigger: a detector arrives on this same call. The document
        // then appears on the change feed carrying this, which is the alert, with the evidence
        // attached rather than a message pointing at something that may not exist yet.
        //
        // ponytail: so a detection that captures nothing raises nothing. A bare alert needs a feed
        // of its own; add one when something asks for an alert without evidence.
        if (detection is not null)
        {
            metadata[DetectionReference.MetadataKey] = detection.ToString();
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var documents = scope.ServiceProvider.GetRequiredService<IDocumentService>();

        using var content = new MemoryStream(bytes);

        // Named by stream and wall-clock capture time, matching recordings, so the two sit
        // together and read as related. A live feed has no beginning, so an offset into it would
        // mean nothing to a person.
        var document = await documents.UploadAsync(
            $"{FileName(name)}-{takenAt:yyyyMMdd-HHmmss}.jpg",
            content,
            "image/jpeg",
            cancellationToken,
            metadata);

        logger.LogInformation("Snapshot of '{Name}' stored as {DocumentId}", name, document.Id);

        return document.Id;
    }

    /// <summary>
    /// A snapshot is a fresh decode of the newest segment, not the harvester's frame.
    ///
    /// The harvester decodes keyframes only at the rate a preview needs, so what it holds is stale
    /// by up to a keyframe interval plus the preview cadence: about three seconds for a fine sender
    /// and about twelve for a coarse one. A snapshot is a deliberate act performed once, and one
    /// decode is nothing next to being twelve seconds wrong about the moment somebody meant to
    /// capture.
    ///
    /// When the stream has nowhere to start, the harvester's picture is all there is, and the
    /// document says so rather than quietly being older than it looks.
    /// </summary>
    private async Task<(byte[]? Bytes, string? Note)> CaptureAsync(
        LiveStreamEntry entry,
        CancellationToken cancellationToken)
    {
        var packets = Newest(entry);

        if (packets is null)
        {
            return entry.Harvester.Preview is { } preview
                ? (preview, "Taken from the live preview, because this feed has sent no keyframe "
                    + "recently enough to decode from. It is smaller and older than a snapshot "
                    + "normally is.")
                : (null, null);
        }

        using var container = new MemoryStream();

        Mux(entry, packets, container);

        container.Position = 0;

        return (await analyzer.LatestFrameAsync(container, "snapshot.ts", cancellationToken), null);
    }

    private static MediaPacket[]? Newest(LiveStreamEntry entry)
        => entry.Hub.Layout is null ? null : entry.Hub.NewestStartablePackets();

    private static void Mux(LiveStreamEntry entry, MediaPacket[] packets, Stream destination)
    {
        // ponytail: the layout is read in a second lock acquisition, so a reconnect landing
        // between the copy and this line would mux old packets against a newer layout. It cannot
        // crash - retired layouts are kept alive until the hub is disposed - and the worst case is
        // one malformed snapshot during a reconnect that reconfigured the encoder. Take both under
        // one lock if that ever shows up as a real complaint.
        using var muxer = new PacketMuxer(destination, entry.Hub.Layout!);

        foreach (var packet in packets)
        {
            muxer.Write(packet);
        }

        muxer.Close();
    }

    public Task<RecordingStatus?> RecordAsync(
        string name,
        TimeSpan? duration,
        DetectionReference? detection = null,
        CancellationToken cancellationToken = default)
    {
        if (!_local.TryGetValue(name, out var entry))
        {
            return Task.FromResult<RecordingStatus?>(null);
        }

        // One recording at a time per stream. A trigger arriving while one runs extends its end
        // rather than starting a second, so continuous detection produces one clip covering the
        // whole event instead of a drift of overlapping near-duplicates.
        // ponytail: an extending trigger keeps the first detection's reference, so a document names
        // what started it rather than everything that kept it going. Keep a list on the recorder if
        // naming every detection in one clip ever matters.
        if (entry.Recorder is { Finished: false } running)
        {
            running.Extend(duration);

            return Task.FromResult<RecordingStatus?>(running.Status);
        }

        var recorder = new StreamRecorder(
            entry.Hub,
            _options,
            scopeFactory,
            logger,
            duration,
            () => entry.Klv.Classification,
            detection,
            metrics);

        entry.Records(recorder, recorder.RunAsync(entry.Lifetime.Token));

        logger.LogInformation(
            "Recording '{Name}' as {RecordingId}, due to end at {EndsAt}",
            name,
            recorder.Id,
            recorder.EndsAt);

        // Returns immediately. The recording then runs here and has no further relationship with
        // whoever asked for it: closing the client, losing it, or never having had one changes
        // nothing.
        return Task.FromResult<RecordingStatus?>(recorder.Status);
    }

    public Task<bool> StopRecordingAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_local.TryGetValue(name, out var entry) || entry.Recorder is not { Finished: false } recorder)
        {
            return Task.FromResult(false);
        }

        recorder.Stop();

        return Task.FromResult(true);
    }

    public async Task<bool> StopAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_local.TryRemove(name, out var entry))
        {
            return await GetAsync(name, cancellationToken) is not null;
        }

        await EndAsync(entry, "stopped", "it was stopped");

        return true;
    }

    /// <summary>
    /// What a viewer asking to start this far back would actually get. Answered before a byte is
    /// written, so the response can say so in a header: asking for twenty seconds and receiving
    /// twenty-six is normal rather than an error, and receiving less than was asked for is what a
    /// stream that has not been running that long has always answered.
    /// </summary>
    public double ResolvePreroll(string name, double seconds)
        => _local.TryGetValue(name, out var entry) && entry.Hub.Layout is { } layout
            ? entry.Hub.ResolvePreroll(Fitting(entry, seconds, Effective(entry, layout)))
            : 0;

    /// <summary>
    /// The rollback to actually ask the buffer for: what the viewer asked for, cut to what its queue
    /// can hold.
    ///
    /// <see cref="StreamHub.Subscribe"/> seeds a viewer's queue from the buffer before a live packet
    /// reaches it, and a seed larger than the queue does not cost the part that did not fit - it
    /// costs all of it. Every seeded packet is offered as not starting a segment, so the first
    /// overflow puts the subscription into resynchronising and discards the lot, leaving the viewer
    /// at the live edge after the header had already promised it thirty seconds. That is a real
    /// case rather than a theoretical one: at twenty-five frames a second beside AAC audio, a full
    /// thirty-second rollback is about 2150 packets against a ceiling of 2000.
    ///
    /// So the ask is cut here, where both the header and the seed read it, and a viewer is told the
    /// shorter figure it will actually be given. <see cref="LiveOptions.ViewerQueueSeconds"/> is
    /// held back out of the room, because the queue has to hold the live flow as well as the
    /// history, and it doubles as the margin the rounding needs: a rollback starts at the keyframe
    /// at or before what was asked for, which reaches further back than the ask by up to one
    /// keyframe interval. Where it reaches past what the queue can hold even so, the overshoot is
    /// taken off the ask and the buffer is asked again. A sender whose keyframe interval is longer
    /// than the whole live slack can still land outside on the second answer, which costs that
    /// viewer its rollback and nothing else - exactly what happened to every viewer before this.
    /// </summary>
    /// <param name="packetsPerSecond">
    /// The rate to fit against, from <see cref="Effective"/>. Passed in rather than resolved here
    /// so that <see cref="Serve"/> fits the rollback and sizes the queue from one reading of it.
    /// The observation behind it is rewritten every beat, and a rise landing between two reads
    /// would seed a queue from the lower figure with a rollback fitted to the higher - which does
    /// not cost the viewer the overshoot but the whole rollback, because every seeded packet is
    /// offered as not starting a segment and the first overflow discards the lot.
    /// </param>
    private double Fitting(LiveStreamEntry entry, double asked, double packetsPerSecond)
    {
        if (asked <= 0)
        {
            return asked;
        }

        var room = (_options.ViewerQueuePackets / packetsPerSecond) - _options.ViewerQueueSeconds;

        if (room <= 0)
        {
            return 0;
        }

        var ask = Math.Min(asked, room);
        var reaches = entry.Hub.ResolvePreroll(ask);

        return reaches <= room ? ask : Math.Max(0, ask - (reaches - room));
    }

    /// <summary>
    /// The rate this stream's viewers are sized against, and the one place either viewer path asks
    /// for it: the sender's declaration, or what the traffic is actually doing where that is more.
    ///
    /// Issue #26, and the arithmetic itself is
    /// <see cref="StreamLayout.EffectivePacketsPerSecond"/>, which says why it is a maximum and why
    /// the synthetic rate is added to the observed half. Here because this is where the two things
    /// it needs meet - the layout is the hub's, the observation is the entry's - and because
    /// <see cref="Serve"/> and <see cref="Fitting"/> must answer identically: a queue sized from
    /// one rate and a rollback fitted to another is a viewer promised more history than its queue
    /// can hold, which costs it the whole rollback rather than the part that did not fit.
    /// </summary>
    private double Effective(LiveStreamEntry entry, StreamLayout layout)
    {
        var effective = layout.EffectivePacketsPerSecond(entry.ObservedPacketsPerSecond);

        // Once per stream per connection and not once per sample, which is the whole of the
        // decision. That a sender is misdeclaring is the kind of thing an operator wants told -
        // an H.264 encoder's VUI timing is wrong in the wild by accident often enough that this
        // is a diagnosis rather than an accusation - but this runs for every viewer that attaches
        // and every rollback resolved, so a line per occurrence would be noise on the busiest path
        // here. See LiveStreamEntry.ReportsMisdeclaredRate for where the once is kept.
        if (effective > layout.PacketsPerSecond * MisdeclaredBeyond && entry.ReportsMisdeclaredRate())
        {
            // The sender's own figure, not the effective rate: the effective rate includes the
            // synthetic track this service publishes, so a stream declaring 1 and sending 50
            // beside a 1 Hz track would be reported as sending 51, and the number in an
            // accusation has to be one the operator can check against the camera.
            logger.LogInformation(
                "Stream '{Name}' declares {Declared} packets a second and is sending {Observed}; "
                + "its viewers' queues are sized from what arrives",
                entry.Name,
                Math.Round(layout.PacketsPerSecond, 2),
                Math.Round(entry.ObservedPacketsPerSecond, 2));
        }

        return effective;
    }

    public Task<double> WriteToViewerAsync(
        ViewerRequest request,
        Stream destination,
        double continueFromSeconds = 0,
        CancellationToken cancellationToken = default)
        => _local.TryGetValue(request.Name, out var entry)
            ? Serve(entry, request, destination, continueFromSeconds, cancellationToken)
            : throw new InvalidOperationException($"'{request.Name}' is not running on {Owner}.");

    /// <summary>
    /// Feeds one viewer until it leaves or the stream ends.
    ///
    /// During an interruption this simply has nothing to write, and the connection stays open. The
    /// client already knows the stream is interrupted from its state, and closing would push every
    /// viewer into reconnecting at the exact moment a reconnect storm is under way on ingest.
    ///
    /// Nothing here holds a thread while a viewer is not reading, and that is the one thing about
    /// this method worth knowing. It used to: the muxer wrote to the viewer's destination from
    /// inside libav's own synchronous write callback, so a consumer that stopped draining held the
    /// thread-pool worker that was writing to it, one worker per slow viewer with nothing bounding
    /// how many - measured at two hundred slow viewers costing two hundred and nineteen pool
    /// workers, a ceiling on viewers per replica counted in threads rather than in bandwidth. The
    /// bytes now land in memory and are handed to the destination between packets, where waiting on
    /// a viewer costs a continuation. A shallower queue would not have fixed it on its own: an
    /// overflowing subscription does not unblock a write already in progress.
    ///
    /// It bounds what it can bound. A destination with no asynchronous write of its own - an SRT
    /// socket, which libsrt gives no non-blocking send for short of its own epoll - still occupies
    /// the thread writing to it, which is what the consumption port has always spent per player,
    /// slow or not. What has gone is the unbounded half: the relay route, which is every viewer that
    /// reached the wrong replica, and which now costs nothing per viewer that will not read.
    ///
    /// That occupancy has since been measured rather than feared, and it is short for a player that
    /// advertised too-late-packet drop in its handshake: libsrt discards from the send buffer instead
    /// of making the write wait, and ten players taking three tenths of what they are sent cost fewer
    /// than half a thread each. It is not short for a player that cleared that flag, which nothing
    /// here chooses - the send then waits with no timeout at all, which is #20.
    /// <c>SrtSendPressureTests</c> has both figures, <c>LiveSlowPlayerTests</c> what the ordinary
    /// case costs.
    /// </summary>
    private async Task<double> Serve(
        LiveStreamEntry entry,
        ViewerRequest request,
        Stream destination,
        double continueFromSeconds,
        CancellationToken cancellationToken)
    {
        var layout = entry.Hub.Layout;

        if (layout is null)
        {
            return continueFromSeconds;
        }

        // Read once and used twice, which the two uses require of each other: the rollback below
        // is fitted to this rate and the queue is sized from it, and the observation behind it is
        // rewritten every beat. Fitting says what a rise landing between two reads would cost.
        var packetsPerSecond = Effective(entry, layout);

        // The same figure the header promised this viewer, and for the reason Fitting gives: a
        // rollback bigger than the queue costs the viewer all of it rather than the part that did
        // not fit.
        var preroll = Fitting(entry, request.Preroll, packetsPerSecond);

        // The rollback is part of the depth, because Subscribe fills the queue from the buffer
        // before a live packet ever reaches it: sized for the live slack alone, a viewer asking to
        // start twenty seconds back would overflow on the way in and lose exactly the rollback it
        // asked for.
        var queued = _options.ViewerQueueSeconds + entry.Hub.ResolvePreroll(preroll);

        using var subscription = entry.Hub.Subscribe(
            layout.QueueDepth(packetsPerSecond, queued, _options.ViewerQueuePackets),
            OverflowPolicy.SkipToLive,
            streamIndexes: [],
            preroll);

        // The muxer writes into memory and this loop writes memory to the viewer, rather than the
        // muxer writing to the viewer itself. See the note above on what the difference is worth.
        //
        // Declared in this order so that they are disposed in the other one. The buffer lends libav
        // a pooled array, and the muxer is what libav writes through: returning that array while a
        // muxer could still write to it would hand one viewer's bytes to whatever rented it next.
        // MuxerBuffer refuses a write after it has been disposed rather than leaving that to the
        // order of two lines, but the order is still the reason nothing has to.
        using var buffer = new MuxerBuffer();
        using var muxer = new PacketMuxer(buffer, layout, "mpegts", continueFromSeconds);

        // Counted for exactly the life of this subscription, which is exactly the life of one
        // viewer's connection - joined once it has actually subscribed rather than the moment the
        // call arrived, and left in a finally so a fault below still lets the count go down.
        entry.ViewerJoined();

        // The session is counted here, for the same reason and at the same moment: past the layout
        // check above, a viewer is being served. Counting where the attach was decided instead made
        // this a count of attempts - the consumption port retries every quarter second, so a stream
        // with no layout yet, or an owner whose address the registry never recorded, scored a
        // session several times a second for as long as the player stayed connected, which is
        // exactly the incident the figure exists to describe.
        metrics.Viewing(request.Relayed ? "relayed" : "direct");

        try
        {
            await foreach (var packet in subscription.Packets.ReadAllAsync(cancellationToken))
            {
                muxer.Write(packet);

                if (muxer.Fault is not null)
                {
                    break;
                }

                await buffer.DrainToAsync(destination, cancellationToken);
            }

        }
        catch (OperationCanceledException)
        {
            // The viewer closed the player. Normal.
        }
        catch (Exception ex)
        {
            // Where a viewer leaving now arrives. The muxer used to swallow it, because the write
            // that failed was its own; a refused write reaches this loop instead, and a player that
            // was closed is still not worth more than a debug line.
            logger.LogDebug(ex, "A viewer of '{Name}' went away", entry.Name);
        }
        finally
        {
            entry.ViewerLeft();
        }

        // No trailer: this connection may be handed straight on to another replica, and a trailer
        // would tell the player the stream had ended when it has not.
        muxer.Abandon();

        return muxer.TimelineSeconds;
    }

    /// <summary>
    /// One pass of the heartbeat: republish what is running here, stand down where this replica
    /// has lost a name, retire streams whose grace period has expired, close recordings that have
    /// reached their end, and take a copy of the registry for the handshake to read.
    /// </summary>
    public async Task TickAsync(CancellationToken cancellationToken)
    {
        // Timed in a finally, so a pass that threw is timed too. That is the reading that matters:
        // a registry taking longer than the beat is what a slow pass usually is, and it throws at
        // the end of the wait rather than at the start of it.
        var started = TimeProvider.System.GetTimestamp();

        try
        {
            foreach (var entry in _local.Values.ToList())
            {
                try
                {
                    await TickAsync(entry, cancellationToken);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "The heartbeat for '{Name}' failed", entry.Name);

                    metrics.BeatFailed("stream");
                }
            }

            // After the pass, so it counts what survived it. One beat stale at worst, which is the
            // same freshness as everything else a replica publishes about itself, and an autoscaler
            // that cared about two seconds would be reacting to a reconnect.
            metrics.Census = Census();

            try
            {
                await RefreshKnownAsync(cancellationToken);
            }
            catch (Exception)
            {
                // Counted and rethrown, unlike the two passes around it. The caller already treats a
                // failed beat as survivable, and losing this read is what makes the handshake answer
                // from a stale copy of what is claimed, so it has to leave more than a log line.
                metrics.BeatFailed("registry");

                throw;
            }

            try
            {
                await AdoptAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                // The store is a second thing that can be unreachable, and unlike the registry its
                // implementations let a failure out rather than swallowing it. A Redis that has gone
                // away must cost the cluster new pull streams, not the heartbeat that keeps the ones
                // already running listed.
                logger.LogWarning(ex, "Reconciling configured sources failed");

                metrics.BeatFailed("sources");
            }
        }
        finally
        {
            metrics.Beat(TimeProvider.System.GetElapsedTime(started));
        }
    }

    /// <summary>
    /// Counts what this replica is holding, for the six figures a dashboard reads as the shape of a
    /// pod: how many streams, how many of them are broken in the two ways that matter, and how much
    /// work is hanging off them.
    ///
    /// A second walk of the local streams rather than an accumulation through the pass above,
    /// because the pass removes entries as it goes and a count taken while it ran would include
    /// streams that are no longer here. One hub lock per stream per beat, which is the same cost
    /// describing them already pays.
    /// </summary>
    private LiveCensus Census()
    {
        // Counted in the walk rather than read off the dictionary afterwards, so the total and the
        // figures inside it describe the same set: a stream arriving between the two would otherwise
        // leave a census claiming more interrupted streams than streams.
        var streams = 0;
        var interrupted = 0;
        var unstartable = 0;
        var viewers = 0;
        var recordings = 0;
        var forwards = 0;

        foreach (var entry in _local.Values)
        {
            streams++;

            if (Interrupted(entry, Silence(entry)))
            {
                interrupted++;
            }

            if (!entry.Hub.BufferState().Startable)
            {
                unstartable++;
            }

            viewers += entry.Viewers;

            if (entry.Recorder is { Finished: false })
            {
                recordings++;
            }

            forwards += entry.Forwards.Count;
        }

        return new LiveCensus(streams, interrupted, unstartable, viewers, recordings, forwards);
    }

    /// <summary>
    /// Picks up configured pull sources that are not live anywhere, by whichever replica gets there
    /// first.
    ///
    /// There is no scheduler and no assignment: every replica sees the same list every beat and
    /// races for what is missing, and <see cref="ClaimAsync"/>'s distributed lock is what makes that
    /// safe. Losing is the normal outcome - with a thousand sources and many replicas most attempts
    /// lose - so it is not logged as a failure and does not count as an attempt worth backing off
    /// from any differently than a success.
    /// </summary>
    private async Task AdoptAsync(CancellationToken cancellationToken)
    {
        if (Full())
        {
            return;
        }

        var configured = await sources.ListAsync(cancellationToken);
        var now = DateTimeOffset.UtcNow;
        var backoff = TimeSpan.FromSeconds(_options.ForwardRetrySeconds);

        var candidates = configured
            .Where(source => source is { Enabled: true, IsPull: true })
            .Where(source => !_local.ContainsKey(source.Name))
            .Where(source => !LiveElsewhere(source.Name))
            .Where(source => !_attempted.TryGetValue(source.Name, out var last) || now - last >= backoff)
            // Shuffled, which is the whole of the anti-stampede measure. A thousand replicas reading
            // one list in one order would all reach for the same source on the same beat and all but
            // one would waste a lock acquisition on it; in a different order each they spread across
            // the work and the lock is contended by a handful rather than by everybody.
            .OrderBy(_ => Random.Shared.Next())
            .ToList();

        // Names the store no longer carries, in the idiom RefreshKnownAsync uses on the registry.
        // Without it a source deleted and recreated under a new name leaves its back-off behind for
        // the life of the process.
        foreach (var name in _attempted.Keys.Except(
                     configured.Select(source => source.Name),
                     StringComparer.Ordinal))
        {
            _attempted.TryRemove(name, out _);
        }

        foreach (var source in candidates)
        {
            if (Full())
            {
                // The same ceiling the handshake refuses publishers at, and for the same reason: a
                // replica at its limit taking pulled streams as well would abandon what it already
                // holds. Re-read each time round, because this loop is what moves the count.
                return;
            }

            // Recorded before the attempt and whatever the outcome. A URL the allowlist refuses and
            // a far end that is down both throw below, and without this every replica would dial an
            // unreachable camera every two seconds for as long as it stays unreachable.
            _attempted[source.Name] = DateTimeOffset.UtcNow;

            try
            {
                if (await PullAsync(source.Name, source.Url!, cancellationToken) is not null)
                {
                    logger.LogInformation("Picked up the configured source '{Name}'", source.Name);
                }
                else
                {
                    logger.LogDebug("'{Name}' was claimed by another replica first", source.Name);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Could not pick up the configured source '{Name}'", source.Name);
            }
        }
    }

    /// <summary>
    /// Whether this replica is at the ceiling <see cref="AdmitPublisher"/> refuses publishers at. A
    /// pulled stream costs what a pushed one costs, so the two are counted against one limit.
    /// </summary>
    private bool Full() => _options.MaxStreams > 0 && _local.Count >= _options.MaxStreams;

    /// <summary>
    /// Whether some other replica is already running this name, read from the heartbeat's copy of
    /// the registry on exactly the rule <see cref="ClaimAsync"/> would apply. Getting it wrong here
    /// costs a lock acquisition and a refusal, not a second stream.
    /// </summary>
    private bool LiveElsewhere(string name)
        => _known.TryGetValue(name, out var held)
            && held.Owner != Owner
            && held.State == LiveStreamState.Live
            && LiveStreamStaleness.OwnerAlive(held, Beat);

    /// <summary>
    /// The one registry read the handshake depends on, taken last so that what this pass just
    /// published about its own streams is in the copy rather than a beat behind it.
    ///
    /// A whole listing every beat, which is what makes the callback a dictionary lookup. It is also
    /// the ceiling on this design: at a thousand streams it is a thousand entries deserialised every
    /// two seconds on every replica.
    ///
    /// ponytail: the registry has no "what changed" and adding one would mean a second Redis
    /// structure to keep honest. If the listing ever shows up in a profile, publish claims on the
    /// change feed that already exists and keep the listing as the periodic repair.
    /// </summary>
    private async Task RefreshKnownAsync(CancellationToken cancellationToken)
    {
        var streams = await registry.ListAsync(cancellationToken);

        foreach (var stream in streams)
        {
            _known[stream.Name] = stream;
        }

        // Names the listing no longer carries are gone from the registry, and leaving them here
        // would lock a name nothing owns until the process restarted.
        foreach (var name in _known.Keys.Except(streams.Select(stream => stream.Name), StringComparer.Ordinal))
        {
            _known.TryRemove(name, out _);
        }
    }

    private async Task TickAsync(LiveStreamEntry entry, CancellationToken cancellationToken)
    {
        if (entry.Recorder is { } recorder && (recorder.Finished || recorder.Due()))
        {
            if (!recorder.Finished)
            {
                recorder.Stop();
            }
            else
            {
                entry.RecordingEnded();
            }
        }

        var shared = await registry.GetAsync(entry.Name, cancellationToken);

        if (shared is not null && shared.Owner != Owner && !LiveStreamStaleness.IsGone(shared, Grace))
        {
            // Displaced. The name was free when the other replica took it - this feed had stopped,
            // or this pod had stopped saying it was alive - so everything here shuts down and any
            // recording closes as a complete document rather than moving.
            logger.LogInformation("'{Name}' now belongs to {Owner}; standing down", entry.Name, shared.Owner);

            // Not EndAsync: the registry entry belongs to the new owner now and removing it would
            // delete a live stream out from under it. The meter still hears about it, because a pod
            // shedding streams it thought were its own is the same event from here whichever way it
            // is written.
            metrics.Ended("displaced");

            _local.TryRemove(entry.Name, out _);
            entry.StandingDown = true;

            await entry.DisposeAsync();

            return;
        }

        var silent = Silence(entry);
        var interrupted = Interrupted(entry, silent);

        if (interrupted && Expired(entry, silent))
        {
            _local.TryRemove(entry.Name, out _);

            await EndAsync(entry, "expired", "its grace period expired");

            return;
        }

        // ponytail: one store read per stream per beat, which at a thousand streams is a thousand
        // more round trips every two seconds on top of the thousand the registry already costs
        // here. The store says of itself that it is read-heavy and rarely written, so both
        // implementations can serve this from memory; if one ever cannot, read the whole list once
        // per beat beside RefreshKnownAsync and reconcile every entry from that copy.
        var source = await sources.GetAsync(entry.Name, cancellationToken);

        if (entry.Manual && source is { Enabled: false })
        {
            // Parking a source stops this service dialling out, which is the only reading of
            // "disabled" an operator who has just switched one off will accept. Leaving the pull
            // running would make the toggle mean "stop trying again later", and the row would sit
            // there disabled while its camera carried on arriving.
            //
            // Deliberately narrow, in two ways. Only a pulled stream: a pushed one is an encoder's
            // to stop, and refusing it is the name lock's business rather than this toggle's. And
            // only when the row exists and says false: an absent row must not sweep anything,
            // because a stream created straight through the manual endpoint has no row at all, and
            // because deleting a configuration is not a licence to yank a live feed from its
            // viewers - that is the stop endpoint, asked for explicitly.
            _local.TryRemove(entry.Name, out _);

            await EndAsync(entry, "source-off", "its source was switched off");

            return;
        }

        ReconcileForwards(entry, source);

        if (await ReconcileMetadata(entry, source))
        {
            // The stream was ended to pick up a configuration change. Nothing left to describe.
            return;
        }

        await registry.UpsertAsync(
            Describe(entry, interrupted ? LiveStreamState.Interrupted : LiveStreamState.Live),
            cancellationToken);
    }

    /// <summary>
    /// Brings the forwards running for this stream into line with what was configured for its name.
    ///
    /// It happens here, in the per-stream heartbeat, and that is the design decision behind the
    /// whole shape: the forwards hang off the local entry, so they follow the stream's owner with no
    /// arrangement of their own. A name that moves to another pod is claimed there and reconciled
    /// there on the next beat, while the pod that lost it stands down and disposes the entry, which
    /// stops its copies. A forward lease would be a second claim to keep in step with the first, and
    /// the failure it would exist to prevent - two pods pushing one stream to one far end - is
    /// already prevented by the name claim, because only one pod has the bytes.
    /// </summary>
    /// <param name="source">
    /// The configured row for this name, read once by the caller because the same read also decides
    /// whether a parked pull should still be running. Null when nothing was configured, which is the
    /// ordinary case for a stream an encoder simply pushed.
    /// </param>
    private void ReconcileForwards(LiveStreamEntry entry, LiveSource? source)
    {
        // A disabled source silences its forwards without forgetting them, which is what parking a
        // source means; an absent one has nothing to say about a stream an encoder simply pushed.
        var desired = source is { Enabled: true }
            ? source.Forwards.Where(target => target.Enabled).ToArray()
            : [];

        var running = entry.Forwards.ToDictionary(
            pair => pair.Key,
            pair => new RunningForward(pair.Value.Url, pair.Value.Finished, pair.Value.LastAttemptAt),
            StringComparer.Ordinal);

        var (start, stop) = ForwardPlan.Decide(
            desired,
            running,
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(_options.ForwardRetrySeconds));

        foreach (var id in stop)
        {
            if (entry.Forwards.TryRemove(id, out var stopping))
            {
                stopping.Dispose();
            }
        }

        foreach (var target in start)
        {
            string? refusal = null;

            try
            {
                // The allowlist that stops "create a stream" becoming "read this local file" is the
                // same one that stops a forward becoming "write this local file".
                RequireAllowed(target.Url, forOutput: true);
            }
            catch (NotSupportedException ex)
            {
                refusal = ex.Message;
            }

            if (refusal is null && entry.Hub.Layout is null)
            {
                // Nothing has been demultiplexed yet, so there is no container to write. Starting
                // now would fail at once and burn the retry window, and leaving it out of the
                // dictionary means the next beat plans it again rather than waiting for a retry.
                continue;
            }

            var forwarder = new StreamForwarder(target.Id, target.Url, entry.Hub, _options, logger);

            entry.Forwards[target.Id] = forwarder;

            // A refusal is answered before the stream has a layout, unlike a real start: the
            // operator mistyped a URL and should be told now rather than when bytes happen to
            // arrive, and there is no transport to open for one.
            if (refusal is not null)
            {
                forwarder.Refuse(refusal);
            }
            else
            {
                forwarder.Start(entry.Lifetime.Token);
            }
        }
    }

    /// <summary>
    /// The synthetic tracks a configured row asks for on this stream's next layout.
    ///
    /// The rate is <see cref="StaticSensorPublisher"/>'s own constant rather than a number written
    /// out here, because the two must not drift: a track whose declared rate is wrong resizes every
    /// viewer's queue on the stream, and an undeclared one is worse still.
    /// </summary>
    private static IReadOnlyList<SyntheticTrack> Synthetic(LiveSource? source)
        => source is { Enabled: true, StaticSensor: not null }
            ? [new SyntheticTrack(SyntheticTrackRole.PlatformMetadata, StaticSensorPublisher.PacketsPerSecond)]
            : [];

    /// <summary>
    /// Brings the metadata this replica synthesises for this stream into line with what was
    /// configured for its name - the metadata twin of <see cref="ReconcileForwards"/>, on the same
    /// once-per-stream-per-beat read of the same row.
    ///
    /// Three things have to agree: what is configured now, what the running layout carries, and
    /// whether a publisher is putting packets on it. The first two can only be made to agree at a
    /// connection, because the track is part of the layout and every consumer already attached
    /// subscribed against the layout it joined on. So this reconciles the publisher freely, and
    /// where the layout itself is wrong it says so or reconnects.
    /// </summary>
    /// <returns>True when the stream was ended to pick the change up, so the caller stops here.</returns>
    private async Task<bool> ReconcileMetadata(LiveStreamEntry entry, LiveSource? source)
    {
        var sensor = source is { Enabled: true } ? source.StaticSensor : null;

        // What the next layout should carry, whenever that is. Parking a source silences its
        // synthesis the same way it silences its forwards.
        entry.Hub.Synthesise(Synthetic(source));

        var layout = entry.Hub.Layout;

        // Precedence, and the one case where a mismatch is the right answer rather than something
        // to reconcile. The sender declares KLV of its own, so no synthetic track was added and
        // none should be: a camera that reports its own telemetry wins over what somebody typed
        // about it. Deliberately decided on what the layout declares and not on whether packets
        // are actually arriving on it - a timeout would make this depend on how long nothing had
        // been seen for, which is a different and much worse question.
        if (layout is { KlvIndex: >= 0, KlvIsSynthetic: false })
        {
            await entry.SynthesiseAsync(null);

            return false;
        }

        var carried = layout is { KlvIsSynthetic: true };

        // Published only onto a track that exists. Asked to publish onto one that does not, the
        // publisher would simply find no index and tick away doing nothing, but saying it here is
        // what keeps "is this stream carrying synthesised metadata" a single fact.
        await entry.SynthesiseAsync(carried ? sensor : null);

        if (layout is null || carried == (sensor is not null))
        {
            return false;
        }

        if (!entry.Manual || source is not { IsPull: true })
        {
            // Nothing here can reopen this connection. A pushed stream is the encoder's to
            // reconnect, and dropping it to force that would be this service deciding a camera
            // should stop sending; a manual stream whose row carries no URL is the same, since
            // AdoptAsync would have nothing to pick the name up with and it would simply end.
            //
            // So the configuration is reported as not yet carried - LiveStream.KlvSynthesised
            // beside LiveStream.HasKlv - rather than reported as live, which is the one answer that
            // would be a lie. Debug rather than information because this is a standing state rather
            // than an event, and the heartbeat visits it every two seconds for as long as it lasts.
            // Both directions of the mismatch reach here and they are not the same news: one is a
            // configuration waiting to appear, the other a track waiting to go. Reported as one
            // message they would read as the first, which for a stream that is carrying a track
            // nothing publishes on any more is the opposite of what is happening.
            logger.LogDebug(
                sensor is null
                    ? "'{Name}' carries a synthesised metadata track that is no longer configured and nothing is publishing on it; it goes when the feed reconnects"
                    : "'{Name}' has sensor metadata configured that its layout does not carry; it will appear when the feed reconnects",
                entry.Name);

            return false;
        }

        // A pulled stream: this replica owns the connection, so it can reopen it, and the same
        // mechanism a parked source uses is the one that does it. The stream ends here and
        // AdoptAsync picks the name up again on a later beat, one connection later carrying the
        // right layout - so this converges rather than repeating, because PullAsync tells the new
        // hub what to synthesise before the demultiplexer starts.
        _local.TryRemove(entry.Name, out _);

        await EndAsync(entry, "sensor-changed", "its sensor metadata configuration changed");

        return true;
    }

    /// <summary>
    /// Whether an interrupted stream has waited long enough. A manual stream that has never
    /// received anything is given the same window from when it was created, so one created a
    /// moment before its sender starts is not swept away in between.
    /// </summary>
    private bool Expired(LiveStreamEntry entry, TimeSpan? silent)
        => silent is { } quiet ? quiet > Grace : DateTimeOffset.UtcNow - entry.StartedAt > Grace;

    /// <summary>How long since a packet arrived, or null when none ever has.</summary>
    private static TimeSpan? Silence(LiveStreamEntry entry)
        => entry.Hub.LastPacketAt is { } last ? DateTimeOffset.UtcNow - last : null;

    private bool Interrupted(LiveStreamEntry entry, TimeSpan? silent)
        => !entry.FeedRunning || silent > TimeSpan.FromSeconds(_options.FeedTimeoutSeconds);

    /// <param name="reason">
    /// One of a fixed handful, for the meter. Separate from <paramref name="why"/> because that one
    /// is a sentence written for a person reading a log, and a tag value has to be a word this
    /// service chose rather than a phrase somebody may reword later.
    /// </param>
    private async Task EndAsync(LiveStreamEntry entry, string reason, string why)
    {
        logger.LogInformation("Stream '{Name}' is gone because {Why}", entry.Name, why);

        metrics.Ended(reason);

        await entry.DisposeAsync();

        // Removed rather than left as finished. After the grace period the stream leaves nothing
        // behind: no entry, no history. The documents it produced are its trace.
        await registry.RemoveAsync(entry.Name, CancellationToken.None);
    }

    private LiveStream Describe(LiveStreamEntry entry, LiveStreamState state)
    {
        var buffer = entry.Hub.BufferState();

        // Asked of libsrt here because this runs once per beat per stream and nowhere else does.
        // The sample is the interval since the last beat, which is what makes the answer "broken
        // now" rather than "broken at some point". A stream with no socket - pulled, or between
        // connections - reports nothing rather than a stale figure from the connection before.
        var health = entry.Transport?.Health();

        // Reported here because this is where the sample is taken. libsrt clears its interval
        // counters on the read above, so whoever reads them owes the meter the figures or they are
        // gone; the hub's packet and byte totals are turned into an interval by the entry, against
        // what it last reported. Describing a stream more than once in a beat - a claim does, and so
        // does a manual creation - therefore stays exact rather than double counting.
        metrics.Fed(entry.TakeFeed(health?.Lost ?? 0, health?.Dropped ?? 0));

        return new LiveStream(
            entry.Name,
            state,
            entry.StartedAt,
            DateTimeOffset.UtcNow,
            Owner,
            _options.PeerBaseUrl,
            entry.Hub.Packets,
            entry.Hub.Bytes,
            entry.Harvester.Preview is not null,
            buffer.Startable,
            buffer.CeilingBinding,
            buffer.HeldSeconds,
            entry.Hub.Layout?.Describe(),
            entry.Recorder is { Finished: false } recorder ? recorder.Status : null,
            entry.ConnectionId,
            entry.Manual,
            health?.Lost ?? 0,
            health?.Dropped ?? 0,
            entry.Klv.Present,
            entry.Klv.LastPacketAt,
            entry.Klv.Classification,
            entry.Detection.Enabled,
            entry.Detection.Rate,
            entry.Detection.Worker,
            // Null rather than an empty list when nothing is forwarded, so a listing of a thousand
            // ordinary streams does not carry a thousand empty arrays through Redis and out again.
            entry.Forwards.IsEmpty ? null : [.. entry.Forwards.Values.Select(forwarder => forwarder.Status)],
            health?.Link,
            entry.Viewers,
            entry.Detection.Model,
            entry.Detection.Labels.Count == 0 ? null : entry.Detection.Labels,
            KlvSynthesised: entry.Hub.Layout is { KlvIsSynthetic: true });
    }

    /// <param name="forOutput">
    /// True for a forward target, which libav has to be able to write rather than read. The same
    /// allowlist governs both: a URL is a URL, and "write this local file" is the mirror of the
    /// attack the list was drawn up against. The support check is not the same, because an FFmpeg
    /// build can carry one direction of a protocol and not the other.
    /// </param>
    private void RequireAllowed(string url, bool forOutput = false)
    {
        var separator = url.IndexOf("://", StringComparison.Ordinal);
        var scheme = separator > 0 ? url[..separator].ToLowerInvariant() : "file";

        if (!_options.AllowedSchemes.Contains(scheme, StringComparer.OrdinalIgnoreCase))
        {
            // Without this, a caller could make the service read anywhere libav can reach, local
            // files included.
            throw new NotSupportedException(
                $"Transport '{scheme}' is not allowed. Allowed: {string.Join(", ", _options.AllowedSchemes)}.");
        }

        if (!FfmpegLibrary.Supports(url, forOutput))
        {
            throw new NotSupportedException(
                $"The loaded FFmpeg has no '{scheme}' support. Available: {string.Join(", ", Transports)}.");
        }
    }

    private static string FileName(string name)
    {
        var flattened = name.Replace('/', '-');

        foreach (var invalid in Path.GetInvalidFileNameChars())
        {
            flattened = flattened.Replace(invalid, '_');
        }

        return flattened;
    }

    public async ValueTask DisposeAsync()
    {
        // Registered twice, as itself and as the port, so the container disposes it twice.
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        await _shutdown.CancelAsync();

        foreach (var entry in _local.Values)
        {
            try
            {
                // Left as interrupted rather than removed, so the replica that takes the name over
                // finds the entry and resumes the same stream: a rolling update is a move, not an
                // end, and deleting here reset every stream's start time once per update while a
                // crash preserved it (.scratch/scale-to-1000/cross-pod.md). The heartbeat written
                // here is the last this entry gets, and that is what starts the clocks: readers
                // stop listing it after the grace period, and the retention sweeper deletes it
                // once the owner has been silent long enough. Written before the entry is torn
                // down, while the hub can still be described.
                await registry.UpsertAsync(Describe(entry, LiveStreamState.Interrupted), CancellationToken.None);
                await entry.DisposeAsync();

                metrics.Ended("shutdown");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Shutting '{Name}' down was untidy", entry.Name);
            }
        }

        _local.Clear();
        _shutdown.Dispose();
    }
}
