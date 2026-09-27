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
    /// <summary>How often the registry is refreshed and the local streams reconsidered.</summary>
    public static readonly TimeSpan Beat = TimeSpan.FromSeconds(2);

    private readonly ConcurrentDictionary<string, LiveStreamEntry> _local = new(StringComparer.Ordinal);

    /// <summary>
    /// The registry as it looked at the last heartbeat, which is the only form the handshake can
    /// read.
    /// </summary>
    private readonly ConcurrentDictionary<string, LiveStream> _known = new(StringComparer.Ordinal);

    /// <summary>When this replica last tried to pick up each configured source.</summary>
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

        // A stream whose owner has stopped heartbeating is gone, and after that it leaves no trace:
        // the registry is a picture of what is live now, and the documents a stream produced are
        // what outlives it.
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
    /// beat, because the caller is answered with the stream as it now stands and a worker lists the
    /// registry rather than asking the owner.
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
        // rate.
        return _options.MaxStreams > 0
            && _local.Count >= _options.MaxStreams
            && !_local.ContainsKey(admission.Name)
                ? Srt.SRT_REJX_OVERLOAD
                : null;
    }

    /// <summary>Takes an accepted socket off the accept thread.</summary>
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
                // is the one-beat window two replicas can both admit in.
                logger.LogInformation(
                    "'{Name}' is held elsewhere, so the connection accepted here is being closed",
                    name);

                transport.Dispose();

                await DiscardAsync(entry);

                return;
            }

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

    /// <summary>Wraps the socket as a libav transport and demultiplexes it until the feed ends.</summary>
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

                // The same stream resuming, so it keeps the start time it has always had.
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

    /// <summary>Throws away an entry this replica turned out not to own.</summary>
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
            throw new InvalidOperationException($"'{name}' is already live on another replica.");
        }

        return Describe(entry, LiveStreamState.Live);
    }

    /// <summary>
    /// Claims a name and opens a pulled input on it, for a protocol that cannot name itself.
    /// </summary>
    private async Task<LiveStreamEntry?> PullAsync(string name, string url, CancellationToken cancellationToken)
    {
        if (!StreamName.TryParse(name, out var parsed, out var rejection))
        {
            throw new ArgumentException(rejection, nameof(name));
        }

        RequireAllowed(url);

        var entry = _local.GetOrAdd(parsed, _ => Create(parsed, manual: true, manualUrl: url));

        if (!await ClaimAsync(entry, cancellationToken))
        {
            await DiscardAsync(entry);

            return null;
        }

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

        // Provenance, not a second trigger: a detector arrives on this same call.
        if (detection is not null)
        {
            metadata[DetectionReference.MetadataKey] = detection.ToString();
        }

        await using var scope = scopeFactory.CreateAsyncScope();
        var documents = scope.ServiceProvider.GetRequiredService<IDocumentService>();

        using var content = new MemoryStream(bytes);

        var document = await documents.UploadAsync(
            $"{FileName(name)}-{takenAt:yyyyMMdd-HHmmss}.jpg",
            content,
            "image/jpeg",
            cancellationToken,
            metadata);

        logger.LogInformation("Snapshot of '{Name}' stored as {DocumentId}", name, document.Id);

        return document.Id;
    }

    /// <summary>A snapshot is a fresh decode of the newest segment, not the harvester's frame.</summary>
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
        // ponytail: the layout is read in a second lock acquisition, so a reconnect landing between
        // the copy and this line would mux old packets against a newer layout.
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

        // One recording at a time per stream.
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
    /// What a viewer asking to start this far back would actually get, which is at least what it
    /// asked for.
    /// </summary>
    public double ResolvePreroll(string name, double seconds)
        => _local.TryGetValue(name, out var entry) ? entry.Hub.ResolvePreroll(seconds) : 0;

    public Task<double> WriteToViewerAsync(
        ViewerRequest request,
        Stream destination,
        double continueFromSeconds = 0,
        CancellationToken cancellationToken = default)
        => _local.TryGetValue(request.Name, out var entry)
            ? Serve(entry, request, destination, continueFromSeconds, cancellationToken)
            : throw new InvalidOperationException($"'{request.Name}' is not running on {Owner}.");

    /// <summary>Feeds one viewer until it leaves or the stream ends.</summary>
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

        using var subscription = entry.Hub.Subscribe(
            _options.ViewerQueuePackets,
            OverflowPolicy.SkipToLive,
            streamIndexes: [],
            request.Preroll);

        using var muxer = new PacketMuxer(destination, layout, "mpegts", continueFromSeconds);

        // Counted for exactly the life of this subscription, which is exactly the life of one
        // viewer's connection - joined once it has actually subscribed rather than the moment the
        // call arrived, and left in a finally so a fault below still lets the count go down.
        entry.ViewerJoined();

        try
        {
            await foreach (var packet in subscription.Packets.ReadAllAsync(cancellationToken))
            {
                muxer.Write(packet);

                if (muxer.Fault is not null)
                {
                    break;
                }
            }

        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
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
    /// One pass of the heartbeat: republish what is running here, stand down where this replica has
    /// lost a name, retire streams whose grace period has expired, close recordings that have
    /// reached their end, and take a copy of the registry for the handshake to read.
    /// </summary>
    public async Task TickAsync(CancellationToken cancellationToken)
    {
        // Timed in a finally, so a pass that threw is timed too.
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

            // After the pass, so it counts what survived it.
            metrics.Census = Census();

            try
            {
                await RefreshKnownAsync(cancellationToken);
            }
            catch (Exception)
            {
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
                // implementations let a failure out rather than swallowing it.
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
    /// </summary>
    private LiveCensus Census()
    {
        // Counted in the walk rather than read off the dictionary afterwards, so the total and the
        // figures inside it describe the same set: a stream arriving between the two would
        // otherwise leave a census claiming more interrupted streams than streams.
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
            // Shuffled, which is the whole of the anti-stampede measure.
            .OrderBy(_ => Random.Shared.Next())
            .ToList();

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
                // holds.
                return;
            }

            // Recorded before the attempt and whatever the outcome.
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
    /// Whether this replica is at the ceiling <see cref="AdmitPublisher"/> refuses publishers at.
    /// </summary>
    private bool Full() => _options.MaxStreams > 0 && _local.Count >= _options.MaxStreams;

    /// <summary>
    /// Whether some other replica is already running this name, read from the heartbeat's copy of
    /// the registry on exactly the rule <see cref="ClaimAsync"/> would apply.
    /// </summary>
    private bool LiveElsewhere(string name)
        => _known.TryGetValue(name, out var held)
            && held.Owner != Owner
            && held.State == LiveStreamState.Live
            && LiveStreamStaleness.OwnerAlive(held, Beat);

    /// <summary>
    /// The one registry read the handshake depends on, taken last so that what this pass just
    /// published about its own streams is in the copy rather than a beat behind it.
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
            logger.LogInformation("'{Name}' now belongs to {Owner}; standing down", entry.Name, shared.Owner);

            // Not EndAsync: the registry entry belongs to the new owner now and removing it would
            // delete a live stream out from under it.
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
        // here.
        var source = await sources.GetAsync(entry.Name, cancellationToken);

        if (entry.Manual && source is { Enabled: false })
        {
            // Parking a source stops this service dialling out, which is the only reading of
            // "disabled" an operator who has just switched one off will accept.
            _local.TryRemove(entry.Name, out _);

            await EndAsync(entry, "source-off", "its source was switched off");

            return;
        }

        ReconcileForwards(entry, source);

        await registry.UpsertAsync(
            Describe(entry, interrupted ? LiveStreamState.Interrupted : LiveStreamState.Live),
            cancellationToken);
    }

    /// <summary>
    /// Brings the forwards running for this stream into line with what was configured for its name.
    /// </summary>
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

    /// <summary>Whether an interrupted stream has waited long enough.</summary>
    private bool Expired(LiveStreamEntry entry, TimeSpan? silent)
        => silent is { } quiet ? quiet > Grace : DateTimeOffset.UtcNow - entry.StartedAt > Grace;

    /// <summary>How long since a packet arrived, or null when none ever has.</summary>
    private static TimeSpan? Silence(LiveStreamEntry entry)
        => entry.Hub.LastPacketAt is { } last ? DateTimeOffset.UtcNow - last : null;

    private bool Interrupted(LiveStreamEntry entry, TimeSpan? silent)
        => !entry.FeedRunning || silent > TimeSpan.FromSeconds(_options.FeedTimeoutSeconds);

    /// <param name="reason">One of a fixed handful, for the meter.</param>
    private async Task EndAsync(LiveStreamEntry entry, string reason, string why)
    {
        logger.LogInformation("Stream '{Name}' is gone because {Why}", entry.Name, why);

        metrics.Ended(reason);

        await entry.DisposeAsync();

        await registry.RemoveAsync(entry.Name, CancellationToken.None);
    }

    private LiveStream Describe(LiveStreamEntry entry, LiveStreamState state)
    {
        var buffer = entry.Hub.BufferState();

        // Asked of libsrt here because this runs once per beat per stream and nowhere else does.
        var health = entry.Transport?.Health();

        // Reported here because this is where the sample is taken.
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
            entry.Detection.Labels.Count == 0 ? null : entry.Detection.Labels);
    }

    /// <param name="forOutput">
    /// True for a forward target, which libav has to be able to write rather than read.
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
                // crash preserved it (.scratch/scale-to-1000/cross-pod.md).
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
