using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// A durable capture of a stream, taken because something asked.
///
/// One shape with an optional duration: a clip is a recording whose stop time was set when it
/// started, and two operations for one thing would drift apart. A person pressing record and a
/// detector firing are the same caller, which is what makes detection genuinely free to add later
/// rather than a second code path.
///
/// It begins in the buffer. A trigger at a moment yields a capture starting before it and running
/// past it, so an event already under way when it was noticed is still caught. That pre-roll is
/// the entire reason the buffer is unconditional.
///
/// **It is written in parts and stored as it goes.** A camera recording for six hours cannot wait
/// until it ends to be stored: the bytes would sit on one pod's disk the whole time and die with
/// it. So a few minutes are muxed to a local file, uploaded through the ordinary storage path as an
/// ordinary object, and the local file deleted. Disk is bounded by a handful of parts however long
/// the recording runs, and what has already been recorded survives the pod.
///
/// **Storing a part does not stop it reading.** The upload runs on its own task, fed a finished
/// file at a time, and the muxing loop opens the next part rather than waiting for it. That used
/// not to be so, and the difference is the whole safety of the thing: the packet queue this
/// subscribes to fails on overflow rather than skipping, so for as long as an upload sat between
/// two reads of it, the queue had to hold everything the stream sent while storage worked - most of
/// a gigabyte for a five-minute part of a contribution feed - or the recording ended and the
/// document was marked truncated. See <see cref="StorePartsAsync"/>.
///
/// A part is not a segment. A segment is the buffer's unit, one keyframe to the next, and is the
/// sender's to decide; a part is ours and is minutes long. A part holds many segments.
///
/// None of it reaches whoever opens the document. One name, one size, one download: the parts are
/// joined on the way out and are seekable end to end, and where the boundaries fell is this
/// class's business and nobody else's.
///
/// Both storage providers stay equal, which is the point of the abstraction: each part is one
/// ordinary write, and nothing here needs multipart upload or anything else only S3 has.
/// </summary>
public sealed class StreamRecorder
{
    private readonly StreamHub _hub;
    private readonly LiveOptions _options;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly Lock _gate = new();
    private readonly string _directory;

    /// <summary>The stream's ST 0102 marking, asked for when a part is stored so the document carries it.</summary>
    private readonly Func<string?> _classification;

    /// <summary>The detection that asked for this recording, when one did. Null when a person did.</summary>
    private readonly DetectionReference? _detection;

    /// <summary>
    /// Told what this recording produced, and told about each part as it is stored rather than only
    /// at the end: a six-hour recording that reports nothing until it finishes is invisible for six
    /// hours. Null where nothing is measuring, which is how the tests build one.
    /// </summary>
    private readonly LiveMetrics? _metrics;

    private DateTimeOffset _endsAt;

    /// <summary>
    /// Bytes of the parts already muxed and handed over for storage, so the running total survives a
    /// part roll.
    ///
    /// Muxed rather than stored, which is the honest figure now that the two differ: a part on its
    /// way to storage is recorded video and saying otherwise would make <see cref="Bytes"/> fall
    /// back by a part every few minutes.
    /// </summary>
    private long _muxed;

    /// <summary>
    /// Parts muxed so far, which names the next file. Separate from <see cref="Parts"/> because that
    /// counts what storage has taken, and the two differ for as long as a part is in flight.
    /// </summary>
    private int _muxedParts;

    public StreamRecorder(
        StreamHub hub,
        LiveOptions options,
        IServiceScopeFactory scopeFactory,
        ILogger logger,
        TimeSpan? duration,
        Func<string?>? classification = null,
        DetectionReference? detection = null,
        LiveMetrics? metrics = null)
    {
        _hub = hub;
        _options = options;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _classification = classification ?? (() => null);
        _detection = detection;
        _metrics = metrics;

        StartedAt = DateTimeOffset.UtcNow;
        _endsAt = StartedAt + (duration ?? TimeSpan.FromSeconds(options.DefaultRecordingSeconds));

        // The ceiling on one document. Disk no longer needs one, because a part is uploaded and
        // deleted as it completes; this exists so a recording nobody stops still ends somewhere
        // rather than growing a document without limit. A still-firing trigger starts the next.
        Deadline = StartedAt + TimeSpan.FromMinutes(options.MaxRecordingMinutes);

        FileName = $"{SafeName(hub.Name)}-{StartedAt:yyyyMMdd-HHmmss}.ts";

        _directory = System.IO.Path.Combine(
            options.RecordingDirectory is { Length: > 0 } directory
                ? directory
                : System.IO.Path.Combine(System.IO.Path.GetTempPath(), "storagedemo-live"),
            Id.ToString("N"));
    }

    public Guid Id { get; } = Guid.NewGuid();

    public DateTimeOffset StartedAt { get; }

    /// <summary>The ceiling, which no trigger can push past.</summary>
    public DateTimeOffset Deadline { get; }

    public DateTimeOffset EndsAt
    {
        get { lock (_gate) { return _endsAt; } }
    }

    public string FileName { get; }

    /// <summary>
    /// Everything captured so far, which grows through the recording rather than at its end.
    ///
    /// Written only by the muxing loop and <see cref="Parts"/> only by the storer, which is what
    /// makes two tasks safe here without a lock: each figure has one writer, and whoever reads them
    /// for a status response is reading a moment rather than a transaction, exactly as it was when
    /// one task wrote both.
    /// </summary>
    public long Bytes { get; private set; }

    /// <summary>Parts stored so far. Visible because it is the honest measure of progress.</summary>
    public int Parts { get; private set; }

    /// <summary>Set when the queue overflowed, which produces a real but short document.</summary>
    public bool Truncated { get; private set; }

    public Guid? DocumentId { get; private set; }

    public bool Finished { get; private set; }

    public RecordingStatus Status => new(Id, StartedAt, EndsAt, Bytes, Truncated);

    /// <summary>
    /// Moves the stop time later. A trigger arriving while a recording runs extends it rather than
    /// starting a second, so continuous detection produces one clip covering the whole event
    /// instead of a drift of overlapping near-duplicates. Someone wanting a separate file stops
    /// and starts.
    /// </summary>
    public void Extend(TimeSpan? duration)
    {
        lock (_gate)
        {
            var proposed = DateTimeOffset.UtcNow
                + (duration ?? TimeSpan.FromSeconds(_options.DefaultRecordingSeconds));

            if (proposed > _endsAt)
            {
                _endsAt = proposed > Deadline ? Deadline : proposed;
            }
        }
    }

    public void Stop() => _stop.Cancel();

    /// <summary>True when it is time to close, whether by its own stop time or by the ceiling.</summary>
    public bool Due() => DateTimeOffset.UtcNow >= EndsAt || DateTimeOffset.UtcNow >= Deadline;

    /// <summary>
    /// How many packets one recording of this stream is given to fall behind by, and the figure that
    /// decides whether a stall costs a document.
    ///
    /// Asked for in seconds and answered in packets, exactly as a viewer's is, because a packet is
    /// one demultiplexed frame and so a depth stated in packets means a different length of time on
    /// every stream that reaches it: twenty thousand of them is over thirteen minutes of a 25 fps
    /// camera and twenty seconds of a transport carrying a thousand packets a second. The short one
    /// is the one that mattered, and nothing in a packet count said it was there.
    ///
    /// Bounded both ways, which is where this differs from a viewer's and is the reason it can be
    /// derived from a sender's own declaration at all. A viewer's queue may only be clamped from
    /// above, because depth is delay for a viewer and a deep queue is the fault being fixed; a
    /// recorder accumulates no delay at all - it writes to a local file - so the only thing a queue
    /// deeper than it needs costs is memory. So the seconds are floored by
    /// <see cref="LiveOptions.RecorderQueuePackets"/>, which is the depth every recording had before
    /// this was derived at all, and capped by <see cref="LiveOptions.RecorderQueueMaxPackets"/>.
    ///
    /// The floor is not a formality, it is the guarantee. A rate read from what the encoder presented
    /// is a sender's claim in both directions: <see cref="StreamLayout"/> discards one too high, but
    /// one too low is a measurement as far as libav can tell - an H.264 sender is free to
    /// declare VUI timing of one frame a second and then send fifty - and believed, it would size
    /// this queue at a few hundred packets on a Fail subscription. Floored at what the option already
    /// said, no stream a sender can present gets a shallower queue than it got before, whatever it
    /// declares, and the seconds can then only ever make a queue deeper than it was.
    /// </summary>
    /// <param name="preroll">
    /// The rollback this recording will actually be given, which is part of the depth because
    /// <see cref="StreamHub.Subscribe"/> fills the queue from the buffer before a live packet reaches
    /// it. A seed that does not fit does not cost the part that did not fit: every seeded packet is
    /// offered as not starting a segment, so the first overflow faults a Fail subscription outright
    /// and the recording is over before it read anything.
    /// </param>
    public static int QueueDepth(StreamLayout layout, LiveOptions options, double preroll)
        => Math.Max(
            options.RecorderQueuePackets,
            layout.QueueDepth(options.RecorderQueueSeconds + preroll, options.RecorderQueueMaxPackets));

    /// <summary>
    /// Runs until its stop time, until it is stopped, or until the stream ends, storing a part
    /// every few minutes as it goes. Nothing about it depends on whoever asked for it still being
    /// there: closing the client, losing the client, or never having had one changes nothing.
    /// </summary>
    public async Task<Guid?> RunAsync(CancellationToken shutdown)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token, shutdown);

        var layout = _hub.Layout;

        if (layout is null)
        {
            _logger.LogWarning("'{Name}' has no layout yet, so there is nothing to record", _hub.Name);
            Finished = true;

            _metrics?.Recorded("empty");

            return null;
        }

        Directory.CreateDirectory(_directory);

        // Deeper than a viewer's, because overflowing here is not a skip. Exhausting it ends the
        // recording and marks the document truncated: dropping packets to keep going would write
        // a hole into a file that claims to be a recording, and silence is worse than stopping.
        //
        // The pre-roll is asked of the hub rather than taken from the option, because a rollback
        // begins at the keyframe at or before what was asked for and a coarse sender gives more than
        // it was asked for. QueueDepth says why a seed that does not fit is worse than it sounds.
        using var subscription = _hub.Subscribe(
            QueueDepth(layout, _options, _hub.ResolvePreroll(_options.PrerollSeconds)),
            OverflowPolicy.Fail,
            streamIndexes: [],
            preroll: _options.PrerollSeconds);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var documents = scope.ServiceProvider.GetRequiredService<IDocumentService>();

        var document = documents.BeginSegmented(FileName, ContentTypes.Guess(FileName), Describe());

        // Finished parts on their way to storage. Bounded, because the alternative to waiting for
        // storage is an unbounded pile of parts on a pod's disk, and this is the only queue in the
        // recording whose overflow is a pause rather than a lost document.
        var muxed = Channel.CreateBounded<MuxedPart>(
            new BoundedChannelOptions(_options.RecorderPendingParts)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = true,
            });

        var storing = StorePartsAsync(muxed, document);

        try
        {
            try
            {
                var packets = subscription.Packets
                    .ReadAllAsync(linked.Token)
                    .GetAsyncEnumerator(linked.Token);

                // The timeline runs across parts rather than restarting in each. Joined back together
                // they have to read as one continuous recording, which is exactly what a muxer
                // continuing where the last one left off produces.
                var timeline = 0d;
                var more = true;

                try
                {
                    while (more && !Due())
                    {
                        (more, timeline) = await WritePartAsync(
                            packets,
                            subscription,
                            layout,
                            timeline,
                            muxed.Writer);
                    }
                }
                finally
                {
                    await packets.DisposeAsync();
                }
            }
            finally
            {
                // Whether the loop ended or threw, nothing more will be muxed - so the storer is told
                // to finish what it is holding and awaited here, before the document is completed.
                // A recording that failed still keeps the parts it captured, and the last word on a
                // document cannot be written before its last part is in it.
                muxed.Writer.TryComplete();

                await storing;
            }

            if (Truncated)
            {
                _logger.LogError(
                    "The recording of '{Name}' ran out of queue and was truncated at {Bytes} bytes",
                    _hub.Name,
                    Bytes);
            }

            // The last word on what this document is: how long it ran, how many parts it took,
            // and whether it is short of what it should be. The probe cannot work any of that out,
            // and for a document in parts it never sees more than the first few minutes anyway.
            if (await document.CompleteAsync(Describe(), CancellationToken.None) is not { } stored)
            {
                _logger.LogInformation(
                    "The recording of '{Name}' captured nothing, so it is not stored",
                    _hub.Name);

                _metrics?.Recorded("empty");

                return null;
            }

            DocumentId = stored.Id;

            // Truncated is still stored: the document is real and short, and saying which it was is
            // the whole reason the flag exists.
            _metrics?.Recorded(Truncated ? "truncated" : "stored");

            _logger.LogInformation(
                "The recording of '{Name}' finished as {DocumentId}: {Parts} parts, {Bytes} bytes",
                _hub.Name,
                stored.Id,
                Parts,
                Bytes);

            return DocumentId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "The recording of '{Name}' failed", _hub.Name);

            _metrics?.Recorded("failed");

            return DocumentId;
        }
        finally
        {
            Finished = true;
            Cleanup();
        }
    }

    /// <summary>
    /// Writes one part to a local file and hands it over to be stored.
    /// </summary>
    /// <returns>Whether the stream is still running, and where the timeline has reached.</returns>
    private async Task<(bool More, double Timeline)> WritePartAsync(
        IAsyncEnumerator<MediaPacket> packets,
        PacketSubscription subscription,
        StreamLayout layout,
        double timeline,
        ChannelWriter<MuxedPart> muxed)
    {
        var path = System.IO.Path.Combine(_directory, $"{_muxedParts:D5}.ts");
        var until = DateTimeOffset.UtcNow + TimeSpan.FromMinutes(_options.RecordingPartMinutes);
        var more = true;
        long written;

        await using (var file = File.Create(path))
        {
            using var muxer = new PacketMuxer(file, layout, "mpegts", timeline);

            try
            {
                while (await packets.MoveNextAsync())
                {
                    muxer.Write(packets.Current);

                    // Counted across the whole recording, not this part, because what a caller
                    // asked about is the recording.
                    Bytes = _muxed + muxer.Written;

                    if (muxer.Fault is not null || Due() || DateTimeOffset.UtcNow >= until)
                    {
                        break;
                    }
                }

                more = !Due();
            }
            catch (OperationCanceledException)
            {
                more = false;
            }

            Truncated |= subscription.Faulted;
            muxer.Close();

            written = muxer.Written;
            timeline = muxer.TimelineSeconds;
        }

        if (written == 0)
        {
            Delete(path);

            return (more, timeline);
        }

        _muxed += written;
        _muxedParts++;
        Bytes = _muxed;

        // Where the upload used to be, and the one line this whole change is about. It waits only
        // once storage has fallen RecorderPendingParts whole parts behind, and that wait is now the
        // only moment in a recording when nothing is draining the packet queue.
        //
        // Not cancellable, for the reason the append itself never was: a muxed part is recorded
        // video, and a shutdown that dropped it on the floor would lose bytes the recorder has
        // already counted. The storer deletes the file once it has taken it.
        await muxed.WriteAsync(new MuxedPart(path, written), CancellationToken.None);

        return (more, timeline);
    }

    /// <summary>
    /// Stores finished parts, one at a time and in order, for as long as the recorder is muxing them.
    ///
    /// This is what takes a part upload off the read path. It used to run between two reads of the
    /// packet queue, so the queue had to hold everything the stream sent for as long as storage
    /// took - a five-minute part of a 25 Mbps contribution feed is about 940 MB, and fitting its
    /// upload inside a queue asked for in seconds would have wanted something like 115 Mbit/s
    /// sustained per concurrent recording. Nothing drained the channel while it happened and this
    /// subscription fails rather than skipping, so a slow storage backend did not cost a recording
    /// some latency, it cost the document. Now the muxing loop hands over a finished file and opens
    /// the next one, and the queue is spent only by an upload that has already fallen a whole part
    /// behind.
    ///
    /// One at a time and in order, because a segmented document is written in order: a part's key is
    /// its position and the parts are joined in that order on the way out. In order is also why this
    /// is one task rather than one per part - two uploads racing would store part four as part three
    /// whenever the smaller of them finished first - and it is why nothing else touches the document
    /// until this has finished.
    ///
    /// Never cancelled, for the same reason the append below has always been passed
    /// <see cref="CancellationToken.None"/>: these bytes are already recorded.
    /// </summary>
    private async Task StorePartsAsync(Channel<MuxedPart> muxed, SegmentedDocument document)
    {
        try
        {
            await foreach (var part in muxed.Reader.ReadAllAsync(CancellationToken.None))
            {
                await using (var stored = File.OpenRead(part.Path))
                {
                    await document.AppendAsync(stored, part.Bytes, Describe(), CancellationToken.None);
                }

                Parts++;

                _metrics?.RecordedBytes(part.Bytes);

                Delete(part.Path);
            }
        }
        catch (Exception ex)
        {
            // Told to the muxing loop rather than only thrown here. It waits on this channel whenever
            // storage has fallen behind, and a storer that died quietly would leave it waiting there
            // until the recording's own deadline - hours, on a document nobody is going to be able to
            // finish anyway.
            muxed.Writer.TryComplete(ex);

            throw;
        }
    }

    /// <summary>A part muxed to a local file and waiting to be stored.</summary>
    private readonly record struct MuxedPart(string Path, long Bytes);

    /// <summary>What the probe cannot work out, and what it never sees for a long recording.</summary>
    private Dictionary<string, string> Describe()
    {
        var metadata = new Dictionary<string, string>
        {
            ["Live stream"] = _hub.Name,
            ["Recording started"] = StartedAt.ToString("u"),
            ["Duration"] = MediaMetadataFormat.Duration((DateTimeOffset.UtcNow - StartedAt).TotalSeconds),
        };

        if (Parts > 1)
        {
            metadata["Recording parts"] = Parts.ToString();
        }

        if (Truncated)
        {
            metadata["Recording"] = "Truncated: the recorder could not keep up with the stream.";
        }

        // ponytail: the marking as it stands when the part is stored, not the highest seen over
        // the recording. Track the maximum if a stream ever changes marking mid-recording.
        if (_classification() is { } marking)
        {
            metadata["Classification"] = marking;
        }

        // Written on every part rather than only at the end, because a part is what gets stored: a
        // six-hour recording is a document from its first few minutes, and it has to say what
        // caused it from then on rather than once it finishes.
        if (_detection is not null)
        {
            metadata[DetectionReference.MetadataKey] = _detection.ToString();
        }

        return metadata;
    }

    private void Cleanup()
    {
        try
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
        catch (IOException)
        {
            // Temp files; the operating system will get them.
        }
    }

    private static void Delete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
        }
    }

    /// <summary>
    /// A stream name may hold slashes, because it is a resource path. A file name may not.
    /// The stream keeps its name; only the document's is flattened.
    /// </summary>
    private static string SafeName(string name)
    {
        var flattened = name.Replace('/', '-');

        foreach (var invalid in System.IO.Path.GetInvalidFileNameChars())
        {
            flattened = flattened.Replace(invalid, '_');
        }

        return flattened;
    }
}
