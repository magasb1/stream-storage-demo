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
/// ordinary object, and the local file deleted. Disk and memory are bounded by one part however
/// long the recording runs, and what has already been recorded survives the pod.
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

    /// <summary>Bytes of the parts already stored, so the running total survives a part roll.</summary>
    private long _stored;

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

    /// <summary>Everything stored so far, which grows through the recording rather than at its end.</summary>
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

        // Larger than a viewer's, because overflowing here is not a skip. Exhausting it ends the
        // recording and marks the document truncated: dropping packets to keep going would write
        // a hole into a file that claims to be a recording, and silence is worse than stopping.
        //
        // The pre-roll is part of the depth for the same reason it is for a viewer: Subscribe fills
        // the queue from the buffer before the first live packet, and a queue that cannot hold what
        // it is seeded with would fail this recording at the moment it started.
        using var subscription = _hub.Subscribe(
            layout.QueueDepth(
                _options.RecorderQueueSeconds + _options.PrerollSeconds,
                _options.RecorderQueuePackets),
            OverflowPolicy.Fail,
            streamIndexes: [],
            preroll: _options.PrerollSeconds);

        await using var scope = _scopeFactory.CreateAsyncScope();
        var documents = scope.ServiceProvider.GetRequiredService<IDocumentService>();

        var document = documents.BeginSegmented(FileName, ContentTypes.Guess(FileName), Describe());

        try
        {
            var packets = subscription.Packets.ReadAllAsync(linked.Token).GetAsyncEnumerator(linked.Token);

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
                        document);
                }
            }
            finally
            {
                await packets.DisposeAsync();
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
    /// Writes one part to a local file, stores it, and deletes the file.
    /// </summary>
    /// <returns>Whether the stream is still running, and where the timeline has reached.</returns>
    private async Task<(bool More, double Timeline)> WritePartAsync(
        IAsyncEnumerator<MediaPacket> packets,
        PacketSubscription subscription,
        StreamLayout layout,
        double timeline,
        SegmentedDocument document)
    {
        var path = System.IO.Path.Combine(_directory, $"{Parts:D5}.ts");
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
                    Bytes = _stored + muxer.Written;

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

        if (written > 0)
        {
            await using (var stored = File.OpenRead(path))
            {
                await document.AppendAsync(stored, written, Describe(), CancellationToken.None);
            }

            _stored += written;
            Parts++;
            Bytes = _stored;

            _metrics?.RecordedBytes(written);
        }

        Delete(path);

        return (more, timeline);
    }

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
