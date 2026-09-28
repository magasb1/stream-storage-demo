using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Storage;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Application;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The ways a recorder can lose a recording, or lose the truth about one.
///
/// None of them is visible to anything smaller than a real recorder with a stream arriving at it and
/// a storage backend doing something inconvenient, which is why there is no unit test here: a
/// channel that has stopped being read, a loop that re-enters itself for ever, and a document that
/// outlives the recording that failed are all behaviour rather than arithmetic.
/// </summary>
public sealed class StreamRecorderTests
{
    /// <summary>
    /// A part upload is held while the stream keeps arriving, against a queue far too small to have
    /// absorbed it.
    ///
    /// The assertion that matters is that the recorder captured more bytes while the upload was stuck
    /// than when it got stuck. Against the code this replaces it cannot: the append sat between two
    /// reads of the packet queue, so the figure stands still for as long as storage does and the
    /// subscription faults behind it. Everything else here - the outcome on the meter, the parts, the
    /// untruncated document - is the consequence, and it is the consequence that was destroying
    /// recordings.
    /// </summary>
    [Fact]
    public async Task A_held_part_upload_does_not_stop_the_recorder_draining_its_queue()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var root = Path.Combine(Path.GetTempPath(), $"storagedemo-recorder-{Guid.NewGuid():N}");

        // A queue of sixty-four packets, parts of six seconds, and no pre-roll: the smallest
        // arrangement in which an upload can outlast a part boundary. The real defaults cannot be
        // used, because a queue that holds five minutes of the sender below would need a test that
        // ran for five minutes to fill it.
        var options = new LiveOptions
        {
            RecordingDirectory = root,
            RecordingPartMinutes = 0.1,
            PrerollSeconds = 0,
            RecorderQueuePackets = 64,
            RecorderPendingParts = 1,
        };

        var storage = new HeldStorage();
        Task<Guid?>? recording = null;

        try
        {
            var repository = new FakeDocumentRepository();

            var documents = new DocumentService(
                storage,
                repository,
                new FakeMediaAnalyzer(),
                new InMemoryAnalysisQueue(),
                new InMemoryChangeFeed(),
                NullLogger<DocumentService>.Instance);

            await using var provider = new ServiceCollection()
                .AddSingleton<IDocumentService>(documents)
                .BuildServiceProvider();

            // Started before the recorder, because a counter is an event: a listener that attaches
            // afterwards sees nothing at all.
            using var metrics = new LiveMetrics();
            using var meters = new Meters(metrics);

            using var hub = new StreamHub("held-recorder", options, NullLogger.Instance, metrics);
            using var layout = Layout(videoFrameRate: 25);

            hub.Adopt(layout);

            var recorder = new StreamRecorder(
                hub,
                options,
                provider.GetRequiredService<IServiceScopeFactory>(),
                NullLogger.Instance,
                TimeSpan.FromSeconds(14),
                metrics: metrics);

            using var publishing = new CancellationTokenSource();

            var publisher = Publish(hub, publishing.Token);

            recording = recorder.RunAsync(CancellationToken.None);

            await storage.Reached.WaitAsync(TimeSpan.FromSeconds(30));

            var whenHeld = recorder.Bytes;

            // Long enough to cross the next part boundary and start the one after it, and to offer the
            // queue about three hundred and fifty packets while it does.
            await Task.Delay(TimeSpan.FromSeconds(7));

            var whileHeld = recorder.Bytes;

            storage.Release();

            var stored = await recording.WaitAsync(TimeSpan.FromSeconds(60));

            await publishing.CancelAsync();
            await publisher;

            Assert.True(
                whileHeld > whenHeld,
                $"the recorder stopped capturing while a part upload was held: {whenHeld} bytes when it "
                    + $"stuck, {whileHeld} seven seconds later");

            Assert.False(recorder.Truncated, "the recording was truncated");
            Assert.NotNull(stored);

            // Both outcomes asserted, not only the good one: a recorder that truncated and a recorder
            // that never ran would both report no truncation.
            Assert.Equal(0, meters.Count("live.recordings", "outcome", "truncated"));
            Assert.Equal(1, meters.Count("live.recordings", "outcome", "stored"));

            Assert.True(
                recorder.Parts > 1,
                $"only {recorder.Parts} part was stored, so no boundary was crossed while the upload was held");

            // Every part handed over was stored, and nothing was left on the pod's disk. Worth
            // asserting because the storer now owns the deleting: a part that reached storage and
            // stayed on disk would be the bound on disk quietly becoming the length of the recording.
            Assert.Equal(recorder.Parts, storage.Saves);
            Assert.False(
                Directory.Exists(Path.Combine(root, recorder.Id.ToString("N"))),
                "the local part files outlived the recording");

            // In order, which is what a second task doing the storing could have cost and the count
            // above would not have noticed. A part's key is its position in the document, so parts
            // stored out of order do not fail, they transpose minutes of the recording.
            Assert.Equal(
                Enumerable.Range(0, recorder.Parts).Select(part => $"{part:D5}").ToArray(),
                storage.Keys.Select(Position).ToArray());

            // And the document accounts for every byte the recorder claims, which is the other half
            // of the same thing: the parts are joined on the way out, so one that went missing
            // between the muxer and the document shows up here and nowhere else.
            var document = Assert.Contains(stored.Value, repository.Documents);

            Assert.Equal(recorder.Parts, document.Parts.Count);
            Assert.Equal(recorder.Bytes, document.Size);
        }
        finally
        {
            // Unconditionally, and before the directory goes: a timeout above would otherwise leave
            // the recorder running and blocked on a hold nobody released, while the delete below took
            // its part files out from under it.
            storage.Release();

            await Settle(recording, root);
        }
    }

    /// <summary>
    /// A part cannot be stored, and the document that survives says so.
    ///
    /// Everything about a failure here is already the way it should be except the last word. The
    /// parts that did reach storage stay readable, because <c>AppendAsync</c> upserts the row as it
    /// goes; the pod's disk is left clean; the recorder marks itself finished promptly so the
    /// coordinator frees the slot. What was missing is that the row left behind describes a whole
    /// recording. Nothing on it said it had failed, so an hour-long recording that died two parts in
    /// read as a complete twelve-minute one - in the one class whose stated position is that silence
    /// is worse than stopping, and which keeps <c>Truncated</c> precisely so that a short document
    /// says it is short.
    /// </summary>
    [Fact]
    public async Task A_recording_that_could_not_store_a_part_leaves_a_document_that_says_so()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var root = Path.Combine(Path.GetTempPath(), $"storagedemo-recorder-{Guid.NewGuid():N}");

        var options = new LiveOptions
        {
            RecordingDirectory = root,
            RecordingPartMinutes = 0.1,
            PrerollSeconds = 0,
            RecorderPendingParts = 1,
        };

        Task<Guid?>? recording = null;

        try
        {
            // The first part stores, the second does not. Which is the case worth testing: a
            // recording that failed before storing anything leaves nothing behind to be wrong about.
            var storage = new FailingStorage(failFrom: 2);
            var repository = new FakeDocumentRepository();

            var documents = new DocumentService(
                storage,
                repository,
                new FakeMediaAnalyzer(),
                new InMemoryAnalysisQueue(),
                new InMemoryChangeFeed(),
                NullLogger<DocumentService>.Instance);

            await using var provider = new ServiceCollection()
                .AddSingleton<IDocumentService>(documents)
                .BuildServiceProvider();

            using var metrics = new LiveMetrics();
            using var meters = new Meters(metrics);

            using var hub = new StreamHub("failing-recorder", options, NullLogger.Instance, metrics);
            using var layout = Layout(videoFrameRate: 25);

            hub.Adopt(layout);

            var recorder = new StreamRecorder(
                hub,
                options,
                provider.GetRequiredService<IServiceScopeFactory>(),
                NullLogger.Instance,
                TimeSpan.FromSeconds(60),
                metrics: metrics);

            using var publishing = new CancellationTokenSource();

            var publisher = Publish(hub, publishing.Token);

            recording = recorder.RunAsync(CancellationToken.None);

            // Well inside the sixty seconds it was asked for: the point is that it gives up when
            // storage does rather than running to its own end.
            var stored = await recording.WaitAsync(TimeSpan.FromSeconds(45));

            await publishing.CancelAsync();
            await publisher;

            Assert.Equal(1, meters.Count("live.recordings", "outcome", "failed"));

            // The part that made it is still there and still readable, which is the half of this that
            // was always right: a recording that lost its tail keeps its head.
            Assert.NotNull(stored);
            Assert.Equal(1, recorder.Parts);
            Assert.True(recorder.Bytes > 0);

            var document = Assert.Contains(stored.Value, repository.Documents);

            Assert.Single(document.Parts);

            // The assertion this test exists for. Not the wording, which will change, but that the
            // document carries the key at all: without it the row claims to be a finished recording.
            Assert.True(
                document.Metadata.TryGetValue("Recording", out var note),
                "the document of a failed recording says nothing about having failed");

            Assert.StartsWith("Incomplete", note);

            // And the pod's disk is clean, which matters more on this path than on the happy one:
            // whatever broke storage is likely to have broken it for every recording on the node.
            Assert.False(
                Directory.Exists(Path.Combine(root, recorder.Id.ToString("N"))),
                "the local part files outlived the failed recording");
        }
        finally
        {
            await Settle(recording, root);
        }
    }

    /// <summary>
    /// The stream ends while the recording still has fifty-nine minutes to run, and the recording
    /// ends with it rather than spinning until its deadline.
    ///
    /// A completed packet queue makes <c>MoveNextAsync</c> return false immediately and for ever.
    /// Read as "this part is finished" it sends the loop round again to open a part, read nothing,
    /// delete the file and do it all again - thousands of times a second, against the pod's disk,
    /// burning a core, for as long as the recording had left. Twelve hours, at the ceiling.
    ///
    /// Closing the hub is the honest way to provoke it: it is one of the two things that complete
    /// that queue - <c>StreamHub.Close</c> here, a Fail subscription overflowing in
    /// <c>PacketSubscription.Offer</c> there - and they complete it the same way, so this covers
    /// both. It is also the one that a fix written in terms of <c>Faulted</c> would miss. The
    /// recorder is deliberately not stopped first, which is what the production shutdown path
    /// happens to do and what was hiding this.
    /// </summary>
    [Fact]
    public async Task A_stream_that_ends_finishes_the_recording_instead_of_spinning()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var root = Path.Combine(Path.GetTempPath(), $"storagedemo-recorder-{Guid.NewGuid():N}");

        var options = new LiveOptions
        {
            RecordingDirectory = root,
            RecordingPartMinutes = 5,
            PrerollSeconds = 0,
            RecorderPendingParts = 1,
        };

        Task<Guid?>? recording = null;

        try
        {
            var storage = new FakeFileStorage();

            var documents = new DocumentService(
                storage,
                new FakeDocumentRepository(),
                new FakeMediaAnalyzer(),
                new InMemoryAnalysisQueue(),
                new InMemoryChangeFeed(),
                NullLogger<DocumentService>.Instance);

            await using var provider = new ServiceCollection()
                .AddSingleton<IDocumentService>(documents)
                .BuildServiceProvider();

            using var metrics = new LiveMetrics();
            using var meters = new Meters(metrics);

            using var hub = new StreamHub("ending-recorder", options, NullLogger.Instance, metrics);
            using var layout = Layout(videoFrameRate: 25);

            hub.Adopt(layout);

            var recorder = new StreamRecorder(
                hub,
                options,
                provider.GetRequiredService<IServiceScopeFactory>(),
                NullLogger.Instance,
                TimeSpan.FromHours(1),
                metrics: metrics);

            using var publishing = new CancellationTokenSource();

            var publisher = Publish(hub, publishing.Token);

            recording = recorder.RunAsync(CancellationToken.None);

            // Enough of a stream to be a recording worth finishing, and far less than the five-minute
            // part it is inside: the loop is mid-part when the queue goes out from under it.
            await Task.Delay(TimeSpan.FromSeconds(2));

            await publishing.CancelAsync();
            await publisher;

            hub.Close();

            // The whole assertion. Against the loop this replaces it never returns, and a timeout is
            // the only shape that failure has.
            var stored = await recording.WaitAsync(TimeSpan.FromSeconds(20));

            // Finished as a recording rather than merely stopped: what it captured before the stream
            // went away is a document, and one part of it.
            Assert.NotNull(stored);
            Assert.Equal(1, recorder.Parts);
            Assert.Single(storage.Objects);
            Assert.Equal(1, meters.Count("live.recordings", "outcome", "stored"));

            // And every byte it counted is in the one object it stored, so it left the loop at a part
            // boundary of its own making rather than part-way through anything.
            Assert.Equal(recorder.Bytes, storage.Objects.Values.Sum(o => (long)o.Length));
        }
        finally
        {
            await Settle(recording, root);
        }
    }

    /// <summary>The trailing part of the key, which is the part's position in the document.</summary>
    private static string Position(string key) => key.Split('/')[^1].Split('.')[0];

    /// <summary>
    /// Lets a recording finish before its directory is taken away, so that a failed assertion above
    /// reports itself rather than being buried under whatever the recorder throws when its files
    /// vanish mid-part.
    /// </summary>
    private static async Task Settle(Task<Guid?>? recording, string root)
    {
        if (recording is not null)
        {
            await Task.WhenAny(recording, Task.Delay(TimeSpan.FromSeconds(30)));
        }

        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>Packets into the hub at about fifty a second, until it is told to stop.</summary>
    private static Task Publish(StreamHub hub, CancellationToken stopping) => Task.Run(
        async () =>
        {
            var pts = 0L;

            for (var packet = 0; !stopping.IsCancellationRequested; packet++)
            {
                // A keyframe every ten, so the buffer has positions a recording could start from.
                hub.Publish(
                    new MediaPacket(0, new byte[1400], pts, pts, Duration: 3600, IsKeyframe: packet % 10 == 0),
                    pts);

                pts += 3600;

                await Task.Delay(20, CancellationToken.None);
            }
        },
        CancellationToken.None);

    /// <summary>
    /// A layout with the fields a recording needs and nothing else, built without a transport: a
    /// frame rate is what a demultiplexer reports after probing, and none of this needs a real one.
    /// </summary>
    private static unsafe StreamLayout Layout(int videoFrameRate)
    {
        var format = ffmpeg.avformat_alloc_context();

        try
        {
            var video = ffmpeg.avformat_new_stream(format, null);

            video->time_base = new AVRational { num = 1, den = 90_000 };
            video->avg_frame_rate = new AVRational { num = videoFrameRate, den = 1 };
            video->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_VIDEO;
            video->codecpar->codec_id = AVCodecID.AV_CODEC_ID_MPEG2VIDEO;
            video->codecpar->width = 320;
            video->codecpar->height = 240;

            return StreamLayout.From(format);
        }
        finally
        {
            ffmpeg.avformat_free_context(format);
        }
    }

    /// <summary>
    /// Storage that holds its first write until it is let go. It is the only way to put a stuck part
    /// upload and a running stream in the same place at the same time, which is the situation the
    /// recorder's queue exists to survive and used not to.
    /// </summary>
    private sealed class HeldStorage : IFileStorage
    {
        private readonly FakeFileStorage _inner = new();

        private readonly TaskCompletionSource _reached =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource _release =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly Lock _gate = new();

        private readonly List<string> _keys = [];

        private int _saves;

        /// <summary>Completes when the first write has arrived and is being held.</summary>
        public Task Reached => _reached.Task;

        public int Saves => Volatile.Read(ref _saves);

        /// <summary>The keys written, in the order they were written.</summary>
        public IReadOnlyList<string> Keys
        {
            get { lock (_gate) { return [.. _keys]; } }
        }

        public void Release() => _release.TrySetResult();

        public async Task SaveAsync(
            string key,
            Stream content,
            string? contentType,
            CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _saves) == 1)
            {
                _reached.TrySetResult();

                await _release.Task;
            }

            await _inner.SaveAsync(key, content, contentType, ct);

            // After the write rather than before it, so the order recorded is the order the parts
            // actually landed in storage and not the order they were offered.
            lock (_gate)
            {
                _keys.Add(key);
            }
        }

        public Task<Stream?> OpenReadAsync(string key, CancellationToken ct = default)
            => _inner.OpenReadAsync(key, ct);

        public Task<Stream?> OpenReadAsync(string key, long offset, CancellationToken ct = default)
            => _inner.OpenReadAsync(key, offset, ct);

        public Task DeleteAsync(string key, CancellationToken ct = default)
            => _inner.DeleteAsync(key, ct);

        public Task<bool> ExistsAsync(string key, CancellationToken ct = default)
            => _inner.ExistsAsync(key, ct);

        public IAsyncEnumerable<StorageObject> ListAsync(
            string prefix,
            CancellationToken ct = default)
            => _inner.ListAsync(prefix, ct);
    }

    /// <summary>
    /// Storage that works until it does not, which is what a backend losing its credentials or its
    /// bucket looks like from here: the parts before the failure are real and readable, and the
    /// recording has to end.
    /// </summary>
    private sealed class FailingStorage(int failFrom) : IFileStorage
    {
        private readonly FakeFileStorage _inner = new();

        private int _saves;

        public Task SaveAsync(
            string key,
            Stream content,
            string? contentType,
            CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref _saves) >= failFrom)
            {
                throw new IOException($"storage is gone (save {_saves} of '{key}')");
            }

            return _inner.SaveAsync(key, content, contentType, ct);
        }

        public Task<Stream?> OpenReadAsync(string key, CancellationToken ct = default)
            => _inner.OpenReadAsync(key, ct);

        public Task<Stream?> OpenReadAsync(string key, long offset, CancellationToken ct = default)
            => _inner.OpenReadAsync(key, offset, ct);

        public Task DeleteAsync(string key, CancellationToken ct = default)
            => _inner.DeleteAsync(key, ct);

        public Task<bool> ExistsAsync(string key, CancellationToken ct = default)
            => _inner.ExistsAsync(key, ct);

        public IAsyncEnumerable<StorageObject> ListAsync(
            string prefix,
            CancellationToken ct = default)
            => _inner.ListAsync(prefix, ct);
    }
}
