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
/// The two halves of the one thing that can make a recorder lose a document: its packet queue
/// overflowing, which does not cost it a moment of picture but ends the recording and marks the
/// document truncated.
///
/// One half is structural and needs a real recording to show: a part upload used to run between two
/// reads of that queue with nothing draining it, so a slow storage backend and a queue measured in
/// anything short of a whole part's worth of stream were together enough to truncate a recording of
/// exactly the feeds people care most about. That is what the held upload below proves, and it is
/// the only way to prove it - no unit can see a channel that has stopped being read.
///
/// The other half is arithmetic, and it is where the sender gets a say: the depth is derived from
/// what the encoder presented about itself, and an encoder is free to present something untrue in
/// either direction.
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
            RecorderQueueSeconds = 1,
            RecorderQueuePackets = 64,
            RecorderQueueMaxPackets = 64,
            RecorderPendingParts = 1,
        };

        try
        {
            var storage = new HeldStorage();

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

            // Started before the recorder, because a counter is an event: a listener that attaches
            // afterwards sees nothing at all.
            using var metrics = new LiveMetrics();
            using var meters = new Meters(metrics);

            using var hub = new StreamHub("held-recorder", options, NullLogger.Instance, metrics);
            using var layout = Layout(videoFrameRate: 25);

            hub.Adopt(layout);

            // Stated rather than assumed, because the whole test rests on it: what arrives during the
            // hold below is several times this, so nothing but a queue that is still being read can
            // get the recording through.
            Assert.Equal(64, StreamRecorder.QueueDepth(layout, options, preroll: 0));

            var recorder = new StreamRecorder(
                hub,
                options,
                provider.GetRequiredService<IServiceScopeFactory>(),
                NullLogger.Instance,
                TimeSpan.FromSeconds(14),
                metrics: metrics);

            using var publishing = new CancellationTokenSource();

            var publisher = Publish(hub, publishing.Token);
            var recording = recorder.RunAsync(CancellationToken.None);

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
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    /// <summary>
    /// The arithmetic, at the two ends a sender can reach and the one in the middle that a real
    /// camera sits at.
    ///
    /// A recorder's depth is floored by the packet count that used to be the whole answer, and the
    /// floor is the guarantee rather than a tidiness: a rate derived from what an encoder presented is
    /// a claim in both directions, and the one too low is the dangerous one here. A sender declaring a
    /// frame a second and sending fifty would otherwise size the one queue in this service whose
    /// overflow destroys its artefact at three hundred packets.
    /// </summary>
    [Fact]
    public void A_recorders_queue_is_never_shallower_than_the_packets_it_was_always_given()
    {
        Assert.SkipUnless(Ffmpeg.IsPresent, "No FFmpeg. Run scripts/fetch-ffmpeg.sh.");

        FfmpegLibrary.EnsureLoaded();

        var options = new LiveOptions();

        using var camera = Layout(videoFrameRate: 25);
        using var slow = Layout(videoFrameRate: 1);
        using var claimed = Layout(videoFrameRate: 10_000);
        using var withAudio = Layout(videoFrameRate: 25, sampleRate: 48_000, frameSize: 1_024);
        using var fast = Layout(videoFrameRate: 240);

        // Five minutes of a 25 fps camera is 7,500 packets, which is less than the figure every
        // recording had before any of this was derived. It gets that figure, so nothing changes for
        // the commonest stream in the service.
        Assert.Equal(20_000, StreamRecorder.QueueDepth(camera, options, preroll: 5));

        // And the same for the claim in each direction. One frame a second is three hundred packets
        // of five minutes, and ten thousand is discarded outright and sized as a stream that declared
        // nothing - both land on the floor, which is the point of having one.
        Assert.Equal(20_000, StreamRecorder.QueueDepth(slow, options, preroll: 5));
        Assert.Equal(20_000, StreamRecorder.QueueDepth(claimed, options, preroll: 5));

        // Audio is what first takes a stream past the floor: 71.875 packets a second over five
        // minutes and five seconds of pre-roll. The old figure was 278 seconds of this stream and
        // called it the same 20,000 it called thirteen minutes of the camera above.
        Assert.Equal(21_922, StreamRecorder.QueueDepth(withAudio, options, preroll: 5));

        // The fast end, which is what the seconds are actually for. The old packet count gave this
        // transport under a minute and a half and said nothing about it; the ceiling now gives it
        // four minutes, and the ceiling is a memory figure rather than a media one.
        Assert.Equal(60_000, StreamRecorder.QueueDepth(fast, options, preroll: 5));
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
    /// A layout with the fields a depth is derived from and nothing else, built without a transport:
    /// a frame rate is what a demultiplexer reports after probing, and none of this needs a real one.
    /// </summary>
    private static unsafe StreamLayout Layout(int videoFrameRate, int sampleRate = 0, int frameSize = 0)
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

            if (sampleRate > 0)
            {
                var audio = ffmpeg.avformat_new_stream(format, null);

                audio->time_base = new AVRational { num = 1, den = sampleRate };
                audio->codecpar->codec_type = AVMediaType.AVMEDIA_TYPE_AUDIO;
                audio->codecpar->codec_id = AVCodecID.AV_CODEC_ID_AAC;
                audio->codecpar->sample_rate = sampleRate;
                audio->codecpar->frame_size = frameSize;
            }

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

        private int _saves;

        /// <summary>Completes when the first write has arrived and is being held.</summary>
        public Task Reached => _reached.Task;

        public int Saves => Volatile.Read(ref _saves);

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
