using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Api.Controllers;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// Live streaming through the real application, end to end: an encoder pointed at one address with
/// a name in its stream identifier, appearing without anything having been requested, showing a
/// preview that visibly updates, recordable and snapshottable with both landing in documents, and
/// disappearing when the feed stops.
///
/// It uses a real SRT sender, because everything interesting here is what libav actually does.
/// </summary>
public sealed class LiveStreamTests : IAsyncLifetime
{
    private const string Token = "live-test-token";

    /// <summary>
    /// Two natives, two scripts, and a test needs whichever halves it uses. The listening ports are
    /// libsrt's, so nothing here is accepted without it; the senders are still FFmpeg's SRT caller,
    /// which <see cref="HasSrt"/> asks about. A test that both listens and sends needs both, and a
    /// skip has to name the script that fixes the half that is missing.
    /// </summary>
    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-live-tests",
        Guid.NewGuid().ToString("N"));

    private readonly List<Process> _senders = [];

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private int _ingestPort;

    private static bool HasSrt() => FfmpegLibrary.InputProtocols().Contains("srt");

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_root);

        _ingestPort = FreePort();

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_root, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_root, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            builder.UseSetting("Live:IngestPort", _ingestPort.ToString());

            // Two, so the whole suite runs against a replica bound to a range rather than a single
            // port. Every test here still uses the first one; only the reconnect test below uses
            // the second.
            builder.UseSetting("Live:IngestPortCount", "2");
            builder.UseSetting("Live:ConsumptionPort", (_ingestPort + 5).ToString());
            builder.UseSetting("Live:PreviewIntervalSeconds", "1");
            builder.UseSetting("Live:RecordingDirectory", Path.Combine(_root, "recordings"));

            // Short, so a test produces several segments in a sensible time. In production this
            // is minutes: it is what bounds a pod's disk for a recording of any length.
            builder.UseSetting("Live:RecordingPartMinutes", "0.15");

            // Short, so a test can watch a feed stop and the stream disappear without waiting out
            // the production grace period.
            builder.UseSetting("Live:GracePeriodSeconds", "5");
            builder.UseSetting("Live:FeedTimeoutSeconds", "2");
            builder.UseEnvironment("Production");
        });

        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var sender in _senders)
        {
            Kill(sender);
        }

        _client.Dispose();
        await _factory.DisposeAsync();

        for (var attempt = 0; attempt < 3 && Directory.Exists(_root); attempt++)
        {
            try
            {
                Directory.Delete(_root, recursive: true);
            }
            catch (IOException)
            {
                await Task.Delay(200);
            }
        }
    }

    [Fact]
    public async Task The_service_reports_the_transports_it_can_actually_carry()
    {
        var status = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");

        Assert.NotNull(status);

        // A build choice, not a code one. Whichever way this instance was built, saying so is the
        // point: a caller should never have to guess.
        Assert.Equal(FfmpegLibrary.OutputProtocols().Contains("srt"), status.Transports.Contains("srt"));
    }

    /// <summary>
    /// Readiness is about whether this replica can serve media, not only whether it can reach a
    /// database. A pod that is reachable but not accepting is worse than one that is plainly
    /// absent, because the Service keeps sending encoders to it.
    /// </summary>
    [Fact]
    public async Task Readiness_means_the_media_ports_are_accepting()
    {
        // Only libsrt. Nothing is sent here, and both ports are opened without FFmpeg being asked.
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);

        var listeners = _factory.Services.GetRequiredService<LiveListeners>();

        var ready = await WaitAsync(
            async () => (await _client.GetAsync("/health/ready")).IsSuccessStatusCode,
            TimeSpan.FromSeconds(60));

        Assert.True(ready, "the replica never became ready");

        // Ready and not accepting would make the signal a lie, which is the whole point of it.
        Assert.True(listeners.IsListening(StreamIntent.Publish), "ready while ingest was not accepting");
        Assert.True(listeners.IsListening(StreamIntent.Subscribe), "ready while consumption was not accepting");
        Assert.Null(listeners.NotServing());
    }

    /// <summary>
    /// A replica that cannot open a media port takes itself out of the Service, rather than
    /// swallowing encoders it could never serve.
    ///
    /// The fault is set here rather than provoked. What sets it in production is libsrt being
    /// absent, which is decided once at start-up and cannot be arranged mid-process, and a replica
    /// that is genuinely without libsrt is already carrying the fault before this test runs. Setting
    /// it directly is the only version of this that says the same thing either way.
    /// </summary>
    [Fact]
    public async Task A_replica_that_cannot_serve_media_is_not_ready()
    {
        var listeners = _factory.Services.GetRequiredService<LiveListeners>();

        // Put back rather than cleared: on a host without libsrt the replica arrived here already
        // faulted, and clearing it would leave the rest of the fixture claiming it can serve.
        var existing = listeners.Fault;

        listeners.Fault = "libsrt is not loaded";

        try
        {
            var response = await _client.GetAsync("/health/ready");

            Assert.False(response.IsSuccessStatusCode, "a replica that cannot serve media stayed ready");
        }
        finally
        {
            listeners.Fault = existing;
        }
    }

    [Fact]
    public async Task The_endpoints_are_closed_without_the_token()
    {
        using var anonymous = _factory.CreateClient();

        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/live")).StatusCode);

        // The detection control plane, both the client's surface and the worker's, is guarded
        // the same way: the peer routes are how a worker writes into a stream.
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.PutAsJsonAsync("/api/live/detect/x", new DetectRequest(true, 1))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/live/detections/x")).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.PutAsJsonAsync("/api/live/peer/detector/x", new DetectorClaim("w"))).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.PostAsJsonAsync("/api/live/peer/detections/x", Vmti.Sample(320, 240))).StatusCode);
    }

    /// <summary>
    /// The detection control plane on one replica: the toggle round-trips, a worker's claim is a
    /// lease that a second worker cannot take, a posted VMTI frame is served back typed and raw,
    /// and clearing the toggle drops the worker so the listing tells the truth at once.
    /// </summary>
    [Fact]
    public async Task Detection_is_toggled_through_the_owner_and_a_worker_claims_it_and_posts_frames()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "live/detected";

        Push(name);
        Assert.NotNull(await WaitForStreamAsync(name, TimeSpan.FromSeconds(40)));

        var toggled = await _client.PutAsJsonAsync(
            $"/api/live/detect/{name}",
            new DetectRequest(true, 5, "yolo", ["truck", "person", "car"]));
        Assert.Equal(HttpStatusCode.OK, toggled.StatusCode);

        var stream = await toggled.Content.ReadFromJsonAsync<LiveStream>();
        Assert.NotNull(stream);
        Assert.True(stream.DetectionEnabled);
        Assert.Equal(5, stream.DetectionRate);
        Assert.Equal("yolo26", stream.DetectionModel);
        Assert.Equal(["person", "car", "truck"], stream.DetectionLabels);
        Assert.Null(stream.DetectionWorker);

        // Published at once, not on the next beat: a worker lists the registry, not the owner.
        Assert.True((await Get(name))!.DetectionEnabled);

        // Nothing posted yet is not found, which is the answer the client's probe relies on.
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync($"/api/live/detections/{name}")).StatusCode);

        var claimed = await _client.PutAsJsonAsync($"/api/live/peer/detector/{name}", new DetectorClaim("worker-1"));
        Assert.Equal(HttpStatusCode.OK, claimed.StatusCode);
        Assert.Equal("worker-1", (await claimed.Content.ReadFromJsonAsync<LiveStream>())!.DetectionWorker);

        // The same worker renews; another is refused while the lease is live.
        Assert.Equal(HttpStatusCode.OK, (await _client.PutAsJsonAsync($"/api/live/peer/detector/{name}", new DetectorClaim("worker-1"))).StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, (await _client.PutAsJsonAsync($"/api/live/peer/detector/{name}", new DetectorClaim("worker-2"))).StatusCode);

        var sample = Vmti.Sample(320, 240);
        Assert.Equal(HttpStatusCode.Accepted, (await _client.PostAsJsonAsync($"/api/live/peer/detections/{name}", sample)).StatusCode);

        // A packet that is not the frame beside it is refused: the two answers must agree.
        var lying = sample with { Raw = [.. sample.Raw[..^1], (byte)(sample.Raw[^1] ^ 1)] };
        Assert.Equal(HttpStatusCode.BadRequest, (await _client.PostAsJsonAsync($"/api/live/peer/detections/{name}", lying)).StatusCode);

        var served = await _client.GetFromJsonAsync<VmtiSample>($"/api/live/detections/{name}");
        Assert.NotNull(served);
        Assert.Equal(sample.Frame, served.Frame with { Detections = sample.Frame.Detections });
        Assert.Equal(sample.Frame.Detections, served.Frame.Detections);
        Assert.Equal(Convert.ToHexString(sample.Raw), Convert.ToHexString(served.Raw));

        var cleared = await _client.PutAsJsonAsync($"/api/live/detect/{name}", new DetectRequest(false));
        Assert.Equal(HttpStatusCode.OK, cleared.StatusCode);

        stream = await Get(name);
        Assert.False(stream!.DetectionEnabled);
        Assert.Null(stream.DetectionWorker);

        // A release from a worker that no longer holds the stream changes nothing.
        Assert.Equal(HttpStatusCode.NotFound, (await _client.DeleteAsync($"/api/live/peer/detector/{name}?worker=worker-1")).StatusCode);
    }

    [Fact]
    public async Task A_manual_stream_may_not_name_a_transport_outside_the_allowed_list()
    {
        // file:// would turn "create a stream" into "read anything on the disk".
        var response = await _client.PostAsJsonAsync(
            "/api/live/manual",
            new CreateManualStreamRequest("nope", "file:///etc/passwd"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task A_manual_stream_with_an_unusable_name_is_refused_rather_than_repaired()
    {
        var response = await _client.PostAsJsonAsync(
            "/api/live/manual",
            new CreateManualStreamRequest("../../etc/passwd", $"udp://127.0.0.1:{FreePort()}"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Acting_on_a_stream_that_is_not_on_air_is_reported_as_missing()
    {
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await _client.PostAsync("/api/live/snapshot/nothing-here", null)).StatusCode);

        Assert.Equal(
            HttpStatusCode.NotFound,
            (await _client.DeleteAsync("/api/live/stream/nothing-here")).StatusCode);
    }

    /// <summary>
    /// The end state, in one test, because these steps only mean anything together: nothing is
    /// requested, the stream names itself, and what it produces are documents.
    /// </summary>
    [Fact]
    public async Task An_encoder_that_names_itself_appears_and_can_be_recorded_and_snapshotted()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "live/match-of-the-day";

        // Nothing is posted first. The encoder simply pushes.
        Push(name);

        var stream = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(stream);
        Assert.Equal(LiveStreamState.Live, stream.State);
        Assert.False(stream.Manual);
        Assert.True(stream.Packets > 0, "no packets were carried");

        // The preview is the picture now, so two of them taken a few seconds apart differ.
        var first = await WaitForPreviewAsync(name, TimeSpan.FromSeconds(40));
        Assert.NotNull(first);
        Assert.Equal(0xFF, first[0]);
        Assert.Equal(0xD8, first[1]);

        await Task.Delay(TimeSpan.FromSeconds(6));

        var second = await Preview(name);
        Assert.NotNull(second);
        Assert.False(first.SequenceEqual(second), "the preview froze rather than following the stream");

        // A snapshot is a fresh decode at full resolution, and it lands in documents.
        var snapshot = await _client.PostAsync($"/api/live/snapshot/{name}", null);
        Assert.Equal(HttpStatusCode.OK, snapshot.StatusCode);

        var snapshotDocument = await WaitForDocumentAsync(
            document => document.FileName.EndsWith(".jpg", StringComparison.Ordinal),
            TimeSpan.FromSeconds(30));

        Assert.NotNull(snapshotDocument);
        Assert.True(snapshotDocument.Size > 0, "the snapshot is empty");

        // Nobody asked for it on behalf of a detection, so it says nothing about one. This is the
        // half of provenance that must not change: a person pressing the button gets what they
        // always got.
        Assert.DoesNotContain("Detection", snapshotDocument.Metadata.Keys);

        // A short recording, which reaches back into the buffer and becomes a document at the end.
        var started = await _client.PostAsJsonAsync($"/api/live/record/{name}", new RecordRequest(5));
        Assert.Equal(HttpStatusCode.Accepted, started.StatusCode);

        var recording = await WaitForDocumentAsync(
            document => document.FileName.EndsWith(".ts", StringComparison.Ordinal),
            TimeSpan.FromSeconds(60));

        Assert.NotNull(recording);
        Assert.True(recording.Size > 0, "the recording is empty");
        Assert.Equal("video/mp2t", recording.ContentType);
        Assert.DoesNotContain("Detection", recording.Metadata.Keys);

        // Named by stream and start time, so several from one stream sit together and read as
        // related. The name's slash is flattened, because a file name may not carry one.
        Assert.StartsWith("live-match-of-the-day-", recording.FileName, StringComparison.Ordinal);
    }

    /// <summary>
    /// Provenance, on one stream because the steps only mean anything together: a detection
    /// triggers a snapshot and a recording through the same routes a person uses, both documents
    /// name the detection that caused them, the recording names it from its first stored part
    /// rather than only once it finishes, and the detection finds both documents again afterwards.
    /// </summary>
    [Fact]
    public async Task A_detection_that_triggered_a_capture_is_named_by_every_document_it_produced()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "detected-camera";

        // Microseconds, because that is what the VMTI precision timestamp carries and what tells
        // one frame from the next.
        var frame = new DateTimeOffset(2026, 9, 12, 10, 31, 2, TimeSpan.Zero).AddTicks(1234560);
        var detection = new DetectionReference(name, frame, 7);
        var reference = detection.ToString();

        Push(name);

        Assert.NotNull(await WaitForStreamAsync(name, TimeSpan.FromSeconds(40)));

        var snapshot = await _client.PostAsJsonAsync($"/api/live/snapshot/{name}", detection);
        Assert.Equal(HttpStatusCode.OK, snapshot.StatusCode);

        var picture = await WaitForDocumentAsync(
            document => document.FileName.EndsWith(".jpg", StringComparison.Ordinal),
            TimeSpan.FromSeconds(30));

        Assert.NotNull(picture);
        Assert.Equal(reference, picture.Metadata["Detection"]);

        // Long enough to cross several part boundaries at the shortened part length above, so that
        // what is asserted below is about parts rather than about one file.
        var started = await _client.PostAsJsonAsync(
            $"/api/live/record/{name}",
            new RecordRequest(25, detection));

        Assert.Equal(HttpStatusCode.Accepted, started.StatusCode);

        var recording = await WaitForDocumentAsync(
            document => document.FileName.EndsWith(".ts", StringComparison.Ordinal),
            TimeSpan.FromSeconds(40));

        Assert.NotNull(recording);

        // It appeared with its first part and already names the detection: a part is what gets
        // stored, so a six-hour recording cannot wait until the end to say what caused it.
        Assert.Equal(reference, recording.Metadata["Detection"]);

        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { Recording: null },
                TimeSpan.FromSeconds(90)),
            "the recording never finished");

        var finished = await Get(recording.Id);

        Assert.NotNull(finished);
        Assert.Equal(reference, finished.Metadata["Detection"]);
        Assert.True(
            int.Parse(finished.Metadata["Recording parts"], CultureInfo.InvariantCulture) > 1,
            "the recording was a single part, so nothing here was proved about the others");

        // And the other direction, which is the point: from a detection to everything it produced.
        var found = await _client.GetFromJsonAsync<List<DocumentResponse>>(ByDetection(detection));

        Assert.Equal(
            new[] { picture.Id, recording.Id }.OrderBy(id => id),
            found!.Select(document => document.Id).OrderBy(id => id));

        // The target beside it in the very same frame is a different detection, which is why the
        // target id is part of the key rather than the frame alone.
        var neighbour = await _client.GetFromJsonAsync<List<DocumentResponse>>(
            ByDetection(detection with { TargetId = 8 }));

        Assert.Empty(neighbour!);
    }

    private static string ByDetection(DetectionReference detection)
        => "/api/documents/by-detection"
            + $"?stream={Uri.EscapeDataString(detection.Stream)}"
            + $"&timestamp={Uri.EscapeDataString(detection.Timestamp.ToString("O", CultureInfo.InvariantCulture))}"
            + $"&targetId={detection.TargetId}";

    /// <summary>
    /// A long recording is written in segments and stored as it goes, so no pod ever holds the
    /// whole thing, and read back as one file so that nobody opening it can tell.
    ///
    /// This is the end of the claim the whole segmented path exists to make: several objects in
    /// storage, one document, and what comes out of the document is a single playable recording of
    /// about the right length.
    /// </summary>
    [Fact]
    public async Task A_long_recording_is_stored_in_segments_and_read_back_as_one_file()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "long-recorder";

        Push(name);

        Assert.NotNull(await WaitForStreamAsync(name, TimeSpan.FromSeconds(40)));

        // Long enough to cross several segment boundaries at the shortened segment length above.
        var started = await _client.PostAsJsonAsync($"/api/live/record/{name}", new RecordRequest(30));
        Assert.Equal(HttpStatusCode.Accepted, started.StatusCode);

        var document = await WaitForDocumentAsync(
            d => d.FileName.StartsWith("long-recorder-", StringComparison.Ordinal)
                && d.FileName.EndsWith(".ts", StringComparison.Ordinal),
            TimeSpan.FromSeconds(40));

        Assert.NotNull(document);

        // The document exists while the recording is still running, which is the point of storing
        // segments as they complete. The stream itself says when the recording is over; size
        // standing still would only mean the gap between two segments.
        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { Recording: null },
                TimeSpan.FromSeconds(90)),
            "the recording never finished");

        var finished = await Get(document.Id);
        Assert.NotNull(finished);
        Assert.True(finished.Size > 0, "the recording is empty");

        // One document, whatever it took to write it.
        var listed = await _client.GetFromJsonAsync<List<DocumentResponse>>("/api/documents");
        Assert.Single(listed!, d => d.FileName.StartsWith("long-recorder-", StringComparison.Ordinal));

        // Several objects behind it, which is what keeps a pod from holding hours of video.
        Assert.True(
            Directory.Exists(Path.Combine(_root, "files", "recordings")),
            "the segments were not stored under their own prefix");

        var segments = Directory
            .GetFiles(Path.Combine(_root, "files", "recordings"), "*.ts", SearchOption.AllDirectories);

        Assert.True(segments.Length > 1, $"expected several segments, got {segments.Length}");

        // And what comes back out is one playable recording, not a pile of pieces.
        var whole = Path.Combine(_root, "downloaded.ts");

        await using (var response = await _client.GetStreamAsync($"/api/documents/{document.Id}/content"))
        await using (var file = File.Create(whole))
        {
            await response.CopyToAsync(file);
        }

        Assert.Equal(finished.Size, new FileInfo(whole).Length);

        var duration = Probe(whole);

        // About as long as it was asked to record. The pre-roll makes it a little longer, and the
        // segment boundaries must not have cost anything in between.
        Assert.True(duration > 20, $"the joined recording is only {duration:0.#}s long");

        // Seeking is what makes it usable from the document list, and the content endpoint serves
        // ranges because the joined stream is seekable.
        using var ranged = new HttpRequestMessage(HttpMethod.Get, $"/api/documents/{document.Id}/content");
        ranged.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(finished.Size - 1000, null);

        var tail = await _client.SendAsync(ranged);

        Assert.Equal(HttpStatusCode.PartialContent, tail.StatusCode);
        Assert.Equal(1000, (await tail.Content.ReadAsByteArrayAsync()).Length);
    }

    /// <summary>
    /// A feed that stops leaves the stream interrupted rather than removing it, and it is gone
    /// only after the grace period. A tile that vanishes and returns is worse than one showing a
    /// state.
    /// </summary>
    [Fact]
    public async Task A_feed_that_stops_is_interrupted_first_and_gone_after_the_grace_period()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "camera-that-leaves";

        var sender = Push(name);

        Assert.NotNull(await WaitForStreamAsync(name, TimeSpan.FromSeconds(40)));

        Kill(sender);

        var interrupted = await WaitAsync(
            async () => await Get(name) is { State: LiveStreamState.Interrupted },
            TimeSpan.FromSeconds(30));

        Assert.True(interrupted, "the stream was not reported as interrupted");

        var gone = await WaitAsync(
            async () => await Get(name) is null,
            TimeSpan.FromSeconds(40));

        Assert.True(gone, "the stream was still listed after its grace period");
    }

    /// <summary>
    /// A live name is locked: while <c>demo</c> is live, nobody else may publish <c>demo</c>.
    ///
    /// Both halves matter and the second is the one worth the test. A refusal that silently
    /// interrupted the incumbent would be worse than the take-over it replaces, so the first
    /// publisher is checked for still being the same connection and still delivering afterwards.
    ///
    /// The wait before the second sender is the handshake cache, not slack: the callback runs on
    /// libsrt's receiver thread and cannot read the registry, so it answers from a copy taken once
    /// a beat, and the name is locked within a beat of being claimed rather than instantly.
    /// </summary>
    [Fact]
    public async Task A_second_publisher_of_a_live_name_is_refused_and_the_first_is_undisturbed()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "contested-camera";

        Push(name);

        var first = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(first);

        await Task.Delay(TimeSpan.FromSeconds(3));

        var before = await Get(name);

        Assert.NotNull(before);

        // Started through the shared helper rather than through Push, because this one's stderr is
        // the evidence and only that helper drains it.
        var second = SrtSenders.StartSender(_ingestPort, $"#!::r={name},m=publish");
        _senders.Add(second);

        Assert.True(
            await SrtSenders.WasRefused(second, TimeSpan.FromSeconds(10)),
            $"the second publisher was not turned away: {SrtSenders.Complaints([second])}");

        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { Packets: var packets } && packets > before.Packets,
                TimeSpan.FromSeconds(15)),
            "the first publisher stopped delivering once the second was refused");

        var after = await Get(name);

        Assert.NotNull(after);
        Assert.Equal(LiveStreamState.Live, after.State);

        // The same connection throughout. A new identifier here would mean the refused publisher
        // had been let in and taken the stream over after all.
        Assert.Equal(before.ConnectionId, after.ConnectionId);
        Assert.Equal(before.StartedAt, after.StartedAt);
    }

    /// <summary>
    /// A reconnect under the same name resumes the same stream rather than creating a second one.
    /// That is what makes the name the identity rather than an incidental label.
    ///
    /// It is also where the name lock lets go. The second <c>Push</c> is a different process
    /// presenting a name this replica already holds, and it is admitted because the feed is
    /// interrupted: the lock follows the feed, not the entry, which is why an encoder that drops can
    /// always come back.
    /// </summary>
    [Fact]
    public async Task A_reconnect_under_the_same_name_resumes_the_same_stream()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "camera-that-returns";

        var first = Push(name);
        var before = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(before);

        Kill(first);

        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { State: LiveStreamState.Interrupted },
                TimeSpan.FromSeconds(30)),
            "the stream never became interrupted");

        Push(name);

        // The reconnect itself, not merely the stream reading live again. A feed that has just
        // been killed keeps delivering for a moment, so "live" on its own proves nothing; a new
        // connection identifier is the only thing that says this is a second attempt.
        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { State: LiveStreamState.Live } resumed
                    && resumed.ConnectionId != before.ConnectionId,
                TimeSpan.FromSeconds(60)),
            "the stream never picked up a second connection");

        var after = await Get(name);

        Assert.NotNull(after);

        // Same stream, resumed rather than replaced: it kept the moment it first went on air.
        Assert.Equal(before.StartedAt, after.StartedAt);

        var listed = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");
        Assert.Single(listed!.Streams, stream => stream.Name == name);
    }

    /// <summary>
    /// The same reconnect, landing on a different port of the same replica.
    ///
    /// Ingest may bind a range of ports so that libsrt gives the replica more than one receive
    /// worker thread, and a sender spread across that range has no reason to come back to the port
    /// it left. The name has to be the identity for that to be safe, so this pins that a port is
    /// carried nowhere: not into the claim, not into the entry, not into what a viewer sees.
    /// </summary>
    [Fact]
    public async Task A_reconnect_on_another_ingest_port_resumes_the_same_stream()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "camera-that-moves-port";

        var first = Push(name);
        var before = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(before);

        Kill(first);

        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { State: LiveStreamState.Interrupted },
                TimeSpan.FromSeconds(30)),
            "the stream never became interrupted");

        Push(name, _ingestPort + 1);

        Assert.True(
            await WaitAsync(
                async () => await Get(name) is { State: LiveStreamState.Live } resumed
                    && resumed.ConnectionId != before.ConnectionId,
                TimeSpan.FromSeconds(60)),
            "the second ingest port never accepted the returning sender");

        var after = await Get(name);

        Assert.NotNull(after);
        Assert.Equal(before.StartedAt, after.StartedAt);

        var listed = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");
        Assert.Single(listed!.Streams, stream => stream.Name == name);
    }

    /// <summary>
    /// The two answers Phase 9 exists to give, through the real application: which stream is broken,
    /// and how much this replica is carrying.
    ///
    /// The first is per stream and comes out of the API, where an operator with a thousand streams
    /// already looks. The second is per pod and comes out of the meter, where an autoscaler looks
    /// and where a stream name must never appear.
    ///
    /// The health figures are asserted as zero because a loopback stream at 600 kbit/s does not lose
    /// packets. That is the honest half of this test and also its limit: it pins that the figures
    /// are read, carried and serialised, and it cannot pin that a broken stream reports a non-zero
    /// one, which needs load this suite has no way to generate. The property name is asserted on the
    /// raw JSON because that, not the C# record, is what an operator's tooling reads.
    /// </summary>
    [Fact]
    public async Task A_live_stream_reports_its_health_on_the_api_and_the_replica_reports_what_it_holds()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "camera-with-a-health-figure";

        var metrics = _factory.Services.GetRequiredService<LiveMetrics>();
        using var meters = new Meters(metrics);

        Assert.Equal(0, meters.Value("live.streams.owned"));

        Push(name);

        var stream = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(stream);
        Assert.Equal(0, stream.PacketsLost);
        Assert.Equal(0, stream.PacketsDropped);

        var listed = await _client.GetStringAsync("/api/live");

        Assert.Contains("\"packetsLost\":", listed, StringComparison.Ordinal);
        Assert.Contains("\"packetsDropped\":", listed, StringComparison.Ordinal);

        // Published by the heartbeat rather than by the claim, so it arrives a beat after the stream
        // does. Waiting for it is also what proves the heartbeat is what publishes it.
        Assert.True(
            await WaitAsync(
                () => Task.FromResult(meters.Value("live.streams.owned") == 1),
                TimeSpan.FromSeconds(20)),
            $"the meter reported {meters.Value("live.streams.owned")} streams owned rather than one");
    }

    /// <summary>
    /// The viewer count, through two real players on the consumption port rather than one: the
    /// interesting failure for a counter touched from more than one connection's thread is getting
    /// the arithmetic wrong under concurrency, which a single viewer cannot expose. Each player
    /// connects with <c>m=request</c>, the same envelope <see cref="LiveReplicas.Watch"/> uses for a
    /// real viewer rather than <see cref="Push"/>'s <c>m=publish</c>.
    /// </summary>
    [Fact]
    public async Task Two_viewers_are_counted_and_each_leaving_is_subtracted()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "camera-with-an-audience";

        // Started before the players, because a counter is an event: a listener attached afterwards
        // sees nothing of what it already counted.
        using var meters = new Meters(_factory.Services.GetRequiredService<LiveMetrics>());

        Push(name);

        var stream = await WaitForStreamAsync(name, TimeSpan.FromSeconds(40));

        Assert.NotNull(stream);
        Assert.Equal(0, stream.Viewers);

        var consumptionPort = _ingestPort + 5;
        var first = SrtSenders.StartPlayer(consumptionPort, $"#!::r={name},m=request");
        var second = SrtSenders.StartPlayer(consumptionPort, $"#!::r={name},m=request");
        _senders.Add(first);
        _senders.Add(second);

        Assert.True(
            await WaitAsync(async () => (await Get(name))?.Viewers == 2, TimeSpan.FromSeconds(20)),
            $"never saw two viewers; last count was {(await Get(name))?.Viewers}");

        Kill(first);

        Assert.True(
            await WaitAsync(async () => (await Get(name))?.Viewers == 1, TimeSpan.FromSeconds(20)),
            $"the count never fell back to one after a viewer left; last was {(await Get(name))?.Viewers}");

        Kill(second);

        Assert.True(
            await WaitAsync(async () => (await Get(name))?.Viewers == 0, TimeSpan.FromSeconds(20)),
            $"the count never fell back to zero; last was {(await Get(name))?.Viewers}");

        // Two players, two sessions, and no more. The session count is of viewers actually served:
        // the consumption port re-attaches every quarter second while it has nothing to serve, so
        // counting where the attach is decided rather than where it succeeds turns this figure into
        // a count of retries the moment anything goes wrong.
        Assert.Equal(2, meters.Total("live.viewer.sessions"));

        Assert.All(
            meters.Of("live.viewer.sessions"),
            session => Assert.Equal(
                new KeyValuePair<string, object?>("route", "direct"),
                Assert.Single(session.Tags)));
    }

    /// <summary>
    /// A transport carrying MISB KLV alongside the picture, through the real ingest: the list
    /// shows the stream carries it and what it is marked, the route serves the minimum set at
    /// the values that were sent with the raw packet beside them, and a snapshot keeps the marking.
    /// </summary>
    [Fact]
    public async Task A_stream_carrying_klv_reports_its_marking_and_serves_the_decoded_minimum_set()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(HasSrt(), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");

        const string name = "uas/klv-carrier";

        var video = Path.Combine(_root, "video.ts");
        var carrier = Path.Combine(_root, "klv.ts");

        SrtSenders.Render(video, seconds: 90);
        Misb.WriteTransportStream(video, carrier, Misb.MinimumSet(), intervalSeconds: 0.1);

        Push(name, file: carrier);

        LiveStream? stream = null;

        // Sixty rather than forty: the file is rendered, remuxed and pushed at wall-clock speed
        // first, and under a loaded machine that alone has eaten most of forty.
        Assert.True(
            await WaitAsync(
                async () => (stream = await Get(name)) is { HasKlv: true, KlvAt: not null },
                TimeSpan.FromSeconds(60)),
            $"the stream never reported KLV: {stream}");

        Assert.Contains("klv", stream!.Layout, StringComparison.Ordinal);

        var response = await _client.GetAsync($"/api/live/klv/{name}");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        var sample = await response.Content.ReadFromJsonAsync<KlvSample>();

        Assert.NotNull(sample);
        Assert.Equal(Convert.ToHexString(Misb.MinimumSet()), Convert.ToHexString(sample.Raw));
        Assert.Equal(Misb.Known.Classification, stream.Classification);

        // The file was written with a PES timestamp on every KLV packet, so it is aligned by that
        // rather than by tag 2.
        Assert.Equal(KlvAlignment.PresentationTimestamp, sample.Alignment);
        Assert.NotNull(sample.ReferencePts);

        Assert.NotNull(sample.Fields);
        Assert.Equal(Misb.Known.Timestamp, sample.Fields.Timestamp);
        Assert.Equal(Misb.Known.Classification, sample.Fields.Classification);
        Assert.Equal(Misb.Known.SensorLatitude, sample.Fields.SensorLatitude!.Value, 1e-6);
        Assert.Equal(Misb.Known.SensorLongitude, sample.Fields.SensorLongitude!.Value, 1e-6);
        Assert.Equal(Misb.Known.FrameCenterLatitude, sample.Fields.FrameCenterLatitude!.Value, 1e-6);
        Assert.Equal(Misb.Known.FrameCenterLongitude, sample.Fields.FrameCenterLongitude!.Value, 1e-6);
        Assert.Equal(Misb.Known.SensorTrueAltitude, sample.Fields.SensorTrueAltitude!.Value, 0.5);

        var snapshot = await _client.PostAsync($"/api/live/snapshot/{name}", null);
        Assert.Equal(HttpStatusCode.OK, snapshot.StatusCode);

        var document = await WaitForDocumentAsync(
            document => document.FileName.StartsWith("uas-klv-carrier-", StringComparison.Ordinal),
            TimeSpan.FromSeconds(30));

        Assert.NotNull(document);
        Assert.Equal(Misb.Known.Classification, document.Metadata["Classification"]);
    }

    private async Task<DocumentResponse?> Get(Guid id)
    {
        var documents = await _client.GetFromJsonAsync<List<DocumentResponse>>("/api/documents");

        return documents?.FirstOrDefault(d => d.Id == id);
    }

    /// <summary>How many seconds of media the file holds, as the bundled ffprobe reads it.</summary>
    private static double Probe(string path)
    {
        string[] arguments =
        [
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            path,
        ];

        using var process = BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffprobe, arguments, readOutput: true);

        var output = process.StandardOutput.ReadToEnd().Trim();
        process.WaitForExit();

        return double.TryParse(output, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var seconds)
            ? seconds
            : 0;
    }

    private async Task<LiveStream?> Get(string name)
    {
        var response = await _client.GetAsync($"/api/live/stream/{name}");

        return response.StatusCode == HttpStatusCode.OK
            ? await response.Content.ReadFromJsonAsync<LiveStream>()
            : null;
    }

    private async Task<byte[]?> Preview(string name)
    {
        var response = await _client.GetAsync($"/api/live/preview/{name}");

        return response.StatusCode == HttpStatusCode.OK
            ? await response.Content.ReadAsByteArrayAsync()
            : null;
    }

    private async Task<LiveStream?> WaitForStreamAsync(string name, TimeSpan timeout)
    {
        LiveStream? found = null;

        await WaitAsync(
            async () => (found = await Get(name)) is { Packets: > 0 },
            timeout);

        return found;
    }

    private async Task<byte[]?> WaitForPreviewAsync(string name, TimeSpan timeout)
    {
        byte[]? found = null;

        await WaitAsync(async () => (found = await Preview(name)) is not null, timeout);

        return found;
    }

    private async Task<DocumentResponse?> WaitForDocumentAsync(
        Func<DocumentResponse, bool> match,
        TimeSpan timeout)
    {
        DocumentResponse? found = null;

        await WaitAsync(
            async () =>
            {
                var documents = await _client.GetFromJsonAsync<List<DocumentResponse>>("/api/documents");
                found = documents?.FirstOrDefault(match);

                return found is not null;
            },
            timeout);

        return found;
    }

    private static async Task<bool> WaitAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await condition())
                {
                    return true;
                }
            }
            catch (HttpRequestException)
            {
                // The host is still starting; the next pass will tell the truth.
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        return false;
    }

    /// <summary>
    /// An encoder pushing at the ingest port with a name in its stream identifier, which is the
    /// whole setup: nothing is requested first.
    /// </summary>
    /// <param name="file">
    /// A transport stream to push as it is, every stream in it, instead of a synthetic picture.
    /// This is how a stream carrying something the command line cannot synthesise, such as KLV,
    /// reaches the service.
    /// </param>
    private Process Push(string name, int? port = null, string? file = null)
    {
        var identifier = Uri.EscapeDataString($"#!::r={name},m=publish");
        var target = $"srt://127.0.0.1:{port ?? _ingestPort}?mode=caller&streamid={identifier}";

        string[] source = file is null
            // A one second keyframe interval, so the buffer holds fine segments rather than two
            // coarse ones.
            ? ["-f", "lavfi", "-i", "testsrc=size=320x240:rate=15", "-c:v", "mpeg2video", "-b:v", "800k", "-g", "15"]
            : ["-i", file, "-map", "0", "-c", "copy"];

        // Paced at wall-clock speed either way: SRT is a connection, not a file copy.
        string[] arguments = ["-hide_banner", "-loglevel", "error", "-re", .. source, "-f", "mpegts", target];

        var sender = BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffmpeg, arguments);
        _senders.Add(sender);

        return sender;
    }

    private static void Kill(Process sender)
    {
        try
        {
            if (!sender.HasExited)
            {
                sender.Kill(entireProcessTree: true);
                sender.WaitForExit(5000);
            }
        }
        catch (InvalidOperationException)
        {
        }
    }

    /// <summary>
    /// A free port from a low, fixed range rather than an ephemeral one. Windows reserves
    /// stretches of the dynamic range, so a port can be handed out and then refuse an explicit
    /// bind moments later, which looks exactly like a listener that will not start.
    /// </summary>
    private static int _nextPort = 9600;

    private static int FreePort()
    {
        var start = Interlocked.Add(ref _nextPort, 20);

        for (var port = start; port < start + 200; port++)
        {
            try
            {
                using var socket = new System.Net.Sockets.Socket(
                    System.Net.Sockets.AddressFamily.InterNetwork,
                    System.Net.Sockets.SocketType.Dgram,
                    System.Net.Sockets.ProtocolType.Udp);

                socket.Bind(new IPEndPoint(IPAddress.Any, port));

                return port;
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
        }

        throw new InvalidOperationException("No free port in the test range.");
    }
}
