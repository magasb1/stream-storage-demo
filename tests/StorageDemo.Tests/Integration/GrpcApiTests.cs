using System.Net.Http.Json;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;
using StorageDemo.Api.Grpc;
using StorageDemo.Grpc;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// Drives the real application over a real gRPC channel: filesystem storage, LiteDB metadata and
/// the full upload, download, watch and delete path. Nothing is stubbed out.
/// </summary>
public sealed class GrpcApiTests : IAsyncLifetime
{
    private const string Token = "grpc-test-token";

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-grpc-tests",
        Guid.NewGuid().ToString("N"));

    private WebApplicationFactory<Program> _factory = null!;
    private GrpcChannel _channel = null!;
    private StorageDemo.Grpc.Documents.DocumentsClient _client = null!;

    public ValueTask InitializeAsync()
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_root, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_root, "db", "app.db"));
            // The monitor is exercised directly in the reconciler tests; a timer here only adds flake.
            builder.UseSetting("StorageMonitor:Enabled", "false");

            // Live is on with a token, so the guard is exercised; nothing is sent to the ports.
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            builder.UseSetting("Live:IngestPort", SrtSenders.FreePort().ToString());
            builder.UseSetting("Live:ConsumptionPort", SrtSenders.FreePort().ToString());
            builder.UseSetting("Retention:Enabled", "true");
            builder.UseSetting("Retention:MaxAgeDays", "30");
            builder.UseEnvironment("Production");
        });

        // The in-memory test server speaks HTTP/2 to gRPC through this handler.
        _channel = GrpcChannel.ForAddress(
            _factory.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = _factory.Server.CreateHandler() });

        _client = new StorageDemo.Grpc.Documents.DocumentsClient(_channel);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Dispose();
        await _factory.DisposeAsync();

        // LiteDB releases its log file just after the host goes away. Cleaning up a temp directory
        // is housekeeping, so a couple of retries and then let it be; failing the test over it
        // would report a problem that does not exist.
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
    public async Task GetProviders_reports_the_configured_combination()
    {
        var providers = await _client.GetProvidersAsync(new Empty());

        Assert.Equal("FileSystem", providers.Storage);
        Assert.Equal("LiteDb", providers.Database);
    }

    [Fact]
    public async Task GetProviders_says_when_documents_expire()
    {
        var providers = await _client.GetProvidersAsync(new Empty());

        Assert.True(providers.RetentionEnabled);
        Assert.Equal(30, providers.RetentionMaxAgeDays);
    }

    [Fact]
    public async Task Live_rpcs_refuse_a_missing_or_wrong_token_and_document_rpcs_ignore_it()
    {
        var name = new LiveStreamName { Name = "guarded" };

        var missing = await Assert.ThrowsAsync<RpcException>(
            () => _client.SnapshotLiveAsync(new SnapshotLiveRequest { Name = "guarded" }).ResponseAsync);
        var wrong = await Assert.ThrowsAsync<RpcException>(
            () => _client.RecordLiveAsync(new RecordLiveRequest { Name = "guarded" }, WithToken("nope")).ResponseAsync);
        var listing = await Assert.ThrowsAsync<RpcException>(() => _client.ListLiveAsync(new Empty()).ResponseAsync);
        var klv = await Assert.ThrowsAsync<RpcException>(() => _client.GetLiveKlvAsync(name).ResponseAsync);
        var detect = await Assert.ThrowsAsync<RpcException>(
            () => _client.SetLiveDetectionAsync(new SetLiveDetectionRequest { Name = "guarded", Enabled = true }).ResponseAsync);
        var detections = await Assert.ThrowsAsync<RpcException>(() => _client.GetLiveDetectionsAsync(name).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, missing.StatusCode);
        Assert.Equal(StatusCode.Unauthenticated, wrong.StatusCode);
        Assert.Equal(StatusCode.Unauthenticated, listing.StatusCode);
        Assert.Equal(StatusCode.Unauthenticated, klv.StatusCode);
        Assert.Equal(StatusCode.Unauthenticated, detect.StatusCode);
        Assert.Equal(StatusCode.Unauthenticated, detections.StatusCode);
        using var watch = _client.WatchLiveDetections(name);
        var streaming = await Assert.ThrowsAsync<RpcException>(() => watch.ResponseStream.MoveNext(CancellationToken.None));
        Assert.Equal(StatusCode.Unauthenticated, streaming.StatusCode);

        // The token is a live concern only; documents never asked for one.
        await _client.ListAsync(new Empty());
    }

    [Fact]
    public async Task Live_rpcs_accept_the_configured_token()
    {
        var listed = await _client.ListLiveAsync(new Empty(), WithToken(Token));
        Assert.True(listed.Enabled);

        // Past the guard and into the service, which has no such stream to act on.
        var failure = await Assert.ThrowsAsync<RpcException>(
            () => _client.SnapshotLiveAsync(
                new SnapshotLiveRequest { Name = "absent" },
                WithToken(Token)).ResponseAsync);

        Assert.Equal(StatusCode.NotFound, failure.StatusCode);
    }

    [Fact]
    public async Task ListLive_carries_the_health_figures()
    {
        // Written straight into the registry, which is what another replica's heartbeat is.
        var registry = _factory.Services.GetRequiredService<ILiveStreamRegistry>();
        var entry = LiveReplicas.Entry("lossy", "elsewhere", DateTimeOffset.UtcNow) with
        {
            PacketsLost = 3,
            PacketsDropped = 7,
        };
        await registry.UpsertAsync(entry);

        var listed = await _client.ListLiveAsync(new Empty(), WithToken(Token));
        var stream = Assert.Single(listed.Streams, s => s.Name == "lossy");

        Assert.Equal(3, stream.PacketsLost);
        Assert.Equal(7, stream.PacketsDropped);
    }

    [Fact]
    public async Task ListLive_carries_klv_presence_and_leaves_an_unmarked_stream_unmarked()
    {
        var registry = _factory.Services.GetRequiredService<ILiveStreamRegistry>();
        var at = new DateTimeOffset(2026, 9, 12, 10, 0, 0, TimeSpan.Zero);

        await registry.UpsertAsync(LiveReplicas.Entry("marked", "elsewhere", DateTimeOffset.UtcNow) with
        {
            HasKlv = true,
            KlvAt = at,
            Classification = "SECRET",
        });
        await registry.UpsertAsync(LiveReplicas.Entry("unmarked", "elsewhere", DateTimeOffset.UtcNow) with
        {
            HasKlv = true,
            KlvAt = at,
        });

        var listed = await _client.ListLiveAsync(new Empty(), WithToken(Token));
        var marked = Assert.Single(listed.Streams, s => s.Name == "marked");
        var unmarked = Assert.Single(listed.Streams, s => s.Name == "unmarked");

        Assert.True(marked.HasKlv);
        Assert.Equal(at, marked.LastKlvAt.ToDateTimeOffset());
        Assert.Equal("SECRET", marked.Classification);

        // Null on the record is absent on the wire, not an empty string: a client tells UNMARKED
        // from a blank marking by asking whether the field is there.
        Assert.True(unmarked.HasKlv);
        Assert.False(unmarked.HasClassification);
    }

    [Fact]
    public async Task GetLiveKlv_of_an_unknown_stream_reports_not_found()
    {
        var failure = await Assert.ThrowsAsync<RpcException>(
            () => _client.GetLiveKlvAsync(new LiveStreamName { Name = "absent" }, WithToken(Token)).ResponseAsync);

        Assert.Equal(StatusCode.NotFound, failure.StatusCode);
    }

    /// <summary>
    /// Not found, never unimplemented: the client probes GetLiveDetections with an empty name to
    /// learn whether the server has detection at all, and disables the button on Unimplemented.
    /// </summary>
    [Fact]
    public async Task The_detection_calls_answer_not_found_for_an_unknown_stream_rather_than_unimplemented()
    {
        var detections = await Assert.ThrowsAsync<RpcException>(
            () => _client.GetLiveDetectionsAsync(new LiveStreamName { Name = string.Empty }, WithToken(Token)).ResponseAsync);
        var toggle = await Assert.ThrowsAsync<RpcException>(
            () => _client.SetLiveDetectionAsync(new SetLiveDetectionRequest { Name = "absent", Enabled = true }, WithToken(Token)).ResponseAsync);

        Assert.Equal(StatusCode.NotFound, detections.StatusCode);
        Assert.Equal(StatusCode.NotFound, toggle.StatusCode);
    }

    /// <summary>
    /// The detection control plane across two replicas: the toggle set on the one that does not
    /// own the stream reaches the owner and comes back in the listing, and a VMTI frame a worker
    /// posted to the owner is served from both, typed and raw.
    /// </summary>
    [Fact]
    public async Task Detection_is_toggled_and_served_from_the_owner_and_from_a_replica_that_does_not_own_the_stream()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "live/grpc-detect";

        await using var replicas = new LiveReplicas();
        var aIngest = SrtSenders.FreePort();
        var a = replicas.Start("pod-a", aIngest);
        var b = replicas.Start("pod-b", SrtSenders.FreePort(), peer: a);

        replicas.Send(aIngest, name);

        await LiveReplicas.Until(
            async () => await replicas.Registry.GetAsync(name) is { Packets: > 0 },
            TimeSpan.FromSeconds(40),
            "A never reported the stream");

        var token = new Metadata { { LiveTokenInterceptor.Header, LiveReplicas.Token } };

        using var viaB = GrpcChannel.ForAddress(b.Server.BaseAddress, new GrpcChannelOptions { HttpHandler = b.Server.CreateHandler() });
        var clientB = new StorageDemo.Grpc.Documents.DocumentsClient(viaB);

        // Set through B, which forwards to A and answers with the stream as A now describes it.
        var toggled = await clientB.SetLiveDetectionAsync(
            new SetLiveDetectionRequest
            {
                Name = name,
                Enabled = true,
                Rate = 5,
                Model = "yolo26",
                Labels = { "person", "car", "truck" },
            },
            token);

        Assert.True(toggled.DetectionEnabled);
        Assert.Equal(5, toggled.DetectionRate);
        Assert.Equal("yolo26", toggled.DetectionModel);
        Assert.Equal(["person", "car", "truck"], toggled.DetectionLabels);
        Assert.False(toggled.HasDetectionWorker);
        Assert.Equal("pod-a", toggled.Owner);

        // A worker posts to the owner directly, having read its address from the listing.
        var sample = Vmti.Sample(320, 240);
        using var owner = replicas.Client(a);
        Assert.Equal(
            System.Net.HttpStatusCode.Accepted,
            (await owner.PostAsJsonAsync($"/api/live/peer/detections/{name}", sample)).StatusCode);

        foreach (var host in new[] { a, b })
        {
            using var channel = GrpcChannel.ForAddress(host.Server.BaseAddress, new GrpcChannelOptions { HttpHandler = host.Server.CreateHandler() });
            var client = new StorageDemo.Grpc.Documents.DocumentsClient(channel);

            var listed = Assert.Single((await client.ListLiveAsync(new Empty(), token)).Streams, s => s.Name == name);
            Assert.True(listed.DetectionEnabled);
            Assert.Equal(5, listed.DetectionRate);
            Assert.Equal("yolo26", listed.DetectionModel);
            Assert.Equal(["person", "car", "truck"], listed.DetectionLabels);

            var served = await client.GetLiveDetectionsAsync(new LiveStreamName { Name = name }, token);
            var expected = sample.Frame.Detections[0];
            var target = Assert.Single(served.Targets);

            Assert.Equal(sample.Frame.Timestamp, served.Timestamp.ToDateTimeOffset());
            Assert.Equal(320, served.FrameWidth);
            Assert.Equal(240, served.FrameHeight);
            Assert.Equal(Convert.ToHexString(sample.Raw), Convert.ToHexString(served.Raw.Span));
            Assert.Equal(expected.Id, target.Id);
            Assert.Equal((expected.Left, expected.Top, expected.Right, expected.Bottom), (target.Left, target.Top, target.Right, target.Bottom));
            Assert.Equal(expected.ConfidencePercent, target.ConfidencePercent);
            Assert.Equal("dog", target.OntologyClass);
            Assert.Equal(expected.Track!.Id.ToString(), target.TrackId);
            Assert.Equal(StorageDemo.Grpc.VmtiTrackStatus.Active, target.TrackStatus);
        }

        var cleared = await clientB.SetLiveDetectionAsync(new SetLiveDetectionRequest { Name = name, Enabled = false }, token);
        Assert.False(cleared.DetectionEnabled);
    }

    /// <summary>
    /// A transport carrying MISB KLV through the real ingest, read back over gRPC from the owner
    /// and from a replica that does not own it, which has to fetch the packet from the owner.
    /// </summary>
    [Fact]
    public async Task GetLiveKlv_serves_the_decoded_set_from_the_owner_and_from_a_replica_that_does_not_own_the_stream()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "uas/grpc-klv";

        await using var replicas = new LiveReplicas();
        var aIngest = SrtSenders.FreePort();
        var a = replicas.Start("pod-a", aIngest);
        var b = replicas.Start("pod-b", SrtSenders.FreePort(), peer: a);

        var video = Path.Combine(replicas.Root, "video.ts");
        var carrier = Path.Combine(replicas.Root, "klv.ts");
        SrtSenders.Render(video, seconds: 90);
        Misb.WriteTransportStream(video, carrier, Misb.MinimumSet(), intervalSeconds: 0.1);

        replicas.Send(aIngest, name, file: carrier);

        await LiveReplicas.Until(
            async () => await replicas.Registry.GetAsync(name) is { HasKlv: true, KlvAt: not null },
            TimeSpan.FromSeconds(40),
            "A never reported KLV");

        var token = new Metadata { { LiveTokenInterceptor.Header, LiveReplicas.Token } };

        foreach (var host in new[] { a, b })
        {
            using var channel = GrpcChannel.ForAddress(
                host.Server.BaseAddress,
                new GrpcChannelOptions { HttpHandler = host.Server.CreateHandler() });
            var client = new StorageDemo.Grpc.Documents.DocumentsClient(channel);

            var listed = await client.ListLiveAsync(new Empty(), token);
            var stream = Assert.Single(listed.Streams, s => s.Name == name);
            Assert.True(stream.HasKlv);
            Assert.Equal(Misb.Known.Classification, stream.Classification);

            var sample = await client.GetLiveKlvAsync(new LiveStreamName { Name = name }, token);

            Assert.Equal(Convert.ToHexString(Misb.MinimumSet()), Convert.ToHexString(sample.Raw.Span));
            Assert.Equal(StorageDemo.Grpc.KlvAlignment.PresentationTimestamp, sample.Alignment);
            Assert.True(sample.HasReferencePts);
            Assert.NotNull(sample.Fields);
            Assert.Equal(Misb.Known.Timestamp, sample.Fields.Timestamp.ToDateTimeOffset());
            Assert.Equal(Misb.Known.Classification, sample.Fields.Classification);
            Assert.Equal(Misb.Known.MissionId, sample.Fields.MissionId);
            Assert.Equal(Misb.Known.PlatformDesignation, sample.Fields.PlatformDesignation);
            Assert.Equal(Misb.Known.Version, sample.Fields.Version);
            Assert.Equal(Misb.Known.SensorLatitude, sample.Fields.SensorLatitude, 1e-6);
            Assert.Equal(Misb.Known.SensorLongitude, sample.Fields.SensorLongitude, 1e-6);
            Assert.Equal(Misb.Known.SensorTrueAltitude, sample.Fields.SensorTrueAltitude, 0.5);
            Assert.Equal(Misb.Known.FrameCenterLatitude, sample.Fields.FrameCenterLatitude, 1e-6);
            Assert.Equal(Misb.Known.FrameCenterLongitude, sample.Fields.FrameCenterLongitude, 1e-6);
            Assert.Equal(Misb.Known.SlantRange, sample.Fields.SlantRange, 0.01);

            // The one item outside the minimum set rides along raw, keyed by its tag.
            Assert.Equal(Misb.Known.TailNumber, sample.Fields.Unparsed[4].ToByteArray());
        }
    }

    /// <summary>
    /// The client's surface for provenance, both ways round: a detector triggers a snapshot over
    /// gRPC and names the detection, the stored document carries it, and asking by that same
    /// detection finds it again. The timestamp crosses the wire as a protobuf Timestamp and has to
    /// come back as the same microsecond, which is the half that would break quietly.
    /// </summary>
    [Fact]
    public async Task SnapshotLive_carries_the_detection_and_ListByDetection_finds_what_it_produced()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "grpc-detected-camera";

        await using var replicas = new LiveReplicas();
        var ingest = SrtSenders.FreePort();
        var a = replicas.Start("pod-a", ingest);

        replicas.Send(ingest, name);

        await LiveReplicas.Until(
            async () => await replicas.Registry.GetAsync(name) is { HasPreview: true },
            TimeSpan.FromSeconds(40),
            "the stream never arrived");

        using var channel = GrpcChannel.ForAddress(
            a.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = a.Server.CreateHandler() });
        var client = new StorageDemo.Grpc.Documents.DocumentsClient(channel);
        var token = new Metadata { { LiveTokenInterceptor.Header, LiveReplicas.Token } };

        var frame = new DateTimeOffset(2026, 9, 12, 10, 31, 2, TimeSpan.Zero).AddTicks(1234560);
        var detection = new DetectionReferenceMessage
        {
            Stream = name,
            Timestamp = Timestamp.FromDateTimeOffset(frame),
            TargetId = 7,
        };

        var stored = await client.SnapshotLiveAsync(
            new SnapshotLiveRequest { Name = name, Detection = detection },
            token);

        var document = await client.GetAsync(stored, token);

        Assert.Equal(
            new DetectionReference(name, frame, 7).ToString(),
            Assert.Single(document.Metadata, entry => entry.Key == "Detection").Value);

        var found = await client.ListByDetectionAsync(detection, token);

        Assert.Equal(stored.Id, Assert.Single(found.Documents).Id);

        // A different target in the same frame produced nothing, so it finds nothing.
        var neighbour = detection.Clone();
        neighbour.TargetId = 8;

        Assert.Empty((await client.ListByDetectionAsync(neighbour, token)).Documents);
    }

    /// <summary>
    /// The picture from a replica that does not own the stream, which in a cluster is most of
    /// them. Two applications in one process sharing a registry; see <see cref="LiveReplicas"/>.
    /// </summary>
    [Fact]
    public async Task DownloadLivePreview_is_answered_by_a_replica_that_does_not_own_the_stream()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "proxied-camera";

        await using var replicas = new LiveReplicas();
        var aIngest = SrtSenders.FreePort();
        var a = replicas.Start("pod-a", aIngest);
        var b = replicas.Start("pod-b", SrtSenders.FreePort(), peer: a);

        replicas.Send(aIngest, name);

        await LiveReplicas.Until(
            async () => await replicas.Registry.GetAsync(name) is { HasPreview: true },
            TimeSpan.FromSeconds(40),
            "A never decoded a preview");

        using var channel = GrpcChannel.ForAddress(
            b.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = b.Server.CreateHandler() });
        var viaB = new StorageDemo.Grpc.Documents.DocumentsClient(channel);

        using var call = viaB.DownloadLivePreview(new LiveStreamName { Name = name });
        var picture = await ReadAllAsync(call.ResponseStream);

        Assert.True(picture.Length > 2 && picture[0] == 0xFF && picture[1] == 0xD8, "not a JPEG");
    }

    private static Metadata WithToken(string token) => new() { { LiveTokenInterceptor.Header, token } };

    [Fact]
    public async Task Upload_then_download_round_trips_the_bytes()
    {
        var payload = "the quick brown fox"u8.ToArray();

        var uploaded = await UploadAsync("notes.txt", "text/plain", payload);

        Assert.Equal("notes.txt", uploaded.FileName);
        Assert.Equal(payload.Length, uploaded.Size);
        Assert.Equal($"documents/{uploaded.Id}/notes.txt", uploaded.StorageKey);

        Assert.Equal(payload, await DownloadAsync(uploaded.Id));
    }

    [Fact]
    public async Task Upload_streams_a_file_larger_than_one_chunk()
    {
        var payload = new byte[300 * 1024];
        Random.Shared.NextBytes(payload);

        var uploaded = await UploadAsync("big.bin", "application/octet-stream", payload);

        Assert.Equal(payload.Length, uploaded.Size);
        Assert.Equal(payload, await DownloadAsync(uploaded.Id));
    }

    [Fact]
    public async Task Uploaded_documents_appear_in_the_listing_and_disappear_after_delete()
    {
        var uploaded = await UploadAsync("listed.txt", "text/plain", "hello"u8.ToArray());

        var listed = await _client.ListAsync(new Empty());
        Assert.Contains(listed.Documents, d => d.Id == uploaded.Id);

        await _client.DeleteAsync(new DocumentId { Id = uploaded.Id });

        var afterDelete = await _client.ListAsync(new Empty());
        Assert.DoesNotContain(afterDelete.Documents, d => d.Id == uploaded.Id);
    }

    [Fact]
    public async Task A_lying_content_type_is_replaced_by_what_the_bytes_say()
    {
        // A JPEG announced as HTML. Believing the client would mean serving it back as markup.
        var jpeg = await File.ReadAllBytesAsync(await SampleJpegAsync());

        var uploaded = await UploadAsync("not-really.html", "text/html", jpeg);

        Assert.Equal("image/jpeg", uploaded.ContentType);
    }

    [Fact]
    public async Task A_format_with_no_signature_keeps_the_declared_type()
    {
        // Plain text has no magic bytes; absence of a signature is not evidence of lying.
        var uploaded = await UploadAsync("notes.txt", "text/plain", "just some words"u8.ToArray());

        Assert.Equal("text/plain", uploaded.ContentType);
    }

    [Fact]
    public async Task Get_of_an_unknown_id_reports_not_found()
    {
        var failure = await Assert.ThrowsAsync<RpcException>(
            () => _client.GetAsync(new DocumentId { Id = Guid.NewGuid().ToString() }).ResponseAsync);

        Assert.Equal(StatusCode.NotFound, failure.StatusCode);
    }

    [Fact]
    public async Task A_malformed_id_is_rejected_rather_than_treated_as_missing()
    {
        var failure = await Assert.ThrowsAsync<RpcException>(
            () => _client.GetAsync(new DocumentId { Id = "not-a-guid" }).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, failure.StatusCode);
    }

    [Fact]
    public async Task An_upload_that_does_not_start_with_metadata_is_rejected()
    {
        using var call = _client.Upload();
        await call.RequestStream.WriteAsync(new UploadRequest { Chunk = ByteString.CopyFrom("x"u8) });
        await call.RequestStream.CompleteAsync();

        var failure = await Assert.ThrowsAsync<RpcException>(() => call.ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, failure.StatusCode);
    }

    [Fact]
    public async Task Watch_pushes_an_event_when_a_document_is_uploaded()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var call = _client.Watch(new Empty(), cancellationToken: timeout.Token);

        // Response headers arrive only once the server has registered the subscription.
        await call.ResponseHeadersAsync;

        var received = ReadFirstAsync(call.ResponseStream, timeout.Token);
        var uploaded = await UploadAsync("watched.txt", "text/plain", "hi"u8.ToArray());
        var change = await received;

        Assert.Equal(ChangeEvent.Types.Kind.Added, change.Kind);
        Assert.Equal(uploaded.Id, change.DocumentId);
        Assert.Equal("watched.txt", change.FileName);
    }

    private static async Task<ChangeEvent> ReadFirstAsync(
        IAsyncStreamReader<ChangeEvent> stream,
        CancellationToken cancellationToken)
    {
        await stream.MoveNext(cancellationToken);
        return stream.Current;
    }

    /// <summary>Generated by the bundled ffmpeg, so no binary fixture lives in the repository.</summary>
    private async Task<string> SampleJpegAsync()
    {
        var path = Path.Combine(_root, "sample.jpg");
        Directory.CreateDirectory(_root);

        if (File.Exists(path))
        {
            return path;
        }

        string[] arguments =
        [
            "-hide_banner", "-loglevel", "error",
            "-f", "lavfi", "-i", "testsrc=size=64x64:duration=1:rate=1",
            "-frames:v", "1", "-y", path,
        ];

        using var process = BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffmpeg, arguments);

        await process.WaitForExitAsync();
        process.WaitForExit();

        // What ffmpeg said, in the message. A missing shared library is the likeliest reason this
        // ever fails, and "did not generate the sample image" on its own sends the reader into the
        // upload path instead of at the loader.
        Assert.True(
            File.Exists(path),
            $"ffmpeg did not generate the sample image: {BundledFfmpeg.Complaints(process)}");

        return path;
    }

    private async Task<DocumentMessage> UploadAsync(string fileName, string contentType, byte[] payload)
    {
        using var call = _client.Upload();

        await call.RequestStream.WriteAsync(new UploadRequest
        {
            Metadata = new UploadMetadata { FileName = fileName, ContentType = contentType },
        });

        foreach (var chunk in payload.Chunk(64 * 1024))
        {
            await call.RequestStream.WriteAsync(new UploadRequest { Chunk = ByteString.CopyFrom(chunk) });
        }

        await call.RequestStream.CompleteAsync();
        return await call.ResponseAsync;
    }

    private async Task<byte[]> DownloadAsync(string id)
    {
        using var call = _client.Download(new DocumentId { Id = id });

        return await ReadAllAsync(call.ResponseStream);
    }

    private static async Task<byte[]> ReadAllAsync(IAsyncStreamReader<Chunk> chunks)
    {
        using var buffer = new MemoryStream();

        await foreach (var chunk in chunks.ReadAllAsync())
        {
            chunk.Data.WriteTo(buffer);
        }

        return buffer.ToArray();
    }
}
