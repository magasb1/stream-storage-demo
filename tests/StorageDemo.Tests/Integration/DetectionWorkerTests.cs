using System.Diagnostics;
using System.Diagnostics.Metrics;
using Grpc.Net.Client;
using Grpc.Core;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Api.Controllers;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;
using StorageDemo.Worker;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// The detection worker end to end, in-process against the real application: the README's dog
/// picture pushed as a still video over SRT, detection switched on at one a second, and the worker
/// claiming the stream, decoding at that rate, detecting, tracking and posting VMTI frames the
/// owner then serves.
/// </summary>
[Collection(OnnxCollection.Name)]
public sealed class DetectionWorkerTests(ITestOutputHelper output) : IAsyncLifetime
{
    private const string Token = "worker-test-token";
    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";
    private const string NoModel = "models/rf-detr-nano.onnx is absent; run scripts/fetch-rfdetr.sh";

    private static readonly string Root = FindRoot();
    private static readonly string Model = Path.Combine(Root, "models", "rf-detr-nano.onnx");
    private static readonly string Dog = Path.Combine(Root, "models", "dog-2.jpeg");

    private readonly string _scratch = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-worker-tests",
        Guid.NewGuid().ToString("N"));

    private readonly List<Process> _senders = [];

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private int _ingestPort;

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_scratch);

        _ingestPort = SrtSenders.FreePort();

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_scratch, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_scratch, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            builder.UseSetting("Live:IngestPort", _ingestPort.ToString());
            builder.UseSetting("Live:ConsumptionPort", (_ingestPort + 1).ToString());
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
            SrtSenders.Kill(sender);
        }

        _client.Dispose();
        await _factory.DisposeAsync();

        for (var attempt = 0; attempt < 3 && Directory.Exists(_scratch); attempt++)
        {
            try
            {
                Directory.Delete(_scratch, recursive: true);
            }
            catch (IOException)
            {
                await Task.Delay(200);
            }
        }
    }

    [Fact]
    public async Task A_worker_claims_a_stream_whose_toggle_is_set_detects_the_dog_and_stands_down_when_it_clears()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(FfmpegLibrary.InputProtocols().Contains("srt"), "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.");
        Assert.SkipUnless(File.Exists(Model), NoModel);

        const string name = "live/dog";

        var still = Path.Combine(_scratch, "dog.ts");
        SrtSenders.Render(still, seconds: 120, image: Dog);

        _senders.Add(SrtSenders.StartSender(_ingestPort, $"#!::r={name},m=publish", file: still));

        await SrtSenders.WaitUntilAsync(
            () => Get(name).GetAwaiter().GetResult() is { Packets: > 0 },
            TimeSpan.FromSeconds(40),
            () => "the stream never appeared");

        var toggled = await _client.PutAsJsonAsync(
            $"/api/live/detect/{name}",
            new DetectRequest(true, 1, "rf-detr", ["dog"]));
        Assert.Equal(HttpStatusCode.OK, toggled.StatusCode);

        // The worker talks to the test host through its handler, the way a pod talks to a Service.
        using var delivery = new StallingDelivery(_factory.Server.CreateHandler());
        using var workerHttp = new HttpClient(delivery) { Timeout = Timeout.InfiniteTimeSpan };
        var completed = 0;
        using var metrics = new MeterListener();
        metrics.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Name == "live.detection.frame.duration") listener.EnableMeasurementEvents(instrument);
        };
        metrics.SetMeasurementEventCallback<double>((_, _, _, _) => Interlocked.Increment(ref completed));
        metrics.Start();
        var worker = new DetectionWorker(
            Options.Create(new WorkerOptions
            {
                ApiBaseUrl = _factory.Server.BaseAddress.ToString(),
                Token = Token,
                NodeName = "worker-test",
                ModelPath = Model,
                ExecutionProvider = "cpu",
            }),
            workerHttp,
            new TestLoggerFactory(output));

        var stopwatch = Stopwatch.StartNew();
        await worker.StartAsync(CancellationToken.None);

        try
        {
            VmtiSample? sample = null;

            // Generous: the model loads, the worker claims on its first beat, subscribes, waits for
            // a keyframe, and RF-DETR Nano takes about half a second a frame on a processor.
            await SrtSenders.WaitUntilAsync(
                () => (sample = Detections(name).GetAwaiter().GetResult()) is { }
                    && sample.Frame.Detections.Any(d => d.OntologyClass == "dog"),
                TimeSpan.FromSeconds(90),
                () => $"no dog was served; the last frame carried [{string.Join(", ", sample?.Frame.Detections.Select(d => $"{d.OntologyClass} {d.ConfidencePercent}") ?? [])}]");

            output.WriteLine($"first dog served {stopwatch.Elapsed.TotalSeconds:F1}s after the worker started");

            var stream = await Get(name);
            Assert.Equal("worker-test", stream!.DetectionWorker);
            Assert.Equal("rf-detr", stream.DetectionModel);
            Assert.Equal(["dog"], stream.DetectionLabels);
            Assert.Contains(name, worker.Held);

            // The picture went through MPEG-2 at its own size, so the geometry is the README's
            // within the codec's blur; the box is on the dog, not merely somewhere in the frame.
            var dog = sample!.Frame.Detections.First(d => d.OntologyClass == "dog");
            Assert.All(sample.Frame.Detections, detection => Assert.Equal("dog", detection.OntologyClass));
            Assert.Equal(720, sample.Frame.FrameWidth);
            Assert.Equal(1280, sample.Frame.FrameHeight);
            Assert.InRange(dog.Left, 158 - 60, 158 + 60);
            Assert.InRange(dog.Top, 493 - 60, 493 + 60);
            Assert.InRange(dog.Right, 459 - 60, 459 + 60);
            Assert.InRange(dog.Bottom, 850 - 60, 850 + 60);

            // Tracked, not merely detected: the box carries a VTracker LS, on the wire too.
            Assert.NotNull(dog.Track);
            Assert.Equal(VmtiTrackStatus.Active, dog.Track.Status);
            var packet = Vmti.Decode(sample.Raw);
            Assert.Contains(packet.Targets, pack => pack.Id == dog.Id && pack.Items.ContainsKey(104));

            // The frame's timestamp is derived from its presentation time: the next frame served is
            // later by about the detection interval, not by whatever the wall clock said.
            var first = sample.Frame.Timestamp;
            await SrtSenders.WaitUntilAsync(
                () => (sample = Detections(name).GetAwaiter().GetResult()) is { } && sample.Frame.Timestamp > first,
                TimeSpan.FromSeconds(15),
                () => "no second frame was served");
            Assert.InRange((sample!.Frame.Timestamp - first).TotalSeconds, 0.5, 5);

            using var grpc = GrpcChannel.ForAddress(_factory.Server.BaseAddress,
                new GrpcChannelOptions { HttpHandler = _factory.Server.CreateHandler() });
            var client = new StorageDemo.Grpc.Documents.DocumentsClient(grpc);
            using var watchDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var watch = client.WatchLiveDetections(new StorageDemo.Grpc.LiveStreamName { Name = name },
                new Metadata { { "x-storage-token", Token } }, cancellationToken: watchDeadline.Token);
            Assert.True(await watch.ResponseStream.MoveNext(watchDeadline.Token));
            var streamedAt = watch.ResponseStream.Current.Timestamp;
            Assert.True(await watch.ResponseStream.MoveNext(watchDeadline.Token));
            Assert.True(watch.ResponseStream.Current.Timestamp.ToDateTime() > streamedAt.ToDateTime());

            delivery.Stall = true;
            await delivery.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            var beforeStall = Volatile.Read(ref completed);
            await SrtSenders.WaitUntilAsync(
                () => Volatile.Read(ref completed) >= beforeStall + 3,
                TimeSpan.FromSeconds(15), () => "inference stopped while result delivery was stalled");
            Assert.True(Volatile.Read(ref delivery.TimedOut) > 0, "stalled POSTs must reach their deadline");
            delivery.Stall = false;

            var cleared = await _client.PutAsJsonAsync($"/api/live/detect/{name}", new DetectRequest(false));
            Assert.Equal(HttpStatusCode.OK, cleared.StatusCode);

            await SrtSenders.WaitUntilAsync(
                () => worker.Held.Count == 0 && Get(name).GetAwaiter().GetResult() is { DetectionWorker: null },
                TimeSpan.FromSeconds(15),
                () => "the worker did not stand down");
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
            worker.Dispose();
        }
    }

    private sealed class StallingDelivery(HttpMessageHandler inner) : DelegatingHandler(inner)
    {
        public volatile bool Stall;
        public int TimedOut;
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (Stall && request.Method == HttpMethod.Post && request.RequestUri!.AbsolutePath.Contains("/peer/detections/"))
            {
                Entered.TrySetResult();
                try { await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken); }
                catch (OperationCanceledException) { Interlocked.Increment(ref TimedOut); throw; }
            }
            return await base.SendAsync(request, cancellationToken);
        }
    }

    private async Task<LiveStream?> Get(string name)
    {
        var response = await _client.GetAsync($"/api/live/stream/{name}");

        return response.StatusCode == HttpStatusCode.OK
            ? await response.Content.ReadFromJsonAsync<LiveStream>()
            : null;
    }

    private async Task<VmtiSample?> Detections(string name)
    {
        var response = await _client.GetAsync($"/api/live/detections/{name}");

        return response.StatusCode == HttpStatusCode.OK
            ? await response.Content.ReadFromJsonAsync<VmtiSample>()
            : null;
    }

    /// <summary>Walks up from the test binaries to the checkout, which is where models/ lives.</summary>
    private static string FindRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null && !Directory.Exists(Path.Combine(directory.FullName, "models")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? AppContext.BaseDirectory;
    }

    /// <summary>The worker's log into the test output, which is where a failure is read.</summary>
    private sealed class TestLoggerFactory(ITestOutputHelper output) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => new TestLogger(output, categoryName);

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }
    }

    private sealed class TestLogger(ITestOutputHelper output, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Debug;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            try
            {
                output.WriteLine($"{logLevel} {category[(category.LastIndexOf('.') + 1)..]}: {formatter(state, exception)}{(exception is null ? string.Empty : $" {exception.GetType().Name}: {exception.Message}")}");
            }
            catch (InvalidOperationException)
            {
                // Logged after the test finished, which xunit refuses.
            }
        }
    }
}
