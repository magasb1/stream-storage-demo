using System.Net;
using System.Net.Http.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Detection;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Worker;

/// <summary>
/// Claims streams whose detection toggle is set, and runs one <see cref="StreamJob"/> per claimed
/// stream against detector sessions shared by every stream selecting the same model.
/// </summary>
public sealed class DetectionWorker : BackgroundService
{
    public static readonly TimeSpan Beat = LiveStreamCoordinator.Beat;

    private readonly WorkerOptions _options;
    private readonly HttpClient _http;
    private readonly ILoggerFactory _loggers;
    private readonly ILogger _logger;
    private readonly Dictionary<string, StreamJob> _jobs = new(StringComparer.Ordinal);
    private readonly Channel<StreamJob> _due = Channel.CreateUnbounded<StreamJob>(
        new UnboundedChannelOptions { SingleReader = true });

    /// <summary>
    /// The worker's private hubs keep the smallest buffer the option allows: nobody rolls back on a
    /// worker, and what a hub holds here is only the packets between one decode and the next.
    /// </summary>
    private readonly LiveOptions _live = new()
    {
        BufferWindowSeconds = 5,
        BufferByteCeiling = 8L * 1024 * 1024,
    };

    private readonly StreamDemuxer _demuxer;

    public DetectionWorker(IOptions<WorkerOptions> options, HttpClient http, ILoggerFactory loggers)
    {
        _options = options.Value;
        _http = http;
        _loggers = loggers;
        _logger = loggers.CreateLogger<DetectionWorker>();
        _demuxer = new StreamDemuxer(Options.Create(_live), loggers.CreateLogger<StreamDemuxer>());

        Name = _options.NodeName is { Length: > 0 } name
            ? name
            : Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName;

        _http.BaseAddress ??= new Uri(_options.ApiBaseUrl.TrimEnd('/') + "/");

        if (_options.Token is { Length: > 0 } token && !_http.DefaultRequestHeaders.Contains("X-Storage-Token"))
        {
            _http.DefaultRequestHeaders.Add("X-Storage-Token", token);
        }
    }

    /// <summary>What this worker writes into a stream's registry entry when it claims it.</summary>
    public string Name { get; }

    /// <summary>The streams this worker currently holds.</summary>
    public IReadOnlyCollection<string> Held
    {
        get { lock (_jobs) { return [.. _jobs.Keys]; } }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        FfmpegLibrary.EnsureLoaded();

        var defaultModel = DetectionModels.Normalize(_options.Model)
            ?? throw new InvalidOperationException("Worker__Model must name a concrete model.");
        var detectors = new Dictionary<string, OnnxDetector>(StringComparer.Ordinal);

        // Load the configured default up front so a bad deployment fails at startup.
        detectors[defaultModel] = CreateDetector(defaultModel, defaultModel);

        var detecting = Task.Run(() => DetectAsync(detectors, defaultModel, stoppingToken), CancellationToken.None);

        try
        {
            using var beats = new PeriodicTimer(Beat);

            do
            {
                try
                {
                    await PollAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning(
                        "The API did not return the stream list within 2 seconds at {Url}; retrying.",
                        _options.ApiBaseUrl);
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    // An API that is briefly unreachable must not take the worker down.
                _logger.LogWarning(
                    "Could not reach the API at {Url} to list streams: {Reason}. Retrying.",
                    _options.ApiBaseUrl,
                    ex.InnerException?.Message ?? ex.Message);
                }
            }
            while (await beats.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await StandDownAsync();
            try
            {
                await detecting;
            }
            finally
            {
                foreach (var detector in detectors.Values) detector.Dispose();
            }
        }
    }

    private sealed record Listing(IReadOnlyList<LiveStream> Streams);

    private async Task PollAsync(CancellationToken cancellationToken)
    {
        using var listingDeadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        listingDeadline.CancelAfter(TimeSpan.FromSeconds(2));
        var listing = await _http.GetFromJsonAsync<Listing>("api/live", listingDeadline.Token) ?? new Listing([]);
        var listed = listing.Streams.ToDictionary(stream => stream.Name, StringComparer.Ordinal);

        foreach (var (name, job) in Jobs())
        {
            var mine = listed.TryGetValue(name, out var stream)
                && stream.DetectionEnabled
                && (stream.DetectionWorker is null || stream.DetectionWorker == Name);

            if (!mine)
            {
                _logger.LogInformation("Standing down from '{Name}'", name);
                await StopAsync(name, job);
                continue;
            }

            job.Owner = Owner(stream!);
            job.Rate = Rate(stream!);
            job.Configure(Model(stream!), stream!.DetectionLabels);

            if (await ClaimAsync(stream!, cancellationToken) is false)
            {
                _logger.LogInformation("'{Name}' is held by {Worker} now; standing down", name, stream!.DetectionWorker);
                await StopAsync(name, job);
            }
        }

        foreach (var stream in listing.Streams)
        {
            if (!stream.DetectionEnabled || stream.DetectionWorker is not null || Holds(stream.Name))
            {
                continue;
            }

            if (await ClaimAsync(stream, cancellationToken) is not true)
            {
                continue;
            }

            _logger.LogInformation("Claimed '{Name}' from {Owner} at {Rate}/s", stream.Name, stream.Owner, Rate(stream));

            var job = new StreamJob(
                stream.Name,
                Owner(stream),
                Rate(stream),
                Model(stream),
                stream.DetectionLabels,
                _http,
                _demuxer,
                _live,
                _due.Writer,
                _options.TrackThreshold,
                _loggers.CreateLogger<StreamJob>());

            lock (_jobs)
            {
                _jobs[stream.Name] = job;
            }
        }
    }

    /// <summary>
    /// True when claimed or renewed, false when another worker holds it, null when the owner could
    /// not say.
    /// </summary>
    private async Task<bool?> ClaimAsync(LiveStream stream, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            using var response = await _http.PutAsJsonAsync(
                $"{Owner(stream)}/api/live/peer/detector/{stream.Name}",
                new { worker = Name },
                deadline.Token);

            return response.StatusCode switch
            {
                HttpStatusCode.OK => true,
                HttpStatusCode.Conflict => false,
                _ => null,
            };
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("The owner of '{Name}' did not renew within 2 seconds; retrying next beat", stream.Name);
            return null;
        }
        catch (HttpRequestException ex) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Could not reach the owner of '{Name}' at {Owner}", stream.Name, Owner(stream));
            return null;
        }
    }

    /// <summary>
    /// Gives every claim back, because the toggle stays set and the streams want another worker.
    /// </summary>
    private async Task StandDownAsync()
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        foreach (var (name, job) in Jobs())
        {
            await StopAsync(name, job);

            try
            {
                using var response = await _http.DeleteAsync(
                    $"{job.Owner}/api/live/peer/detector/{name}?worker={Uri.EscapeDataString(Name)}",
                    deadline.Token);
            }
            catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException)
            {
                // The lease lapses on its own; releasing it is a courtesy.
                _logger.LogWarning(ex, "Could not release '{Name}'; its lease will lapse", name);
            }
        }
    }

    /// <summary>Takes the newest waiting frames up to the batch size and runs them as one call.</summary>
    private async Task DetectAsync(
        Dictionary<string, OnnxDetector> detectors,
        string defaultModel,
        CancellationToken cancellationToken)
    {
        var batch = new List<PendingFrame>(_options.MaxBatch);
        var retryAfter = new Dictionary<string, DateTimeOffset>(StringComparer.Ordinal);
        var loading = new Dictionary<string, Task<OnnxDetector>>(StringComparer.Ordinal);

        try
        {
            while (await _due.Reader.WaitToReadAsync(cancellationToken))
            {
                while (batch.Count < _options.MaxBatch && _due.Reader.TryRead(out var job))
                {
                    if (job.TakeFrame() is { } frame) batch.Add(frame);
                }
                if (batch.Count == 0) continue;

                try
                {
                    foreach (var group in batch.GroupBy(frame => frame.Model))
                    {
                        if (!detectors.ContainsKey(group.Key)
                            && retryAfter.GetValueOrDefault(group.Key) > DateTimeOffset.UtcNow)
                        {
                            continue;
                        }

                        try
                        {
                            var frames = group.ToArray();
                            if (!detectors.TryGetValue(group.Key, out var detector))
                            {
                                if (!loading.TryGetValue(group.Key, out var load))
                                {
                                    // Compiling an OpenVINO model can take seconds.
                                    load = Task.Run(() => CreateDetector(group.Key, defaultModel), CancellationToken.None);
                                    loading[group.Key] = load;
                                    continue;
                                }
                                if (!load.IsCompleted)
                                {
                                    continue;
                                }

                                detector = await load;
                                detectors[group.Key] = detector;
                                loading.Remove(group.Key);
                            }

                            var results = detector.Detect(frames.Select(frame => frame.Frame).ToArray());
                            for (var i = 0; i < frames.Length; i++)
                            {
                                frames[i].Job.Detected(frames[i], results[i]);
                            }
                        }
                        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                        {
                            // One unavailable optional model must not prevent streams using the
                            // already-loaded default from being processed in this same batch.
                            _logger.LogWarning(ex, "A {Model} batch of {Count} frames failed", group.Key, group.Count());
                            retryAfter[group.Key] = DateTimeOffset.UtcNow.AddSeconds(30);
                            loading.Remove(group.Key);
                        }
                    }
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "A batch of {Count} frames failed", batch.Count);
                }
                finally
                {
                    foreach (var frame in batch)
                    {
                        frame.Dispose();
                    }

                    batch.Clear();
                }
            }
        }
        catch (OperationCanceledException)
        {
        }

        while (_due.Reader.TryRead(out var left))
        {
            left.TakeFrame()?.Dispose();
        }

        // A background compilation already in flight cannot be cancelled by ONNX Runtime.
        foreach (var load in loading.Values)
        {
            try
            {
                var detector = await load;
                if (!detectors.Values.Contains(detector)) detector.Dispose();
            }
            catch
            {
            }
        }
    }

    private async Task StopAsync(string name, StreamJob job)
    {
        lock (_jobs)
        {
            _jobs.Remove(name);
        }

        await job.DisposeAsync();
    }

    private List<KeyValuePair<string, StreamJob>> Jobs()
    {
        lock (_jobs)
        {
            return [.. _jobs];
        }
    }

    private bool Holds(string name)
    {
        lock (_jobs)
        {
            return _jobs.ContainsKey(name);
        }
    }

    /// <summary>
    /// The owner's address from the listing, or the API's own when the owner recorded none.
    /// </summary>
    private string Owner(LiveStream stream)
        => (stream.OwnerAddress is { Length: > 0 } address ? address : _options.ApiBaseUrl).TrimEnd('/');

    private double Rate(LiveStream stream)
        => stream.DetectionRate > 0 ? stream.DetectionRate : _options.DefaultRate;

    private string Model(LiveStream stream)
        => DetectionModels.Normalize(stream.DetectionModel)
            ?? DetectionModels.Normalize(_options.Model)
            ?? DetectionModels.RfDetr;

    private OnnxDetector CreateDetector(string model, string defaultModel)
    {
        var (descriptor, conventionalPath, configuredPath) = model switch
        {
            DetectionModels.RfDetr => (
                DetectorDescriptor.RfDetrNano,
                "models/rf-detr-nano.onnx",
                _options.RfDetrModelPath),
            DetectionModels.Yolo26 => (
                DetectorDescriptor.Yolo26Nano,
                "models/yolo26-nano.onnx",
                _options.Yolo26ModelPath),
            _ => throw new InvalidOperationException($"Unsupported detection model '{model}'."),
        };
        var path = configuredPath.Length > 0
            ? configuredPath
            : model == defaultModel && _options.ModelPath.Length > 0 ? _options.ModelPath : conventionalPath;
        path = Resolve(path);

        _logger.LogInformation("Loading detection model {Model} from {Path}", model, path);
        return new OnnxDetector(
            path,
            descriptor,
            _options.Threshold,
            _logger,
            _options.ExecutionProvider,
            _options.OpenVinoCachePath);
    }

    /// <summary>
    /// A model path as given, or the same relative path found by walking up from the binary.
    /// </summary>
    private static string Resolve(string path)
    {
        if (Path.IsPathRooted(path) || File.Exists(path))
        {
            return path;
        }

        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            var candidate = Path.Combine(directory.FullName, path);

            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        return path;
    }

}
