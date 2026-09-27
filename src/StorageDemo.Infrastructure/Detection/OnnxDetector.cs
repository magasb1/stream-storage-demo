using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Infrastructure.Detection;

/// <summary>
/// Runs one ONNX detector, described by a <see cref="DetectorDescriptor"/>, on the best execution
/// provider in the native runtime selected at publish time.
/// </summary>
public sealed unsafe partial class OnnxDetector : IDetector, IDisposable
{
    private const int Channels = 3;

    /// <summary>
    /// How long a detection took, in milliseconds, published on the pod's own meter (<see
    /// cref="LiveMetrics"/>) so <c>dotnet-counters monitor --counters StorageDemo.Live</c> reads it
    /// with no package and no exporter.
    /// </summary>
    private static readonly Histogram<double> Duration = new Meter(LiveMetrics.MeterName).CreateHistogram<double>(
        "live.detection.duration",
        unit: "ms",
        description: "Wall time of one detection call, by execution provider and batch size.");

    /// <summary>
    /// One thread pool for the process rather than one per session, which is the default and fights
    /// itself once a service holds several sessions (research section 3, "Threads").
    /// </summary>
    private static readonly Lazy<bool> GlobalThreadPools = new(() =>
    {
        if (OrtEnv.IsCreated)
        {
            return false;
        }

        var options = new EnvironmentCreationOptions
        {
            logId = "storagedemo",
            threadOptions = new OrtThreadingOptions { GlobalIntraOpNumThreads = 0, GlobalInterOpNumThreads = 1, GlobalSpinControl = false },
        };

        _ = OrtEnv.CreateInstanceWithOptions(ref options);

        return true;
    });

    private readonly DetectorDescriptor _descriptor;
    private readonly InferenceSession _session;
    private readonly RunOptions _runOptions = new();
    private readonly string[] _inputNames;
    private readonly string[] _outputNames;
    private readonly OrtValue[] _inputValues = new OrtValue[1];

    /// <summary>
    /// Kept as a logit: sigmoid is monotonic, so comparing the raw logit against the threshold's
    /// logit keeps every score the threshold would and skips 27,000 exps per frame for the rest.
    /// </summary>
    private readonly float _cutoff;

    private readonly byte _padValue;

    // ponytail: one input buffer and one scaler behind one lock, so calls serialise.
    private readonly Lock _gate = new();
    private readonly int _frameBytes;
    private readonly int _stride;
    private byte* _input;
    private int _capacity;
    private SwsContext* _scaler;

    // swscale reads plane arrays, four entries at least; filled per frame, never reallocated.
    private readonly byte*[] _source = new byte*[8];
    private readonly int[] _sourceStride = new int[8];
    private readonly byte*[] _target = new byte*[4];
    private readonly int[] _targetStride = new int[4];

    /// <summary>
    /// The reference resize is two-tap bilinear with antialiasing off (DetectorGeometry.Stretch,
    /// citing rfdetr's _resize.py).
    /// </summary>
    private const SwsFlags Flags = SwsFlags.SWS_FAST_BILINEAR | SwsFlags.SWS_FULL_CHR_H_INT | SwsFlags.SWS_ACCURATE_RND;

    /// <param name="threshold">Scores below this, after the sigmoid where one applies, are dropped.</param>
    public OnnxDetector(
        string modelPath,
        DetectorDescriptor descriptor,
        float threshold,
        ILogger logger,
        string executionProvider = "auto",
        string? openVinoCachePath = null)
    {
        if (threshold is <= 0 or >= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(threshold), threshold, "A probability strictly between 0 and 1.");
        }

        if (descriptor.NeedsNms)
        {
            // Still unbuilt after D4, and deliberately: RF-DETR's decode is set-based and YOLO26's
            // export uses the one-to-one head, so neither descriptor here asks for it.
            throw new NotSupportedException("Non-maximum suppression is not built; no descriptor here needs it (detection-plan.md D4).");
        }

        FfmpegLibrary.EnsureLoaded();

        _descriptor = descriptor;
        _cutoff = descriptor.ScoresAreLogits ? MathF.Log(threshold / (1 - threshold)) : threshold;
        _frameBytes = descriptor.InputSize * descriptor.InputSize * Channels;
        _inputNames = [descriptor.InputName];
        _outputNames = descriptor.ScoresOutput is null
            ? [descriptor.BoxesOutput]
            : [descriptor.BoxesOutput, descriptor.ScoresOutput];

        // The letterbox fill, 114, from the geometry that has one; Stretch covers the whole canvas
        // and never shows it.
        _padValue = descriptor.Geometry is DetectorGeometry.Letterbox letterbox ? (byte)letterbox.PadValue : (byte)0;

        // Asked before the options are made, and that order is load-bearing: constructing
        // SessionOptions initialises the runtime's default environment, so asking afterwards always
        // answers "somebody else made it" and every session silently runs its own thread pool.
        var ours = GlobalThreadPools.Value;

        if (!ours)
        {
            logger.LogWarning(
                "Another component created this process's ONNX Runtime environment, so this "
                + "detector runs its own thread pool rather than the shared one. It works and "
                + "costs threads; measured at 22 percent throughput on a processor.");
        }
        var requested = executionProvider.Trim().ToLowerInvariant();
        var available = OrtEnv.Instance().GetAvailableProviders();
        var candidates = ProviderCandidates(requested, available, descriptor.SupportsOpenVinoNpu);
        var mayFallback = requested is "auto" or "openvino";
        InferenceSession? selected = null;

        foreach (var candidate in candidates)
        {
            using var options = new SessionOptions();

            if (ours)
            {
                options.DisablePerSessionThreads();
            }

            InferenceSession? attempt = null;

            try
            {
                ConfigureProvider(options, candidate, openVinoCachePath);
                attempt = new InferenceSession(modelPath, options);
                _session = attempt;
                Provider = candidate.Label;

                VerifyContract();
                Warm(logger);
                selected = attempt;
                break;
            }
            catch (Exception ex) when (mayFallback && candidate.Kind != ProviderKind.Cpu && IsProviderFailure(ex))
            {
                attempt?.Dispose();
                logger.LogInformation(
                    "{Provider} could not initialize this model, trying the next execution provider: {Reason}",
                    candidate.Label,
                    ex.Message);
            }
            catch
            {
                attempt?.Dispose();
                throw;
            }
        }

        _session = selected ?? throw new InvalidOperationException(
            $"No usable ONNX Runtime execution provider was found for '{executionProvider}'. Available: {string.Join(", ", available)}.");

        Classes = ReadEmbeddedMetadata(logger) ?? descriptor.Classes;

        logger.LogInformation(
            "Detector {Model} on the {Provider} execution provider (available: {Providers}), input {Input} {Shape}",
            Path.GetFileName(modelPath),
            Provider,
            string.Join(", ", available),
            descriptor.InputName,
            string.Join("x", _session.InputMetadata[descriptor.InputName].Dimensions));

        _stride = descriptor.InputSize * Channels;
        _targetStride[0] = _stride;

    }

    /// <summary>The execution provider and physical device on which the warmed session runs.</summary>
    public string Provider { get; private set; } = null!;

    /// <summary>
    /// The table actually in use: the file's own <c>names</c> when it carries them, the
    /// descriptor's otherwise.
    /// </summary>
    public IReadOnlyDictionary<int, string> Classes { get; }

    public VmtiDetection[] Detect(IntPtr frame) => Detect([frame])[0];

    public VmtiDetection[][] Detect(ReadOnlySpan<IntPtr> frames)
    {
        if (frames.IsEmpty)
        {
            return [];
        }

        lock (_gate)
        {
            // Timed inside the lock: what is wanted is what a detection costs, not how long this
            // caller queued behind another one.
            var started = Stopwatch.GetTimestamp();

            EnsureCapacity(frames.Length);

            for (var i = 0; i < frames.Length; i++)
            {
                Place((AVFrame*)frames[i], _input + i * _frameBytes);
            }

            var size = _descriptor.InputSize;

            // Wraps the pinned native buffer; nothing is copied.
            using var input = OrtValue.CreateTensorValueWithData(
                OrtMemoryInfo.DefaultInstance,
                TensorElementType.UInt8,
                [frames.Length, size, size, Channels],
                (IntPtr)_input,
                frames.Length * _frameBytes);

            _inputValues[0] = input;

            using var outputs = _session.Run(_runOptions, _inputNames, _inputValues, _outputNames);

            // (batch, queries, columns): four for RF-DETR's cxcywh, six for a YOLO row that carries
            // its own score and class.
            var boxes = outputs[0].GetTensorDataAsSpan<float>();
            var boxShape = outputs[0].GetTensorTypeAndShape().Shape;
            var queries = (int)boxShape[1];
            var columns = (int)boxShape[2];

            var scores = ReadOnlySpan<float>.Empty;
            var classes = 0;

            if (_descriptor.ScoresOutput is not null)
            {
                scores = outputs[1].GetTensorDataAsSpan<float>();
                classes = (int)outputs[1].GetTensorTypeAndShape().Shape[2];
            }

            var results = new VmtiDetection[frames.Length][];

            for (var i = 0; i < frames.Length; i++)
            {
                var frame = (AVFrame*)frames[i];
                var rows = boxes.Slice(i * queries * columns, queries * columns);

                results[i] = _descriptor.BoxFormat == BoxFormat.PixelCorners
                    ? DecodeRows(rows, columns, frame->width, frame->height)
                    : Decode(rows, scores.Slice(i * queries * classes, queries * classes), classes, frame->width, frame->height);
            }

            Duration.Record(
                Stopwatch.GetElapsedTime(started).TotalMilliseconds,
                new KeyValuePair<string, object?>("provider", Provider),
                new KeyValuePair<string, object?>("batch", frames.Length));

            return results;
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _session.Dispose();
            _runOptions.Dispose();

            ffmpeg.sws_freeContext(_scaler);
            _scaler = null;

            NativeMemory.AlignedFree(_input);
            _input = null;
            _capacity = 0;
        }
    }

    /// <summary>
    /// Provider packages contain competing native libraries named onnxruntime, so a publish carries
    /// one flavor.
    /// </summary>
    private static IReadOnlyList<ProviderCandidate> ProviderCandidates(
        string requested,
        IReadOnlyCollection<string> available,
        bool supportsOpenVinoNpu)
    {
        var providers = available.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var result = new List<ProviderCandidate>();

        void Add(ProviderKind kind, string providerName, string label, string? device = null)
        {
            if (kind == ProviderKind.Cpu || providers.Contains(providerName))
            {
                result.Add(new ProviderCandidate(kind, label, device));
            }
        }

        switch (requested)
        {
            case "auto":
                Add(ProviderKind.Cuda, "CUDAExecutionProvider", "CUDA");
                if (supportsOpenVinoNpu)
                {
                    Add(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/NPU", "NPU");
                }
                Add(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/GPU", "GPU");
                Add(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/CPU", "CPU");
                Add(ProviderKind.DirectML, "DmlExecutionProvider", "DirectML");
                Add(ProviderKind.Cpu, "CPUExecutionProvider", "CPU");
                break;
            case "openvino":
                Require("OpenVINOExecutionProvider");
                if (supportsOpenVinoNpu)
                {
                    result.Add(new(ProviderKind.OpenVino, "OpenVINO/NPU", "NPU"));
                }
                result.Add(new(ProviderKind.OpenVino, "OpenVINO/GPU", "GPU"));
                result.Add(new(ProviderKind.OpenVino, "OpenVINO/CPU", "CPU"));
                break;
            case "openvino-npu": AddRequired(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/NPU", "NPU"); break;
            case "openvino-gpu": AddRequired(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/GPU", "GPU"); break;
            case "openvino-cpu": AddRequired(ProviderKind.OpenVino, "OpenVINOExecutionProvider", "OpenVINO/CPU", "CPU"); break;
            case "cuda": AddRequired(ProviderKind.Cuda, "CUDAExecutionProvider", "CUDA"); break;
            case "tensorrt": AddRequired(ProviderKind.TensorRT, "TensorrtExecutionProvider", "TensorRT"); break;
            case "directml": AddRequired(ProviderKind.DirectML, "DmlExecutionProvider", "DirectML"); break;
            case "cpu": result.Add(new(ProviderKind.Cpu, "CPU")); break;
            default:
                throw new ArgumentException(
                    $"Unknown execution provider '{requested}'. Use auto, cpu, cuda, tensorrt, directml, openvino, openvino-npu, openvino-gpu, or openvino-cpu.",
                    nameof(requested));
        }

        return result;

        void Require(string providerName)
        {
            if (!providers.Contains(providerName))
            {
                throw new InvalidOperationException(
                    $"The requested provider needs {providerName}, but this publish contains: {string.Join(", ", available)}.");
            }
        }

        void AddRequired(ProviderKind kind, string providerName, string label, string? device = null)
        {
            Require(providerName);
            result.Add(new(kind, label, device));
        }
    }

    private static void ConfigureProvider(
        SessionOptions options,
        ProviderCandidate provider,
        string? openVinoCachePath)
    {
        switch (provider.Kind)
        {
            case ProviderKind.Cpu:
                break;
            case ProviderKind.Cuda:
                options.AppendExecutionProvider_CUDA();
                break;
            case ProviderKind.TensorRT:
                options.AppendExecutionProvider_Tensorrt();
                options.AppendExecutionProvider_CUDA();
                break;
            case ProviderKind.DirectML:
                options.EnableMemoryPattern = false;
                options.ExecutionMode = ExecutionMode.ORT_SEQUENTIAL;
                options.AppendExecutionProvider_DML();
                break;
            case ProviderKind.OpenVino:
                // OpenVINO performs its own device-specific graph optimization.
                options.GraphOptimizationLevel = GraphOptimizationLevel.ORT_DISABLE_ALL;
                var cache = string.IsNullOrWhiteSpace(openVinoCachePath)
                    ? Path.Combine(Path.GetTempPath(), "storagedemo-openvino-cache")
                    : Path.GetFullPath(openVinoCachePath);
                Directory.CreateDirectory(cache);
                options.AppendExecutionProvider(
                    "OpenVINO",
                    new Dictionary<string, string>
                    {
                        ["device_type"] = provider.Device!,
                        ["cache_dir"] = cache,
                    });
                break;
            default:
                throw new UnreachableException();
        }
    }

    private static bool IsProviderFailure(Exception ex)
        => ex is OnnxRuntimeException
            or DllNotFoundException
            or EntryPointNotFoundException
            or BadImageFormatException;

    private enum ProviderKind { Cpu, Cuda, TensorRT, DirectML, OpenVino }

    private sealed record ProviderCandidate(ProviderKind Kind, string Label, string? Device = null);

    /// <summary>
    /// A YOLO export self-describes: <c>metadata_props</c> carries <c>names</c> and <c>imgsz</c>,
    /// so the runner reads them instead of being configured (detection-plan.md D4).
    /// </summary>
    private IReadOnlyDictionary<int, string>? ReadEmbeddedMetadata(ILogger logger)
    {
        var metadata = _session.ModelMetadata.CustomMetadataMap;

        // imgsz is "[640, 640]".
        if (metadata.TryGetValue("imgsz", out var imgsz))
        {
            var sizes = EmbeddedInteger().Matches(imgsz).Select(m => int.Parse(m.Value)).ToArray();

            if (sizes.Length != 2 || sizes[0] != _descriptor.InputSize || sizes[1] != _descriptor.InputSize)
            {
                throw new InvalidOperationException(
                    $"The model's own imgsz is {imgsz}, not [{_descriptor.InputSize}, {_descriptor.InputSize}]; "
                    + "the descriptor's geometry and the file disagree.");
            }
        }

        // The three output contracts D4 warns about are decided by flags frozen at export, and this
        // is the one that is visible: end2end says the one-to-one head is active, which is why
        // NeedsNms is false and why the 300 rows are objects rather than 8400 anchors.
        if (metadata.TryGetValue("end2end", out var end2end)
            && !_descriptor.NeedsNms
            && !end2end.Equals("True", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The model says end2end={end2end}, so its head emits anchors needing suppression, "
                + "and this descriptor says none is needed. Re-export with the one-to-one head.");
        }

        if (!metadata.TryGetValue("names", out var names))
        {
            logger.LogInformation("The model carries no class names of its own; using the descriptor's table of {Count}.", _descriptor.Classes.Count);
            return null;
        }

        var embedded = EmbeddedName().Matches(names).ToDictionary(m => int.Parse(m.Groups[1].Value), m => m.Groups[2].Value);

        if (embedded.Count == 0)
        {
            throw new InvalidOperationException($"The model's 'names' metadata parsed to nothing: {names}");
        }

        logger.LogInformation("Class names read from the model's own metadata: {Count}.", embedded.Count);

        return embedded;
    }

    [GeneratedRegex(@"\d+")]
    private static partial Regex EmbeddedInteger();

    /// <summary><c>0: 'person'</c>.</summary>
    [GeneratedRegex(@"(\d+)\s*:\s*'((?:[^'\\]|\\.)*)'")]
    private static partial Regex EmbeddedName();

    /// <summary><c>'dets' [-1,300,4], 'labels' [-1,300,91]</c>.</summary>
    private static string Shapes(IReadOnlyDictionary<string, NodeMetadata> metadata)
        => string.Join(", ", metadata.Select(node => $"'{node.Key}' [{string.Join(",", node.Value.Dimensions)}]"));

    /// <summary>
    /// The file matches the descriptor: names exist, input is uint8 NHWC at the canvas size.
    /// </summary>
    private void VerifyContract()
    {
        if (!_session.InputMetadata.TryGetValue(_descriptor.InputName, out var input))
        {
            throw new InvalidOperationException($"The model has no input named '{_descriptor.InputName}'; it has {Shapes(_session.InputMetadata)}.");
        }

        foreach (var name in _outputNames)
        {
            if (!_session.OutputMetadata.ContainsKey(name))
            {
                throw new InvalidOperationException($"The model has no output named '{name}'; it has {Shapes(_session.OutputMetadata)}.");
            }
        }

        var size = _descriptor.InputSize;
        var dimensions = input.Dimensions;

        // Batch is dimension 0 and is allowed to be anything, including fixed; a fixed batch fails
        // at Run with the runtime's own shape error when a larger batch arrives.
        if (input.ElementDataType != TensorElementType.UInt8
            || dimensions.Length != 4
            || dimensions[1] != size
            || dimensions[2] != size
            || dimensions[3] != Channels)
        {
            throw new InvalidOperationException(
                $"Input '{_descriptor.InputName}' is {input.ElementDataType} [{string.Join(", ", dimensions)}], "
                + $"not uint8 [batch, {size}, {size}, {Channels}]; the descriptor and the file disagree.");
        }
    }

    /// <summary>
    /// One inference on a blank canvas, at construction, so whatever the provider defers to its
    /// first <c>Run</c> is paid at startup rather than by the first stream's first frame.
    /// </summary>
    private void Warm(ILogger logger)
    {
        var started = Stopwatch.GetTimestamp();

        EnsureCapacity(1);
        NativeMemory.Fill(_input, (nuint)_frameBytes, _padValue);

        var size = _descriptor.InputSize;

        using (var input = OrtValue.CreateTensorValueWithData(
            OrtMemoryInfo.DefaultInstance,
            TensorElementType.UInt8,
            [1, size, size, Channels],
            (IntPtr)_input,
            _frameBytes))
        {
            using var _ = _session.Run(_runOptions, _inputNames, [input], _outputNames);
        }

        logger.LogInformation(
            "Session warmed on {Provider} in {Elapsed:F0} ms; the first frame pays a steady-state detection now.",
            Provider,
            Stopwatch.GetElapsedTime(started).TotalMilliseconds);
    }

    private void EnsureCapacity(int frames)
    {
        if (frames <= _capacity)
        {
            return;
        }

        NativeMemory.AlignedFree(_input);
        _input = (byte*)NativeMemory.AlignedAlloc((nuint)(frames * _frameBytes), 64);
        _capacity = frames;
    }

    /// <summary>The frame onto its place on the canvas, converting pixel format on the way.</summary>
    private void Place(AVFrame* frame, byte* canvas)
    {
        var (left, top, width, height) = _descriptor.Geometry.Place(frame->width, frame->height);

        // The cached context is reused while the source keeps its size and format, which a stream
        // does; sws_scale_frame was tried and left the destination untouched when source and
        // destination matched, so the plain call with explicit planes is used instead.
        _scaler = ffmpeg.sws_getCachedContext(
            _scaler,
            frame->width,
            frame->height,
            (AVPixelFormat)frame->format,
            width,
            height,
            AVPixelFormat.AV_PIX_FMT_RGB24,
            (int)Flags,
            null,
            null,
            null);

        if (_scaler is null)
        {
            throw new InvalidOperationException($"swscale cannot convert a {frame->width}x{frame->height} frame of format {frame->format}.");
        }

        for (var plane = 0u; plane < 8; plane++)
        {
            _source[plane] = frame->data[plane];
            _sourceStride[plane] = frame->linesize[plane];
        }

        // The padding around the placement, when the geometry leaves any: 114 everywhere, then
        // swscale writes the picture over the middle of it.
        if (width != _descriptor.InputSize || height != _descriptor.InputSize)
        {
            NativeMemory.Fill(canvas, (nuint)_frameBytes, _padValue);
        }

        _target[0] = canvas + top * _stride + left * Channels;

        ffmpeg.sws_scale(_scaler, _source, _sourceStride, 0, frame->height, _target, _targetStride);
    }

    /// <summary>
    /// The reference decode is sigmoid, flatten queries x classes, keep the best 300
    /// (models/README.md, "Decode"); with a threshold that is every (query, class) above it, and
    /// the same query may appear under two classes, which is intended (multi-label).
    /// </summary>
    private VmtiDetection[] Decode(ReadOnlySpan<float> boxes, ReadOnlySpan<float> scores, int classes, int frameWidth, int frameHeight)
    {
        var kept = new List<VmtiDetection>();

        for (var index = 0; index < scores.Length; index++)
        {
            var raw = scores[index];
            if (raw <= _cutoff || !Classes.TryGetValue(index % classes, out var name))
            {
                continue;
            }

            var query = index / classes;
            var score = _descriptor.ScoresAreLogits ? 1 / (1 + MathF.Exp(-raw)) : raw;

            var box = boxes.Slice(query * 4, 4);

            kept.Add(_descriptor.Geometry.ToFrameNormalised(
                kept.Count + 1,
                (box[0], box[1], box[2], box[3]),
                frameWidth,
                frameHeight) with
            {
                ConfidencePercent = (int)Math.Round(score * 100),
                OntologyClass = name,
            });
        }

        return [.. kept];
    }

    /// <summary>
    /// <see cref="BoxFormat.PixelCorners"/>: one row per query, <c>x1, y1, x2, y2, score,
    /// classId</c>, corners already in canvas pixels so the geometry's own inverse takes them
    /// straight.
    /// </summary>
    private VmtiDetection[] DecodeRows(ReadOnlySpan<float> rows, int columns, int frameWidth, int frameHeight)
    {
        var kept = new List<VmtiDetection>();

        for (var row = 0; row + columns <= rows.Length; row += columns)
        {
            var score = rows[row + 4];

            if (score <= _cutoff)
            {
                break;
            }

            if (!Classes.TryGetValue((int)rows[row + 5], out var name))
            {
                continue;
            }

            kept.Add(_descriptor.Geometry.ToFrame(
                kept.Count + 1,
                (rows[row], rows[row + 1], rows[row + 2], rows[row + 3]),
                frameWidth,
                frameHeight) with
            {
                ConfidencePercent = (int)Math.Round(score * 100),
                OntologyClass = name,
            });
        }

        return [.. kept];
    }
}
