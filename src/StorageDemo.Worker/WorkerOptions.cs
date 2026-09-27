using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Worker;

/// <summary>
/// Configuration for one detection worker, bound from the <c>Worker</c> section, so in a container
/// it reads <c>Worker__ApiBaseUrl</c> and <c>Worker__Token</c> the way the API reads
/// <c>Live__PeerBaseUrl</c>.
/// </summary>
public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    /// <summary>Where the worker lists streams, for example "http://storage-demo:80".</summary>
    [Required]
    public string ApiBaseUrl { get; init; } = string.Empty;

    /// <summary>The live token, sent as X-Storage-Token on every call.</summary>
    public string? Token { get; init; }

    /// <summary>How this worker names itself in a stream's registry entry.</summary>
    public string? NodeName { get; init; }

    /// <summary>Which model's contract to decode, because a file alone does not say.</summary>
    public string Model { get; init; } = "rf-detr";

    /// <summary>The file.</summary>
    public string ModelPath { get; init; } = string.Empty;

    /// <summary>Optional per-family paths used when streams select a model in the UI.</summary>
    public string RfDetrModelPath { get; init; } = string.Empty;

    public string Yolo26ModelPath { get; init; } = string.Empty;

    /// <summary>Hardware used for inference.</summary>
    [RegularExpression(
        "(?i)^(auto|cpu|cuda|tensorrt|directml|openvino|openvino-npu|openvino-gpu|openvino-cpu)$",
        ErrorMessage = "ExecutionProvider must be auto, cpu, cuda, tensorrt, directml, openvino, openvino-npu, openvino-gpu, or openvino-cpu.")]
    public string ExecutionProvider { get; init; } = "auto";

    /// <summary>Persistent compiled-model cache for OpenVINO.</summary>
    public string OpenVinoCachePath { get; init; } = string.Empty;

    /// <summary>The detector keeps boxes scoring above this.</summary>
    [Range(0.01, 0.99)]
    public float Threshold { get; init; } = 0.1f;

    /// <summary>
    /// The tracker's tau: boxes above it go into the first association, and a box needs tau + 0.1
    /// to start a track.
    /// </summary>
    [Range(0.1, 0.99)]
    public double TrackThreshold { get; init; } = 0.4;

    /// <summary>Detections per second for a stream whose toggle says zero.</summary>
    [Range(0.1, 60)]
    public double DefaultRate { get; init; } = 1;

    /// <summary>The most frames sent to the model as one batch.</summary>
    [Range(1, 64)]
    public int MaxBatch { get; init; } = 8;
}
