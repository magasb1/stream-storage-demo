using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Api.Observability;

/// <summary>Where this replica sends what it measures, and whether it sends anything at all.</summary>
public sealed class TelemetryOptions
{
    public const string SectionName = "Telemetry";

    public bool Enabled { get; init; }

    /// <summary>What this service is called in a dashboard.</summary>
    [Required]
    public string ServiceName { get; init; } = "storage-demo";

    /// <summary>
    /// Read from the assembly when this is empty, which is what a build stamps and a deployment
    /// should not have to repeat.
    /// </summary>
    public string? ServiceVersion { get; init; }

    /// <summary>Where the collector is.</summary>
    public string? Endpoint { get; init; }

    /// <summary><c>grpc</c> or <c>http/protobuf</c>.</summary>
    [RegularExpression("^(grpc|http/protobuf)$")]
    public string Protocol { get; init; } = "grpc";

    public bool Metrics { get; init; } = true;

    public bool Traces { get; init; } = true;

    /// <summary>How much of the trace traffic to keep.</summary>
    [Range(0, 1)]
    public double TraceSampleRatio { get; init; } = 1;

    /// <summary>Whether to trace the subscription RPCs.</summary>
    public bool TraceSubscriptions { get; init; }
}
