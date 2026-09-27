using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Api.Observability;

/// <summary>
/// Where this replica sends what it measures, and whether it sends anything at all.
///
/// Off by default, for the same reason live streaming is: switching it on makes the process dial out
/// to a collector every few seconds, and a service that does that uninvited is a surprise in
/// somebody's egress rules. The meters exist either way - <c>dotnet-counters</c> reads every one of
/// them with nothing configured and no package involved - so this decides export, not measurement.
/// </summary>
public sealed class TelemetryOptions
{
    public const string SectionName = "Telemetry";

    public bool Enabled { get; init; }

    /// <summary>
    /// What this service is called in a dashboard. One name for every replica: which replica a
    /// measurement came from is <c>service.instance.id</c>, and folding that into the name would
    /// make a rolling update look like a new service every time.
    /// </summary>
    [Required]
    public string ServiceName { get; init; } = "storage-demo";

    /// <summary>
    /// Read from the assembly when this is empty, which is what a build stamps and a deployment
    /// should not have to repeat.
    /// </summary>
    public string? ServiceVersion { get; init; }

    /// <summary>
    /// Where the collector is. Empty falls back to the OpenTelemetry environment variables, which is
    /// how an operator who already has a collector expects to point a container at it:
    /// <c>OTEL_EXPORTER_OTLP_ENDPOINT</c>, and its per-signal forms.
    /// </summary>
    public string? Endpoint { get; init; }

    /// <summary>
    /// <c>grpc</c> or <c>http/protobuf</c>. Both are OTLP and every collector speaks both; gRPC is
    /// the default because it is what the collector's own examples listen on, and because this
    /// service is already a gRPC service and the port is easier to explain than the path.
    /// </summary>
    [RegularExpression("^(grpc|http/protobuf)$")]
    public string Protocol { get; init; } = "grpc";

    public bool Metrics { get; init; } = true;

    public bool Traces { get; init; } = true;

    /// <summary>
    /// How much of the trace traffic to keep. One is every request, which is right for a demo and
    /// for a laptop and wrong for a thousand streams: the wall of live tiles alone is a request per
    /// client per second, and the same money spent on a tenth of them tells the same story.
    /// </summary>
    [Range(0, 1)]
    public double TraceSampleRatio { get; init; } = 1;

    /// <summary>
    /// Whether to trace the subscription RPCs.
    ///
    /// Off, because a span is a thing that finished and these do not: a client watching a wall of
    /// tiles holds one open for as long as it is running, so what arrives at the collector is an
    /// hour-long span with nothing inside it, and it arrives only once the client has closed. They
    /// are measured instead, by <c>api.grpc.streams.active</c> and
    /// <c>api.grpc.stream.messages</c>, which is the shape the question actually has. Turn it on to
    /// debug one client's subscription.
    /// </summary>
    public bool TraceSubscriptions { get; init; }
}
