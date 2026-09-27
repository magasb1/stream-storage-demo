using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Observability;

/// <summary>
/// The exporter the meters have been waiting for.
///
/// Every instrument in this service is a plain <c>System.Diagnostics.Metrics</c> instrument and
/// always has been, which is why this file adds no measurement of its own: it subscribes to what is
/// already published and sends it somewhere. With nothing configured the meters still work, and
/// <c>dotnet-counters monitor -n StorageDemo.Api --counters StorageDemo.Live,StorageDemo.Api</c>
/// reads all of them with no package and no collector.
///
/// OTLP to a collector rather than a scrape endpoint on this process, which is a deliberate choice
/// and the reason the export took this long to ship. The Prometheus exporter for .NET has never had
/// a stable release - it is still beta as this is written, three years on - and a media service with
/// a publicly reachable port is the wrong place to take a dependency on a preview package whose
/// job is to open another one. A collector alongside the pod scrapes nothing off it, speaks a stable
/// protocol, and is where the Prometheus shape belongs anyway: the collector exposes it, Prometheus
/// scrapes the collector, and Grafana reads Prometheus. The whole stack is in
/// <c>docker/docker-compose.observability.yml</c>, and <c>docs/observability.md</c> says what to
/// look at.
/// </summary>
public static class Telemetry
{
    /// <summary>
    /// The subscription RPCs, by the path a gRPC call actually arrives on. Prefix-matched, so the
    /// four <c>Watch</c> RPCs are one line rather than four.
    /// </summary>
    private const string Subscriptions = "/storagedemo.v1.Documents/Watch";

    public static WebApplicationBuilder AddTelemetry(this WebApplicationBuilder builder)
    {
        // Registered whether or not anything is exported. The instruments are how this service
        // measures itself, and a developer reading them with dotnet-counters should not have to
        // configure a collector first.
        builder.Services.AddSingleton<ApiMetrics>();

        builder.Services.AddOptions<TelemetryOptions>()
            .Bind(builder.Configuration.GetSection(TelemetryOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        var options = builder.Configuration.GetSection(TelemetryOptions.SectionName).Get<TelemetryOptions>()
            ?? new TelemetryOptions();

        if (!options.Enabled)
        {
            return builder;
        }

        var telemetry = builder.Services.AddOpenTelemetry().ConfigureResource(resource => resource
            .AddService(
                options.ServiceName,
                serviceVersion: Version(options),
                // The same name the stream registry records as a stream's owner, by the same rule:
                // the configured node name, then the pod name the orchestrator injected, then the
                // machine. That is what lets a dashboard and `GET /api/live` be talked about in one
                // sentence - the pod a metric came from is the pod a stream says it belongs to.
                serviceInstanceId: builder.Configuration["Live:NodeName"] is { Length: > 0 } node
                    ? node
                    : Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName)
            .AddAttributes([
                new KeyValuePair<string, object>("deployment.environment.name", builder.Environment.EnvironmentName),
            ]));

        if (options.Metrics)
        {
            telemetry.WithMetrics(metrics => metrics
                // This service's own two meters. Everything domain-shaped is in the first: streams
                // owned, bytes fed, packets lost, recordings, the heartbeat's own latency.
                .AddMeter(LiveMetrics.MeterName)
                .AddMeter(ApiMetrics.MeterName)

                // The framework's, which is most of the request story and costs nothing to ask for.
                // Hosting times every request on both surfaces, because a gRPC call is an HTTP/2
                // request; Kestrel answers for connections and queues; the gRPC meter adds the
                // status codes a trailer hides from the HTTP view. Its instrument names are the
                // pre-conventions ones - `total-calls`, `current-calls`, `calls-failed` - which is
                // gRPC's business and not something to rename on the way past.
                .AddMeter("Microsoft.AspNetCore.Hosting")
                .AddMeter("Microsoft.AspNetCore.Server.Kestrel")
                .AddMeter("Grpc.AspNetCore.Server")

                // The peer hop, S3 and every other call out of this process.
                .AddMeter("System.Net.Http")

                // Built into the runtime since .NET 9, so the working set, the garbage collector and
                // the thread pool arrive without an instrumentation package. Working set is the one
                // to watch: it is what bounds a pod, and it is why this project runs the workstation
                // collector against the ASP.NET default (see StorageDemo.Api.csproj).
                .AddMeter("System.Runtime")

                // Present only on the PostgreSQL path. A meter nothing publishes to costs nothing.
                .AddMeter("Microsoft.EntityFrameworkCore")
                .AddMeter("Npgsql")

                .AddOtlpExporter(exporter => Configure(exporter, options)));
        }

        if (options.Traces)
        {
            telemetry.WithTracing(tracing => tracing
                .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(options.TraceSampleRatio)))
                .AddAspNetCoreInstrumentation(instrumentation => instrumentation.Filter = Traceable(options))
                // The in-cluster hop, which is the trace worth having: a snapshot asked of the wrong
                // replica, or a viewer relayed to the owner, is two spans on two pods under one
                // trace id, and that is the picture nothing else in this service can draw.
                .AddHttpClientInstrumentation()
                .AddOtlpExporter(exporter => Configure(exporter, options)));
        }

        return builder;
    }

    /// <summary>
    /// Which requests are worth a span. Everything, less the two kinds that would drown it.
    ///
    /// The subscription RPCs are excluded because a span describes something that finished: a client
    /// watching the live wall holds one open for as long as it runs, so the span arrives late,
    /// arrives empty, and is an hour wide. The probes are excluded because Kubernetes asks twice a
    /// second forever and the answer is always the same; a readiness probe that starts failing shows
    /// up in the pod's own events and in the request metrics, not in a trace nobody opened.
    /// </summary>
    /// <remarks>Public so the tests can ask it directly; nothing else calls it.</remarks>
    public static Func<HttpContext, bool> Traceable(TelemetryOptions options)
        => context => !context.Request.Path.StartsWithSegments("/health")
            && (options.TraceSubscriptions || !IsSubscription(context.Request.Path));

    /// <summary>
    /// Whether this is one of the four <c>Watch</c> RPCs.
    ///
    /// A plain prefix comparison, and not <c>StartsWithSegments</c>, which matches only on a segment
    /// boundary: the RPCs are <c>Watch</c>, <c>WatchLiveStreams</c>, <c>WatchLiveKlv</c> and
    /// <c>WatchLiveDetections</c>, and only the first of them ends where the prefix does. Written the
    /// other way this excluded one of the four and quietly traced the three that matter most.
    /// </summary>
    private static bool IsSubscription(PathString path)
        => path.HasValue && path.Value!.StartsWith(Subscriptions, StringComparison.Ordinal);

    private static void Configure(OtlpExporterOptions exporter, TelemetryOptions options)
    {
        exporter.Protocol = options.Protocol == "http/protobuf"
            ? OtlpExportProtocol.HttpProtobuf
            : OtlpExportProtocol.Grpc;

        // Left alone when nothing was configured, so the OpenTelemetry environment variables decide
        // and an operator who already has a collector points this at it the way they point
        // everything else at it.
        if (options.Endpoint is { Length: > 0 } endpoint)
        {
            exporter.Endpoint = new Uri(endpoint);
        }
    }

    private static string Version(TelemetryOptions options)
        => options.ServiceVersion is { Length: > 0 } configured
            ? configured
            : Assembly.GetEntryAssembly()?.GetName().Version?.ToString() ?? "0.0.0";
}
