using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Api.Observability;

/// <summary>The exporter the meters have been waiting for.</summary>
public static class Telemetry
{
    /// <summary>The subscription RPCs, by the path a gRPC call actually arrives on.</summary>
    private const string Subscriptions = "/storagedemo.v1.Documents/Watch";

    public static WebApplicationBuilder AddTelemetry(this WebApplicationBuilder builder)
    {
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
                serviceInstanceId: builder.Configuration["Live:NodeName"] is { Length: > 0 } node
                    ? node
                    : Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName)
            .AddAttributes([
                new KeyValuePair<string, object>("deployment.environment.name", builder.Environment.EnvironmentName),
            ]));

        if (options.Metrics)
        {
            telemetry.WithMetrics(metrics => metrics
                .AddMeter(LiveMetrics.MeterName)
                .AddMeter(ApiMetrics.MeterName)

                .AddMeter("Microsoft.AspNetCore.Hosting")
                .AddMeter("Microsoft.AspNetCore.Server.Kestrel")
                .AddMeter("Grpc.AspNetCore.Server")

                .AddMeter("System.Net.Http")

                .AddMeter("System.Runtime")

                .AddMeter("Microsoft.EntityFrameworkCore")
                .AddMeter("Npgsql")

                .AddOtlpExporter(exporter => Configure(exporter, options)));
        }

        if (options.Traces)
        {
            telemetry.WithTracing(tracing => tracing
                .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(options.TraceSampleRatio)))
                .AddAspNetCoreInstrumentation(instrumentation => instrumentation.Filter = Traceable(options))
                .AddHttpClientInstrumentation()
                .AddOtlpExporter(exporter => Configure(exporter, options)));
        }

        return builder;
    }

    /// <summary>Which requests are worth a span.</summary>
    public static Func<HttpContext, bool> Traceable(TelemetryOptions options)
        => context => !context.Request.Path.StartsWithSegments("/health")
            && (options.TraceSubscriptions || !IsSubscription(context.Request.Path));

    /// <summary>Whether this is one of the four <c>Watch</c> RPCs.</summary>
    private static bool IsSubscription(PathString path)
        => path.HasValue && path.Value!.StartsWith(Subscriptions, StringComparison.Ordinal);

    private static void Configure(OtlpExporterOptions exporter, TelemetryOptions options)
    {
        exporter.Protocol = options.Protocol == "http/protobuf"
            ? OtlpExportProtocol.HttpProtobuf
            : OtlpExportProtocol.Grpc;

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
