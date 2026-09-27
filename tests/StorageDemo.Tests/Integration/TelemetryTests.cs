using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Api.Observability;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// The exporter against the real application, with the collector deliberately absent.
///
/// That is the case worth a test. Measurement is not supposed to be load-bearing: a collector that
/// has gone away, or was never there, must cost a request nothing and must not stop the service
/// starting. The opposite arrangement - telemetry that can take a service down - is a famous way to
/// lose an afternoon, and it is cheap to prove this one cannot.
/// </summary>
public sealed class TelemetryTests
{
    [Fact]
    public async Task A_request_is_measured_and_a_collector_that_is_not_there_costs_nothing()
    {
        var root = Path.Combine(
            Path.GetTempPath(),
            "storage-demo-telemetry-tests",
            Guid.NewGuid().ToString("N"));

        await using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(root, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(root, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");

            // Off, so this test needs no media ports and no libsrt: what it is about is the
            // exporter, and the live surface is measured by the streaming tests.
            builder.UseSetting("Live:Enabled", "false");

            // On, and pointed at a port nothing is listening on. Port 1 is reserved and unbindable
            // on every platform this runs on, so this is not a port a developer might happen to have
            // something on.
            builder.UseSetting("Telemetry:Enabled", "true");
            builder.UseSetting("Telemetry:Endpoint", "http://127.0.0.1:1");
            builder.UseEnvironment("Production");
        });

        var client = factory.CreateClient();

        // Resolved off the running server rather than the factory, and the listener started before
        // the request, because a counter is an event: a listener attached afterwards sees nothing
        // however many times it was added to.
        using var meters = new Meters(factory.Server.Services.GetRequiredService<ApiMetrics>());

        var response = await client.GetAsync($"/api/documents/{Guid.NewGuid()}/content");

        // The service answers normally with an unreachable collector configured, which is the point.
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        var downloads = meters.Of("api.documents.downloads");
        var recorded = Assert.Single(downloads);

        Assert.Equal(1, recorded.Value);
        Assert.Equal(
            [
                new KeyValuePair<string, object?>("surface", "rest"),
                new KeyValuePair<string, object?>("kind", "document"),
                new KeyValuePair<string, object?>("outcome", "missing"),
            ],
            recorded.Tags);

        Directory.Delete(root, recursive: true);
    }

    /// <summary>
    /// Which requests are worth a span.
    ///
    /// The four subscription RPCs share a prefix that only the first of them ends at, which is
    /// exactly where a segment-wise prefix match gets this wrong and quietly traces the three
    /// hour-long ones it was written to exclude.
    /// </summary>
    [Theory]
    [InlineData("/health/live", false)]
    [InlineData("/health/ready", false)]
    [InlineData("/storagedemo.v1.Documents/Watch", false)]
    [InlineData("/storagedemo.v1.Documents/WatchLiveStreams", false)]
    [InlineData("/storagedemo.v1.Documents/WatchLiveKlv", false)]
    [InlineData("/storagedemo.v1.Documents/WatchLiveDetections", false)]
    [InlineData("/storagedemo.v1.Documents/ListLive", true)]
    [InlineData("/storagedemo.v1.Documents/Upload", true)]
    [InlineData("/api/live/preview/camera1", true)]
    public void The_long_lived_subscriptions_and_the_probes_are_left_out_of_traces(string path, bool traced)
    {
        var traceable = Telemetry.Traceable(new TelemetryOptions());

        Assert.Equal(traced, traceable(Request(path)));
    }

    /// <summary>One switch turns the subscriptions back on, for debugging a single client.</summary>
    [Fact]
    public void Subscriptions_can_be_traced_on_purpose()
    {
        var traceable = Telemetry.Traceable(new TelemetryOptions { TraceSubscriptions = true });

        Assert.True(traceable(Request("/storagedemo.v1.Documents/WatchLiveKlv")));

        // The probes stay out either way. Kubernetes asks twice a second forever and the answer is
        // always the same.
        Assert.False(traceable(Request("/health/ready")));
    }

    private static HttpContext Request(string path)
    {
        var context = new DefaultHttpContext();

        context.Request.Path = path;

        return context;
    }

    /// <summary>
    /// With export off - the default, and how every existing test hosts this application - the
    /// meters still exist, because they are how the service measures itself and
    /// <c>dotnet-counters</c> reads them with nothing configured at all.
    /// </summary>
    [Fact]
    public void The_meters_exist_whether_or_not_anything_is_exported()
    {
        var builder = WebApplication.CreateBuilder();

        builder.AddTelemetry();

        using var app = builder.Build();

        Assert.NotNull(app.Services.GetRequiredService<ApiMetrics>());
    }
}
