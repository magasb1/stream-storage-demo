using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Json;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Api.Controllers;
using StorageDemo.Api.Grpc;
using StorageDemo.Core.Streaming;
using StorageDemo.Grpc;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// The configuration surface, through the real application: a source is written, read back with
/// what it is doing beside it, edited, and forgotten.
///
/// No encoder and no SRT anywhere here, which is the point of the seam. A source is what somebody
/// asked for, and asking for it has to work whether or not a single packet has ever arrived - a
/// test that needed a live feed to prove the configuration round-trips would be testing ingest.
/// </summary>
public sealed class LiveSourceTests : IAsyncLifetime
{
    private const string Token = "source-test-token";

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-source-tests",
        Guid.NewGuid().ToString("N"));

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private GrpcChannel _channel = null!;
    private StorageDemo.Grpc.Documents.DocumentsClient _grpc = null!;

    /// <summary>
    /// The store, in the test's own memory rather than whatever the deployment configures. Every
    /// implementation of it is read-heavy and unclever by design, so which one is underneath says
    /// nothing about the routes above it, and the one this repository ships in production is backed
    /// by Redis, which this suite deliberately does not need.
    /// </summary>
    private sealed class MemorySourceStore : ILiveSourceStore
    {
        private readonly ConcurrentDictionary<string, LiveSource> _sources = new(StringComparer.Ordinal);

        public Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<LiveSource>>([.. _sources.Values]);

        public Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default)
            => Task.FromResult(_sources.GetValueOrDefault(name));

        public Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default)
        {
            _sources[source.Name] = source;
            return Task.CompletedTask;
        }

        public Task RemoveAsync(string name, CancellationToken cancellationToken = default)
        {
            _sources.TryRemove(name, out _);
            return Task.CompletedTask;
        }
    }

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_root);

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_root, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_root, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            // Nothing is ever sent to either port; the live guard only has to be switched on.
            builder.UseSetting("Live:IngestPort", SrtSenders.FreePort().ToString());
            builder.UseSetting("Live:ConsumptionPort", SrtSenders.FreePort().ToString());
            builder.UseEnvironment("Production");

            builder.ConfigureTestServices(services =>
                services.AddSingleton<ILiveSourceStore, MemorySourceStore>());
        });

        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        _channel = GrpcChannel.ForAddress(
            _factory.Server.BaseAddress,
            new GrpcChannelOptions { HttpHandler = _factory.Server.CreateHandler() });

        _grpc = new StorageDemo.Grpc.Documents.DocumentsClient(_channel);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _channel.Dispose();
        _client.Dispose();
        await _factory.DisposeAsync();

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

    /// <summary>
    /// The whole row survives the trip, forwards included, and the listing pairs it with what it is
    /// doing without a second call. Nothing is on air, so the stream half is null, and the row is
    /// still listed: a source only appearing once its feed works would hide exactly the source an
    /// operator came to look at.
    /// </summary>
    [Fact]
    public async Task A_source_and_its_forwards_round_trip_through_the_configuration()
    {
        const string name = "live/camera1";

        var saved = await Save(name, new SaveLiveSourceRequest(
            name,
            "srt://198.51.100.7:9000?streamid=camera1",
            Enabled: true,
            [new ForwardTarget("gateway", "srt://198.51.100.9:9100?mode=caller"),
             new ForwardTarget("wall", "udp://239.0.0.7:5000", Enabled: false)]));

        Assert.Equal(HttpStatusCode.OK, saved.StatusCode);

        // The name carries a slash, which is why the route is a catch-all: a stream name is a
        // resource path, and a listing that could not address one would be half a surface.
        var one = await _client.GetFromJsonAsync<LiveSourceResponse>($"/api/live/sources/{name}");

        Assert.NotNull(one);
        Assert.Equal(name, one.Source.Name);
        Assert.Equal("srt://198.51.100.7:9000?streamid=camera1", one.Source.Url);
        Assert.True(one.Source.Enabled);
        Assert.Null(one.Stream);

        Assert.Collection(
            one.Source.Forwards,
            forward =>
            {
                Assert.Equal("gateway", forward.Id);
                Assert.Equal("srt://198.51.100.9:9100?mode=caller", forward.Url);
                Assert.True(forward.Enabled);
            },
            forward =>
            {
                Assert.Equal("wall", forward.Id);
                Assert.False(forward.Enabled);
            });

        var listed = await _client.GetFromJsonAsync<List<LiveSourceResponse>>("/api/live/sources");

        Assert.NotNull(listed);
        Assert.Single(listed, row => row.Source.Name == name && row.Stream is null);

        // A name nobody configured is absent rather than empty.
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await _client.GetAsync("/api/live/sources/never-configured")).StatusCode);
    }

    /// <summary>
    /// An operator adding a forward types a URL, not an identifier, so the server mints one. The
    /// second half is the one that matters: the id has to survive an edit, because that is what
    /// makes a forward whose URL changed the same forward rather than a new one, and it is what
    /// the reported status of a running copy is keyed by.
    /// </summary>
    [Fact]
    public async Task A_forward_saved_without_an_id_is_given_one_and_keeps_it_across_an_edit()
    {
        const string name = "minted";

        var first = await Saved(name, new SaveLiveSourceRequest(
            name,
            null,
            Forwards: [new ForwardTarget(string.Empty, "udp://239.0.0.1:5000")]));

        var id = Assert.Single(first.Forwards).Id;

        Assert.False(string.IsNullOrWhiteSpace(id), "the forward came back without an id");

        var second = await Saved(name, new SaveLiveSourceRequest(
            name,
            null,
            Forwards: [new ForwardTarget(id, "udp://239.0.0.2:5001"), new ForwardTarget(string.Empty, "rtp://239.0.0.3:5002")]));

        Assert.Equal(id, second.Forwards[0].Id);
        Assert.Equal("udp://239.0.0.2:5001", second.Forwards[0].Url);
        Assert.NotEqual(id, second.Forwards[1].Id);
        Assert.False(string.IsNullOrWhiteSpace(second.Forwards[1].Id));

        // Two forwards under one id are two rows nothing could tell apart afterwards.
        var collided = await Save(name, new SaveLiveSourceRequest(
            name,
            null,
            Forwards: [new ForwardTarget(id, "udp://239.0.0.2:5001"), new ForwardTarget(id, "udp://239.0.0.4:5003")]));

        Assert.Equal(HttpStatusCode.BadRequest, collided.StatusCode);
    }

    /// <summary>
    /// Both directions are held to the allowlist, and both refusals matter. A source URL outside it
    /// makes the service read anywhere libav can reach; a forward URL outside it makes the service
    /// write there, which is the same hole pointed the other way.
    /// </summary>
    [Fact]
    public async Task A_source_may_not_name_a_transport_outside_the_allowed_list()
    {
        var pulled = await Save("intruder", new SaveLiveSourceRequest("intruder", "file:///etc/passwd"));

        Assert.Equal(HttpStatusCode.BadRequest, pulled.StatusCode);

        var forwarded = await Save("leaker", new SaveLiveSourceRequest(
            "leaker",
            "srt://198.51.100.7:9000",
            Forwards: [new ForwardTarget(string.Empty, "file:///etc/passwd")]));

        Assert.Equal(HttpStatusCode.BadRequest, forwarded.StatusCode);

        // A URL with no scheme at all is a path, and treating it as anything else would leave the
        // allowlist trivially walked around.
        var bare = await Save("bare", new SaveLiveSourceRequest("bare", "/etc/passwd"));

        Assert.Equal(HttpStatusCode.BadRequest, bare.StatusCode);

        Assert.Empty((await _client.GetFromJsonAsync<List<LiveSourceResponse>>("/api/live/sources"))!);
    }

    /// <summary>
    /// A configured name is a stream name and is held to the same rule, because the two meet: an
    /// encoder presents a name at the handshake and a source claims one here, and a name only one
    /// of them would accept is a name the other could never serve.
    /// </summary>
    /// <remarks>
    /// The names here are ones that survive the trip. A name like <c>../../etc/passwd</c> is
    /// normalised away by the client before the request is sent and never reaches the route at all,
    /// so asserting on it would prove something about <see cref="HttpClient"/> rather than about
    /// this service; the rule refuses it either way, and <c>StreamNameTests</c> is where that is
    /// pinned.
    /// </remarks>
    [Fact]
    public async Task A_source_whose_name_is_not_a_usable_stream_name_is_refused()
    {
        // Outside the allowed character set, which is deliberately narrower than what a URL will
        // carry: a name is also a registry key and part of a document name.
        var spaced = await Save("camera 1", new SaveLiveSourceRequest("camera 1", "udp://239.0.0.1:5000"));

        Assert.Equal(HttpStatusCode.BadRequest, spaced.StatusCode);

        var long_ = new string('c', 200);

        Assert.Equal(
            HttpStatusCode.BadRequest,
            (await Save(long_, new SaveLiveSourceRequest(long_, "udp://239.0.0.1:5000"))).StatusCode);
    }

    /// <summary>
    /// A body naming a different source than the route is a caller that has lost track of which row
    /// it is editing, and either answer to "which did it mean" silently overwrites the other row.
    /// </summary>
    [Fact]
    public async Task A_route_and_a_body_that_name_different_sources_are_refused()
    {
        var response = await Save("camera-a", new SaveLiveSourceRequest("camera-b", "udp://239.0.0.1:5000"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync("/api/live/sources/camera-a")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync("/api/live/sources/camera-b")).StatusCode);
    }

    [Fact]
    public async Task Deleting_a_source_forgets_the_configuration()
    {
        const string name = "temporary";

        Assert.Equal(HttpStatusCode.OK, (await Save(name, new SaveLiveSourceRequest(name, "udp://239.0.0.1:5000"))).StatusCode);

        var deleted = await _client.DeleteAsync($"/api/live/sources/{name}");

        Assert.Equal(HttpStatusCode.NoContent, deleted.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync($"/api/live/sources/{name}")).StatusCode);

        // Forgetting something that is already forgotten is not a failure: a delete that is retried
        // after a lost response must not report one.
        Assert.Equal(
            HttpStatusCode.NoContent,
            (await _client.DeleteAsync($"/api/live/sources/{name}")).StatusCode);
    }

    /// <summary>
    /// These routes decide what the service connects out to, so they are if anything more sensitive
    /// than the ones already behind the token.
    /// </summary>
    [Fact]
    public async Task The_endpoints_are_closed_without_the_token()
    {
        using var anonymous = _factory.CreateClient();

        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/live/sources")).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.GetAsync("/api/live/sources/x")).StatusCode);
        Assert.Equal(HttpStatusCode.Unauthorized, (await anonymous.DeleteAsync("/api/live/sources/x")).StatusCode);

        Assert.Equal(
            HttpStatusCode.Unauthorized,
            (await anonymous.PutAsJsonAsync("/api/live/sources/x", new SaveLiveSourceRequest("x", "udp://239.0.0.1:5000"))).StatusCode);

        // Refused before it is stored, not merely refused an answer.
        Assert.Empty((await _client.GetFromJsonAsync<List<LiveSourceResponse>>("/api/live/sources"))!);
    }

    /// <summary>
    /// The same three operations over gRPC, which is the surface the desktop client actually uses:
    /// a source is saved with a forward that has no id, comes back with one, appears on the listing
    /// with no stream beside it because none is on air, and is then forgotten.
    ///
    /// Both surfaces exist on purpose - REST for a command-line tool or an agent, gRPC for the
    /// client - so a feature that worked on one of them would be half delivered.
    /// </summary>
    [Fact]
    public async Task The_same_source_is_saved_listed_and_deleted_over_grpc()
    {
        var saved = await _grpc.SaveLiveSourceAsync(
            new LiveSourceMessage
            {
                Name = "grpc/camera",
                Url = "srt://198.51.100.7:9000",
                Enabled = true,
                Forwards = { new ForwardTargetMessage { Url = "udp://239.0.0.1:5000", Enabled = true } },
            },
            Metadata());

        var id = Assert.Single(saved.Forwards).Id;

        Assert.False(string.IsNullOrWhiteSpace(id), "the forward came back without an id");
        Assert.NotNull(saved.UpdatedAt);

        var listed = await _grpc.ListLiveSourcesAsync(new Empty(), Metadata());
        var source = Assert.Single(listed.Sources);

        Assert.Equal("grpc/camera", source.Name);
        Assert.Equal(id, Assert.Single(source.Forwards).Id);

        // Configuration, not state: nothing is on air, and the row is listed all the same.
        Assert.Null(source.Stream);

        await _grpc.DeleteLiveSourceAsync(new LiveStreamName { Name = "grpc/camera" }, Metadata());

        Assert.Empty((await _grpc.ListLiveSourcesAsync(new Empty(), Metadata())).Sources);
    }

    /// <summary>
    /// The allowlist and the token are properties of the service, not of REST. A second surface
    /// that enforced neither would be a way round both.
    /// </summary>
    [Fact]
    public async Task The_grpc_surface_enforces_the_same_allowlist_and_the_same_token()
    {
        var refused = await Assert.ThrowsAsync<RpcException>(
            () => _grpc.SaveLiveSourceAsync(
                new LiveSourceMessage { Name = "intruder", Url = "file:///etc/passwd" },
                Metadata()).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, refused.StatusCode);

        var anonymous = await Assert.ThrowsAsync<RpcException>(
            () => _grpc.ListLiveSourcesAsync(new Empty()).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, anonymous.StatusCode);

        Assert.Equal(
            StatusCode.Unauthenticated,
            (await Assert.ThrowsAsync<RpcException>(
                () => _grpc.SaveLiveSourceAsync(new LiveSourceMessage { Name = "x" }).ResponseAsync)).StatusCode);

        Assert.Equal(
            StatusCode.Unauthenticated,
            (await Assert.ThrowsAsync<RpcException>(
                () => _grpc.DeleteLiveSourceAsync(new LiveStreamName { Name = "x" }).ResponseAsync)).StatusCode);
    }

    private static Metadata Metadata() => new() { { LiveTokenInterceptor.Header, Token } };

    /// <summary>
    /// A fixed camera's position survives the trip and the gRPC port agrees, and a value outside an
    /// ST 0601 item's range is refused rather than stored.
    ///
    /// Both surfaces, because they funnel through one <c>LiveSourceRules.Refuse</c> and a row one
    /// port could save and the other refuses would make the ranges a suggestion. Refused rather
    /// than saturated at the encoder: a camera configured at 25,000 metres would otherwise be
    /// published, conformingly, at 19,000, and nothing downstream could tell that from a
    /// measurement.
    /// </summary>
    [Fact]
    public async Task A_fixed_cameras_position_round_trips_and_an_impossible_one_is_refused()
    {
        const string name = "mast/north";

        var sensor = new StaticSensor(
            Longitude: -1.826,
            Latitude: 51.179,
            AltitudeMetres: 143.5,
            TrueBearing: 218.4,
            RelativeElevation: -12.75,
            HorizontalFov: 6.2,
            VerticalFov: 3.5,
            Classification: "SECRET");

        var saved = await Saved(name, new SaveLiveSourceRequest(name, Url: null, Enabled: true, Forwards: null, StaticSensor: sensor));

        Assert.Equal(sensor, saved.StaticSensor);

        var read = await _client.GetFromJsonAsync<LiveSourceResponse>($"/api/live/sources/{name}");

        Assert.NotNull(read);
        Assert.Equal(sensor, read.Source.StaticSensor);

        // The same row over gRPC, where the marking is optional so that absent and empty stay
        // different answers.
        var listed = await _grpc.ListLiveSourcesAsync(new Empty(), Metadata());
        var row = Assert.Single(listed.Sources, source => source.Name == name);

        Assert.NotNull(row.StaticSensor);
        Assert.Equal(sensor.Latitude, row.StaticSensor.Latitude);
        Assert.Equal(sensor.TrueBearing, row.StaticSensor.TrueBearing);
        Assert.Equal("SECRET", row.StaticSensor.Classification);

        // Every range refused is the ST 0601 item's own, so these are the values that have no
        // encoding rather than values somebody thought were unlikely.
        foreach (var impossible in new[]
                 {
                     sensor with { AltitudeMetres = 25_000 },
                     sensor with { Latitude = 91 },
                     sensor with { Longitude = 181 },
                     sensor with { TrueBearing = 361 },
                     sensor with { RelativeElevation = -181 },
                     sensor with { HorizontalFov = 181 },
                     sensor with { VerticalFov = -1 },
                     sensor with { Classification = "COSMIC" },
                 })
        {
            var refused = await Save("mast/south", new SaveLiveSourceRequest("mast/south", null, true, null, impossible));

            Assert.Equal(HttpStatusCode.BadRequest, refused.StatusCode);
        }

        Assert.Equal(
            HttpStatusCode.NotFound,
            (await _client.GetAsync("/api/live/sources/mast/south")).StatusCode);

        // Removing the configuration is sending the row back without it, the same way a forward is
        // removed: the source is the aggregate.
        var cleared = await Saved(name, new SaveLiveSourceRequest(name, Url: null, Enabled: true));

        Assert.Null(cleared.StaticSensor);
    }

    /// <summary>
    /// Half a configuration is refused rather than completed with zeros.
    ///
    /// The case the nesting was once claimed to prevent and does not: nesting makes the presence of
    /// the whole object detectable and says nothing about its members, so a latitude sent alone
    /// used to store a camera at 0 degrees north, 0 degrees east - the Gulf of Guinea - and pass
    /// every range check, because every ST 0601 range here includes zero.
    ///
    /// Three guards now stand where one was assumed, because the hole is in three places. The
    /// members are required, so a partial object does not deserialise. The gRPC scalars are
    /// optional, because proto3 cannot otherwise tell a field left out from a deliberate zero. And
    /// the field of view is refused at zero, which catches the object of explicit zeros that gets
    /// past both - a camera with no field of view is not a camera.
    /// </summary>
    [Fact]
    public async Task Half_a_configuration_is_refused_rather_than_filled_in_with_zeros()
    {
        const string name = "mast/partial";

        // Sent as raw JSON rather than as the record, because the record cannot express the bug:
        // this is what a caller sends, and what used to be stored as a reading.
        var partial = await _client.PutAsync(
            $"/api/live/sources/{name}",
            JsonContent.Create(new Dictionary<string, object?>
            {
                ["name"] = name,
                ["enabled"] = true,
                ["staticSensor"] = new Dictionary<string, object?> { ["latitude"] = 51.2 },
            }));

        Assert.Equal(HttpStatusCode.BadRequest, partial.StatusCode);

        // An object of explicit zeros deserialises, so the rule rather than the shape refuses it.
        var zeroed = await Save(name, new SaveLiveSourceRequest(
            name,
            Url: null,
            Enabled: true,
            Forwards: null,
            StaticSensor: new StaticSensor(0, 0, 0, 0, 0, 0, 0)));

        Assert.Equal(HttpStatusCode.BadRequest, zeroed.StatusCode);

        // And a camera pointing somewhere real with no field of view is the same refusal, so the
        // rule is about the field of view rather than about the row happening to be all zeros.
        var blind = await Save(name, new SaveLiveSourceRequest(
            name,
            Url: null,
            Enabled: true,
            Forwards: null,
            StaticSensor: new StaticSensor(-1.826, 51.179, 143.5, 218.4, -12.75, 0, 0)));

        Assert.Equal(HttpStatusCode.BadRequest, blind.StatusCode);

        // Nothing was stored by any of the three.
        Assert.Equal(
            HttpStatusCode.NotFound,
            (await _client.GetAsync($"/api/live/sources/{name}")).StatusCode);

        // The same over gRPC, where proto3 would otherwise make an unset scalar and a deliberate
        // zero the same bytes. The message carries a latitude and nothing else.
        var refused = await Assert.ThrowsAsync<RpcException>(
            () => _grpc.SaveLiveSourceAsync(
                new LiveSourceMessage
                {
                    Name = "grpc/partial",
                    Enabled = true,
                    StaticSensor = new StaticSensorMessage { Latitude = 51.2 },
                },
                Metadata()).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, refused.StatusCode);
        Assert.Contains("longitude", refused.Status.Detail, StringComparison.Ordinal);

        Assert.DoesNotContain(
            (await _grpc.ListLiveSourcesAsync(new Empty(), Metadata())).Sources,
            source => source.Name == "grpc/partial");
    }

    private Task<HttpResponseMessage> Save(string name, SaveLiveSourceRequest request)
        => _client.PutAsJsonAsync($"/api/live/sources/{name}", request);

    private async Task<LiveSource> Saved(string name, SaveLiveSourceRequest request)
    {
        var response = await Save(name, request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        return (await response.Content.ReadFromJsonAsync<LiveSource>())!;
    }

}
