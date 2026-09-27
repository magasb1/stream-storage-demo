using System.Diagnostics;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Core.Coordination;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// Two replicas in one process: the same application twice, each with its own ports and its own
/// node name, sharing one <see cref="InMemoryLiveStreamRegistry"/> and one <see
/// cref="InMemoryLock"/>.
/// </summary>
internal sealed class LiveReplicas : IAsyncDisposable
{
    public const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";
    public const string NoFfmpegSrt = "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.";

    /// <summary>Set on every replica, so the hop between two of them carries it or is refused.</summary>
    public const string Token = "replica-test-token";

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-replica-tests",
        Guid.NewGuid().ToString("N"));

    private readonly InMemoryLock _coordination = new();
    private readonly List<Process> _callers = [];
    private readonly List<WebApplicationFactory<Program>> _hosts = [];
    private readonly List<HttpClient> _clients = [];

    public LiveReplicas() => Directory.CreateDirectory(_root);

    /// <summary>What every replica here sees, and what every assertion is made against.</summary>
    public InMemoryLiveStreamRegistry Registry { get; } = new();

    public static bool HasSrt() => FfmpegLibrary.InputProtocols().Contains("srt");

    /// <summary>One replica: its own ports, its own name, somebody else's registry.</summary>
    public WebApplicationFactory<Program> Start(
        string node,
        int ingestPort,
        ILiveStreamRegistry? registry = null,
        int graceSeconds = 5,
        WebApplicationFactory<Program>? peer = null,
        int maxStreams = 0)
    {
        var host = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_root, node, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_root, node, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            builder.UseSetting("Live:NodeName", node);
            builder.UseSetting("Live:PeerBaseUrl", $"http://{node}");
            builder.UseSetting("Live:IngestPort", ingestPort.ToString());
            builder.UseSetting("Live:ConsumptionPort", (ingestPort + 1).ToString());
            builder.UseSetting("Live:GracePeriodSeconds", graceSeconds.ToString());
            builder.UseSetting("Live:FeedTimeoutSeconds", "2");
            builder.UseSetting("Live:MaxStreams", maxStreams.ToString());

            // One file under this fixture's own sandbox, not per node: a source is shared cluster
            // state, the same reason pod-a and pod-b share one Registry below.
            builder.UseSetting("Live:SourceFile", Path.Combine(_root, "sources.json"));
            builder.UseEnvironment("Production");

            // Registered last, so these instances are what the application resolves.
            builder.ConfigureServices(services =>
            {
                services.AddSingleton(registry ?? Registry);
                services.AddSingleton<IDistributedLock>(_coordination);

                if (peer is not null)
                {
                    services.AddHttpClient(LiveOptions.PeerClient)
                        .ConfigurePrimaryHttpMessageHandler(peer.Server.CreateHandler);
                }
            });
        });

        _hosts.Add(host);

        // The host starts on the first client, and with it the ingest port and the heartbeat.
        _clients.Add(Client(host));

        return host;
    }

    /// <summary>A client carrying the token, for a test that calls a replica's API directly.</summary>
    public HttpClient Client(WebApplicationFactory<Program> host)
    {
        var client = host.CreateClient();

        client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        return client;
    }

    /// <summary>Scratch space that goes with the fixture, for a file a sender is to push.</summary>
    public string Root => _root;

    public Process Send(int ingestPort, string name, string? file = null) => Track(
        SrtSenders.StartSender(ingestPort, $"#!::r={name},m=publish", file: file));

    /// <summary>A player on a replica's consumption port, which is <c>ingestPort + 1</c>.</summary>
    public Process Watch(int ingestPort, string name, double from = 0) => Track(
        SrtSenders.StartViewer(
            ingestPort + 1,
            from > 0 ? $"#!::r={name},user_from={from},m=request" : $"#!::r={name},m=request"));

    /// <summary>A registry entry nothing will ever update again, which is a pod that was killed.</summary>
    public static LiveStream Entry(
        string name,
        string owner,
        DateTimeOffset heartbeat,
        DateTimeOffset? startedAt = null)
        => new(
            name,
            LiveStreamState.Live,
            startedAt ?? heartbeat,
            heartbeat,
            owner,
            null,
            1,
            1,
            false,
            true,
            false,
            0,
            null,
            null,
            "aaaaaaaa");

    /// <param name="describe">What to say when it never came true, so a failure names its half.</param>
    public static async Task Until(Func<Task<bool>> condition, TimeSpan timeout, string describe)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }

        Assert.Fail($"{describe} within {timeout}");
    }

    public static async Task<bool> Exited(Process caller, TimeSpan within)
    {
        using var deadline = new CancellationTokenSource(within);

        try
        {
            await caller.WaitForExitAsync(deadline.Token);

            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var caller in _callers)
        {
            SrtSenders.Kill(caller);
        }

        foreach (var client in _clients)
        {
            client.Dispose();
        }

        foreach (var host in _hosts)
        {
            await host.DisposeAsync();
        }

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

    private Process Track(Process caller)
    {
        _callers.Add(caller);

        return caller;
    }
}
