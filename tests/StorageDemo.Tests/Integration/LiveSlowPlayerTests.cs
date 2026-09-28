using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StorageDemo.Api.Controllers;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// What a slow player on the consumption port costs this replica, which is the question
/// <see cref="LiveSlowViewerTests"/> answers for the other route.
///
/// The two routes fail differently and that is the whole point. A relayed viewer is written to
/// through a thread-pool continuation and a bounded queue, and used to be written to synchronously -
/// one held worker per slow viewer. A direct player is written to by
/// <see cref="SrtSocketStream.Write"/>, which blocks inline in <c>srt_sendmsg</c> because libsrt has
/// no asynchronous send, so on paper a player that will not take its bytes holds the pool thread
/// serving it with nothing bounding how many. Against that, SRT is a live protocol: it is supposed
/// to throw away what it can no longer deliver inside the latency window rather than wait for a peer
/// that has stopped listening. Which of the two actually happens decided whether a send timeout was
/// worth adding, and nobody had measured it.
///
/// Measured here, and libsrt saves us: the send does not wait. Ten players reading three tenths of a
/// 12 Mbit/s stream moved this process's pool from five workers to five, its threads from 29 to 28,
/// and skipped nothing to live in two minutes - against the 219 workers two hundred slow relayed
/// viewers cost before #19. What each of them costs instead is memory, and it is bounded: the send
/// buffer libsrt fills for that socket, about twelve megabytes with its defaults, plus that viewer's
/// own queue, which is four seconds of the stream. Resident memory went from 197 MB to 404 MB for
/// ten of them and flattened as each socket's buffer filled, which is what a bound looks like.
///
/// Past that the transport throws the surplus away, and the second test here is libsrt's own account
/// of it from the receiving end: a reader taking three tenths of this stream gets 1.9 Mbit/s of it
/// and loses about 970 packets a second, because its receive buffer is full and it has nowhere to put
/// what arrives. That is the answer to #11 - a slow direct player loses picture where a slow relayed
/// one used to hold a thread.
///
/// A player that paces itself strictly is eventually disconnected by its own library rather than by
/// this service: libsrt breaks a connection whose receiver can no longer place what is arriving, and
/// says so plainly. Three player shapes and three outcomes, all of them survivable here, none of
/// them a held thread.
///
/// The bitrate is part of the measurement rather than a detail of it. This pushes 12 Mbit/s so that
/// the buffers at both ends fill inside a window a test can wait out; at the 600 kbit/s the rest of
/// the suite sends, the same player is absorbed for several minutes before anything happens at all,
/// and a test that waited thirty seconds would measure nothing and report it as a clean bill of
/// health. What is bounded here is bytes, so how long it takes to reach is the sender's bitrate.
///
/// In <see cref="LoadCollection"/>, for the reason <see cref="LiveSlowViewerTests"/> gives: the
/// thread counts are the whole process's, and beside a busy neighbour the growth this looks for
/// reads as noise either way.
/// </summary>
[Collection(LoadCollection.Name)]
public sealed class LiveSlowPlayerTests(ITestOutputHelper output) : IAsyncLifetime
{
    private const string Token = "slow-player-test-token";

    private const string Name = "slow-player";

    /// <summary>
    /// How many slow players each step adds. Two steps, so the table answers the question in the
    /// shape it was asked - threads against the number of slow players - rather than at one point:
    /// a fault that costs a thread each doubles its growth when the second step lands, and one that
    /// does not stays flat.
    /// </summary>
    private const int PerStep = 5;

    private const int Steps = 2;

    /// <summary>
    /// What makes these players slow: each reads three tenths of what it is sent, so seven tenths of
    /// every player's stream piles up somewhere. The figure #11 names.
    /// </summary>
    private const double ReadRate = 0.3;

    /// <summary>
    /// What the sender pushes, and it is sized rather than inherited. The transport absorbs a slow
    /// player until the buffers at both ends are full, which is a number of bytes and not a number of
    /// seconds: about a dozen megabytes each with libsrt's defaults, which one player's surplus
    /// reaches in ten to twenty seconds at this bitrate and would take several minutes to reach at
    /// the 600 kbit/s the rest of the suite sends. A test at the lower rate would pass without the
    /// transport ever having been asked the question.
    /// </summary>
    private const string Picture = "testsrc2=size=1280x720:rate=25";

    private const string Bitrate = "12M";

    /// <summary>
    /// What that bitrate is worth in kilobytes a second, because a reader's pace has to be set in
    /// bytes and a transport-stream's is not quite its video bitrate. Measured rather than derived:
    /// the pattern above arrives at 12.4 Mbit/s, which is about 1500 KB/s. A figure a little out
    /// makes the reader slightly slower or faster than three tenths and changes nothing that is
    /// being asked.
    /// </summary>
    private const double Kilobytes = 1500;

    /// <summary>
    /// How long the pattern runs. It has to outlast the whole run: <c>-re</c> stops pacing at the end
    /// of a file, and a sender that reaches it stops being a sender.
    /// </summary>
    private const int PatternSeconds = 200;

    /// <summary>
    /// How long each step is watched for. Long enough that a player's surplus has filled the buffers
    /// at both ends several times over - twenty seconds at this bitrate - so that what is measured
    /// afterwards is the transport's answer rather than its patience.
    /// </summary>
    private static readonly TimeSpan Stalled = TimeSpan.FromSeconds(60);

    /// <summary>
    /// How often the process is asked what it is holding. The answer that matters is the peak rather
    /// than the last reading: a send that blocks and is then broken by the transport holds its thread
    /// only until the break, and a single reading at the end of the window can miss it entirely.
    /// </summary>
    private static readonly TimeSpan Sample = TimeSpan.FromSeconds(5);

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-slow-player-tests",
        Guid.NewGuid().ToString("N"));

    private readonly List<Process> _processes = [];

    private readonly Said _said = new();

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private int _ingestPort;
    private string _pattern = null!;

    private int ConsumptionPort => _ingestPort + 1;

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_root);

        _ingestPort = SrtSenders.FreePort();
        _pattern = Path.Combine(_root, "pattern.ts");

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Storage:Provider", "FileSystem");
            builder.UseSetting("Storage:FileSystem:RootPath", Path.Combine(_root, "files"));
            builder.UseSetting("Database:Provider", "LiteDb");
            builder.UseSetting("Database:LiteDb:Path", Path.Combine(_root, "db", "app.db"));
            builder.UseSetting("StorageMonitor:Enabled", "false");
            builder.UseSetting("Live:Enabled", "true");
            builder.UseSetting("Live:Token", Token);
            builder.UseSetting("Live:IngestPort", _ingestPort.ToString(CultureInfo.InvariantCulture));
            builder.UseSetting(
                "Live:ConsumptionPort",
                ConsumptionPort.ToString(CultureInfo.InvariantCulture));
            builder.UseSetting("Live:SourceFile", Path.Combine(_root, "sources.json"));
            builder.UseEnvironment("Production");

            // The transport's own account of why a send failed, which is the one thing here that
            // cannot be inferred from the outside: a viewer that left, a peer the transport gave up
            // on and a buffer that filled all look identical in a viewer count. It arrives as the
            // message of the IOException the muxing loop logs, and is worth a provider of its own
            // because the alternative is reading a verdict off a thread count.
            builder.ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddProvider(_said);
                logging.SetMinimumLevel(LogLevel.Debug);
            });
        });

        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var process in _processes)
        {
            SrtSenders.Kill(process);
        }

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

    [Fact]
    public async Task Slow_players_on_the_consumption_port_do_not_each_hold_a_thread()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);
        Assert.SkipUnless(Vitals.Available, "The thread and memory figures here are read from /proc.");

        SrtSenders.Render(_pattern, PatternSeconds, picture: Picture, bitrate: Bitrate);

        // Started before anything attaches, because a counter is an event: a listener that starts
        // afterwards sees nothing of what it already counted.
        using var meters = new Meters(_factory.Services.GetRequiredService<LiveMetrics>());

        _processes.Add(SrtSenders.StartSender(_ingestPort, $"#!::r={Name},m=publish", file: _pattern));

        await LiveReplicas.Until(
            async () => await Carrying() is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(60),
            "the stream never went live");

        // A player that takes its bytes as fast as they are handed over, held for the whole run. It
        // is the control: every figure below is about what the slow players cost on top of an
        // ordinary one, and a replica that had stopped serving everybody would otherwise read as a
        // replica that survived.
        var healthy = SrtSenders.StartCopyPlayers(ConsumptionPort, [Name]);

        _processes.Add(healthy);

        await LiveReplicas.Until(
            async () => await Carrying() is { Viewers: >= 1 },
            TimeSpan.FromSeconds(30),
            "the healthy player never attached");

        // Read with one player already being served, so the baseline carries what a direct viewer
        // costs when it is behaving and the growth below is only what being slow adds.
        var before = Vitals.Read(_processes);
        var fed = (await Carrying())?.Packets ?? 0;
        var slow = new List<Process>();
        var peak = before;

        output.WriteLine(
            $"{"at",-8}{"players",-9}{"viewers",-9}{"pool",-6}{"threads",-9}{"rss",-8}"
            + $"{"service",-9}{"rig",-7}{"skips",-7}{"udp",-6}alive");

        for (var step = 1; step <= Steps; step++)
        {
            for (var index = 0; index < PerStep; index++)
            {
                slow.Add(SrtSenders.StartCopyPlayers(ConsumptionPort, [Name], ReadRate));
            }

            _processes.AddRange(slow.TakeLast(PerStep));

            var attached = step * PerStep + 1;

            await LiveReplicas.Until(
                async () => await Carrying() is { } stream && stream.Viewers >= attached,
                TimeSpan.FromSeconds(30),
                $"the replica never served all {attached} players");

            var started = DateTime.UtcNow;

            while (DateTime.UtcNow - started < Stalled)
            {
                await Task.Delay(Sample);

                var now = Vitals.Read(_processes);
                var since = now.Since(before, DateTime.UtcNow - started);
                var stream = await Carrying();
                var alive = slow.Count(player => !player.HasExited);

                // The peak, not the last reading. See Sample.
                if (now.PoolThreads > peak.PoolThreads)
                {
                    peak = now;
                }

                output.WriteLine(
                    $"{(DateTime.UtcNow - started).TotalSeconds,-8:0}{slow.Count,-9}"
                    + $"{stream?.Viewers ?? 0,-9}{now.PoolThreads,-6}{now.Threads,-9}"
                    + $"{now.ResidentBytes / (1024 * 1024),-8}{since.Cores,-9:0.00}{since.Rig,-7:0.00}"
                    + $"{Skips(meters),-7:0}{since.KernelUdpErrors,-6}{alive}");
            }
        }

        var after = Vitals.Read(_processes);
        var final = await Carrying();

        output.WriteLine($"before: {Describe(before)}");
        output.WriteLine($"peak:   {Describe(peak)}");
        output.WriteLine($"after:  {Describe(after)}");
        output.WriteLine($"busiest threads: {after.Since(before, Stalled * Steps).Busiest(6)}");
        output.WriteLine($"stream: {final?.State} {final?.Packets} packets, {final?.Viewers} viewers, "
            + $"lost {final?.PacketsLost}, dropped {final?.PacketsDropped}, "
            + $"retransmitted {final?.Link?.PacketsRetransmitted}, "
            + $"receiving {final?.Link?.ReceiveRateMbps:0.0} Mbit/s");
        output.WriteLine($"skip-to-live: {Skips(meters):0}");
        output.WriteLine($"the transport said: {_said.About("viewer")}");
        output.WriteLine($"the players said: {SrtSenders.Complaints(slow)}");

        // Half the slow players, and the bound is the same shape as the relayed route's: a pool grows
        // on queued work whether or not anything is blocked, so a figure of zero would be measuring
        // scheduling luck. What it excludes is one thread per slow player, which is the fault this
        // exists to catch and what a send that waits without bound looks like from outside.
        var allowed = Steps * PerStep / 2;

        Assert.True(
            peak.PoolThreads - before.PoolThreads <= allowed,
            $"the pool grew {peak.PoolThreads - before.PoolThreads} workers for {Steps * PerStep} "
            + $"slow players ({Describe(before)} then {Describe(peak)})");

        Assert.True(
            peak.Threads - before.Threads <= allowed,
            $"the process grew {peak.Threads - before.Threads} threads for {Steps * PerStep} slow "
            + $"players ({Describe(before)} then {Describe(peak)})");

        // The other half of the same question, and the half a thread count cannot answer. A replica
        // that coped by abandoning the stream, or by starving the viewer that was behaving, has not
        // coped: ingest is still delivering and the healthy player is still connected and still
        // complaining about nothing.
        //
        // The packet count is the ingest side of that, and it is a comparison rather than a state:
        // a stream that stopped being fed stays live for its whole grace period, so "still live" on
        // its own would pass through the first half-minute of a replica that had stalled ingest.
        Assert.True(
            final is { State: LiveStreamState.Live } && final.Packets > fed,
            $"ingest did not keep feeding '{Name}' while the slow players were served: "
            + $"{final?.State} at {final?.Packets ?? 0} packets against {fed} before them");

        Assert.False(
            healthy.HasExited,
            $"the healthy player did not survive the slow ones: {SrtSenders.Complaints([healthy])}");

        // And the bound above is only worth something if the slow players were still being served
        // while it held. A replica that had dropped them all would pass every assertion so far
        // having been asked nothing: the threads it did not hold would be the threads it had no
        // viewers for. Half of them, because libsrt does eventually disconnect a player whose
        // receiver can no longer place what arrives, and one that goes late in the window has
        // already been served for most of it.
        Assert.True(
            final?.Viewers >= 1 + (Steps * PerStep / 2),
            $"only {final?.Viewers ?? 0} of {Steps * PerStep + 1} players were still being served, so the "
            + "thread counts above are not a measurement of serving slow players");
    }

    /// <summary>
    /// The same player, asked of libsrt rather than of the process table: what the transport does with
    /// the bytes a player will not take.
    ///
    /// A reader of this suite's own rather than an ffmpeg process, and the reason is that a
    /// connection's figures belong to the two sockets that hold it. The viewer's socket is the
    /// service's and nothing publishes what it says - <c>GET /api/live</c> carries
    /// <see cref="SrtLinkStats"/> for the publisher's socket alone, which is a different connection
    /// in the other direction - so the only end of a viewer's connection this suite can question is
    /// the viewer's own. What that end reports is enough: packets it never got, packets that arrived
    /// too late to play, and what was retransmitted trying.
    ///
    /// It reads through <see cref="SrtSocketStream"/>, the same class the service writes viewers
    /// through, so the payload rules and the timeout handling under this measurement are the ones in
    /// production rather than a second implementation of them.
    /// </summary>
    [Fact]
    public async Task What_a_slow_player_will_not_take_is_dropped_rather_than_waited_on()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        SrtSenders.Render(_pattern, PatternSeconds, picture: Picture, bitrate: Bitrate);

        using var meters = new Meters(_factory.Services.GetRequiredService<LiveMetrics>());

        _processes.Add(SrtSenders.StartSender(_ingestPort, $"#!::r={Name},m=publish", file: _pattern));

        await LiveReplicas.Until(
            async () => await Carrying() is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(60),
            "the stream never went live");

        using var sipping = Sipping.Open(ConsumptionPort, Name, ReadRate * Kilobytes);

        await LiveReplicas.Until(
            async () => await Carrying() is { Viewers: >= 1 },
            TimeSpan.FromSeconds(30),
            "the replica never served the slow reader");

        var lost = 0;
        var dropped = 0;
        var retransmitted = 0;
        var held = TimeSpan.Zero;

        output.WriteLine($"{"at",-6}{"took",-9}{"arriving",-11}{"lost",-9}{"dropped",-9}{"resent",-8}rtt");

        var started = DateTime.UtcNow;

        while (DateTime.UtcNow - started < Stalled)
        {
            await Task.Delay(Sample);

            // Cleared on every read, the way the heartbeat reads a publisher's, so each row is the
            // interval rather than a running total: what is happening now, not what happened once.
            if (sipping.Health() is not { } health)
            {
                // The socket has gone, which is the end of the measurement and is itself part of
                // what was measured. libsrt breaks a connection whose receiver can no longer place
                // what is arriving - "SEQUENCE DISCREPANCY. BREAKING CONNECTION ... Reception no
                // longer possible", on its own log, from the receiving end - so a reader this slow
                // is eventually disconnected by its own library rather than by anything here.
                break;
            }

            held = DateTime.UtcNow - started;
            lost += health.Lost;
            dropped += health.Dropped;
            retransmitted += health.Link.PacketsRetransmitted;

            output.WriteLine(
                $"{held.TotalSeconds,-6:0}{sipping.Bytes / 1024 / 1024,-9}"
                + $"{health.Link.ReceiveRateMbps,-11:0.00}{health.Lost,-9}{health.Dropped,-9}"
                + $"{health.Link.PacketsRetransmitted,-8}{health.Link.RoundTripTimeMs:0.0}");
        }

        output.WriteLine(
            $"the reader took {sipping.Bytes / 1024 / 1024} MB of a {Kilobytes * 8 / 1000:0.#} Mbit/s "
            + $"stream at {ReadRate:0.##} of it; lost {lost}, dropped {dropped}, resent "
            + $"{retransmitted}, and its connection {(sipping.Faulted ? $"broke after {held.TotalSeconds:0} s" : "held")}");
        output.WriteLine($"skip-to-live: {Skips(meters):0}");
        output.WriteLine($"the transport said: {_said.About("viewer")}");

        // The finding, and the one worth a regression: a player that cannot take the stream loses
        // picture rather than making the replica wait for it. Either counter satisfies it, because
        // the two are one event seen from two ends of the same second - media that never arrived
        // reads as loss, and media that arrived too late to play reads as a drop, and which of them
        // a discarded packet becomes depends on whether the receiver asked for it back first.
        //
        // It fails if that stops being true, which is the point of writing it down. A transport that
        // waited instead would hand this reader a clean stream at three tenths of the rate and
        // nothing else, and what each of those waits costs in threads is the test above.
        Assert.True(
            lost + dropped > 0,
            $"libsrt delivered {sipping.Bytes / 1024 / 1024} MB to a reader taking {ReadRate:0.##} of "
            + "the stream without losing or dropping a packet, which means it waited for it instead");

        // Enough that the figures above are a slow player's rather than a handshake's: a reader that
        // was refused, or served for a second and dropped, would report no loss for a reason that
        // has nothing to do with what this measures.
        Assert.True(
            sipping.Bytes > 4 * 1024 * 1024,
            $"the reader was given only {sipping.Bytes} bytes, so nothing above is about a player "
            + "that was being served and could not keep up");
    }

    private static double Skips(Meters meters) => meters
        .Of("live.overflows")
        .Where(overflow => overflow.Tags.Contains(
            new KeyValuePair<string, object?>("policy", "skip-to-live")))
        .Sum(overflow => overflow.Value);

    private static string Describe(Vitals vitals)
        => $"{vitals.PoolThreads} pool workers, {vitals.Threads} process threads, "
            + $"{vitals.ResidentBytes / (1024 * 1024)} MB resident";

    private async Task<LiveStream?> Carrying()
    {
        try
        {
            var status = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");

            return status?.Streams.FirstOrDefault(stream => stream.Name == Name);
        }
        catch (Exception)
        {
            // An unanswered listing is a measurement of a busy process, not a reason to stop.
            return null;
        }
    }

    /// <summary>
    /// A player on the consumption port that takes a fixed share of the stream and no more, reading
    /// through the same <see cref="SrtSocketStream"/> the service writes viewers through.
    ///
    /// A thread of its own, because an SRT read blocks and there is no asynchronous read to have
    /// instead - the same reason every other blocking libsrt loop in this codebase owns its thread.
    /// One thread is also why this belongs in a test of its own rather than beside the thread counts:
    /// a reader inside the process under measurement is a reader that shows up in the measurement.
    /// </summary>
    private sealed unsafe class Sipping : IDisposable
    {
        private readonly SrtSocketStream _player;

        private readonly CancellationTokenSource _stopping = new();

        private readonly Thread _pump;

        private long _bytes;

        private Sipping(SrtSocketStream player, double kilobytesASecond)
        {
            _player = player;

            _pump = new Thread(() => Read(kilobytesASecond))
            {
                IsBackground = true,
                Name = "slow player",
            };

            _pump.Start();
        }

        /// <summary>What this player has actually been given, which is the point of the exercise.</summary>
        public long Bytes => Interlocked.Read(ref _bytes);

        public bool Faulted => _player.Faulted;

        public static Sipping Open(int port, string name, double kilobytesASecond)
        {
            Srt.EnsureStarted();

            var socket = Srt.srt_create_socket();

            Assert.True(
                socket != Srt.SRT_INVALID_SOCK,
                $"Could not create a player socket: {Srt.LastError()}");

            // A second, so a read looks up between them and a test being torn down does not wait out
            // the peer idle timeout instead. SrtListener sets the same figure for the same reason.
            Srt.SetInt32(socket, SRT_SOCKOPT.SRTO_RCVTIMEO, 1000);
            Srt.SetString(socket, SRT_SOCKOPT.SRTO_STREAMID, $"#!::r={name},m=request");

            var address = new IPEndPoint(IPAddress.Loopback, port).Serialize();

            fixed (byte* raw = address.Buffer.Span)
            {
                Assert.True(
                    Srt.srt_connect(socket, raw, address.Size) == 0,
                    $"Could not reach the consumption port on {port}: {Srt.LastError()}");
            }

            return new Sipping(new SrtSocketStream(socket, writable: false), kilobytesASecond);
        }

        /// <inheritdoc cref="SrtSocketStream.Health"/>
        public (int Lost, int Dropped, SrtLinkStats Link)? Health() => _player.Health();

        public void Dispose()
        {
            _stopping.Cancel();

            // Closed before the thread is waited for, because closing is what unblocks a read that
            // is already inside libsrt: the loop below only looks at the token between reads.
            _player.Dispose();
            _pump.Join(TimeSpan.FromSeconds(5));
            _stopping.Dispose();
        }

        private void Read(double kilobytesASecond)
        {
            // Exactly the payload size, which is the smallest buffer an SRT read may be given.
            var buffer = new byte[_player.PayloadSize];
            var started = DateTime.UtcNow;

            while (!_stopping.IsCancellationRequested)
            {
                var read = _player.Read(buffer);

                if (read <= 0)
                {
                    // The stream ended or the socket did. Faulted says which, and the test asserts
                    // on it rather than this loop deciding anything.
                    return;
                }

                var total = Interlocked.Add(ref _bytes, read);

                // Paced against the running total rather than a fixed pause per read, so the rate is
                // the rate asked for however large a payload the handshake settled on.
                var wait = started + TimeSpan.FromSeconds(total / (kilobytesASecond * 1024)) - DateTime.UtcNow;

                if (wait > TimeSpan.Zero)
                {
                    Thread.Sleep(wait);
                }
            }
        }
    }

    /// <summary>
    /// What the streaming code said, kept rather than printed, so a run can quote the transport's own
    /// words for a failed send instead of inferring them from a viewer count.
    ///
    /// Bounded, and it has to be: the host runs at debug level for this one line, and a stream at
    /// this bitrate would otherwise leave a run's worth of packet-level chatter in memory.
    /// </summary>
    private sealed class Said : ILoggerProvider
    {
        private readonly ConcurrentQueue<string> _lines = new();

        public ILogger CreateLogger(string category) => new Line(this, category);

        /// <summary>Everything said that mentions this word, newest first, at most a handful.</summary>
        public string About(string word)
        {
            var said = _lines
                .Where(line => line.Contains(word, StringComparison.OrdinalIgnoreCase))
                .Reverse()
                .Take(3)
                .ToArray();

            return said.Length == 0 ? "nothing" : string.Join(" | ", said);
        }

        public void Dispose()
        {
        }

        private void Add(string line)
        {
            _lines.Enqueue(line);

            while (_lines.Count > 200)
            {
                _lines.TryDequeue(out _);
            }
        }

        private sealed class Line(Said said, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
                => null;

            public bool IsEnabled(LogLevel level)
                => category.StartsWith("StorageDemo.Infrastructure.Streaming", StringComparison.Ordinal);

            public void Log<TState>(
                LogLevel level,
                EventId id,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (IsEnabled(level))
                {
                    said.Add(exception is null
                        ? formatter(state, exception)
                        : $"{formatter(state, exception)}: {exception.Message}");
                }
            }
        }
    }
}
