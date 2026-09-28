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
/// Measured here, and the send does not wait - on one condition, which is the viewer's and not this
/// service's. libsrt discards from a socket's send buffer only for a peer that advertised too-late-
/// packet drop in its handshake; that is libsrt's default, so it is what an ordinary player does, and
/// <c>SrtSendPressureTests</c> measures both what it buys and what its absence costs. Everything
/// below therefore describes ordinary players, which is the case #11 asked about.
///
/// The figure this turns on is the skip count, not the thread count. A direct viewer is written to
/// inline, so a send that waits stalls that viewer's drain, and four seconds of stalling fills its
/// subscription and scores a skip-to-live. Ten players taking three tenths of the stream skipped
/// nothing across two minutes, which bounds every single write below about four seconds without
/// depending on scheduling noise at all. The thread counts agree and say less: fewer than half a
/// thread each, which excludes the one-per-viewer shape #4 measured on the relayed route and does not
/// establish zero - attaching ten players costs about three workers and three threads by itself.
///
/// What the player gets instead of a stall is a stream with holes, and the second test is libsrt's
/// own account of that from the receiving end: a reader taking three tenths is handed 1.9 Mbit/s of a
/// 12.29 Mbit/s stream and loses about 980 packets a second once its own buffer is full, because it
/// has nowhere to put what arrives. A reader pacing strictly by bytes is then disconnected by
/// its own library, which says so plainly - "SEQUENCE DISCREPANCY ... Reception no longer possible" -
/// while the ffmpeg players, which pace by timestamps, stay connected and simply lose picture.
///
/// What it costs this replica in memory is deliberately not claimed here. Resident memory climbs
/// while slow players are served and the table prints it, but ten slow players are also ten muxers,
/// ten subscriptions and ten four-second queues, and nothing in this test separates those from the
/// send buffer. The send buffer's own steady state is measured in <c>SrtSendPressureTests</c>, on the
/// socket rather than off the process, and the rig's LIVE_SCALE_SLOW_DIRECT exists to find the slope
/// at a hundred of them.
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
    /// What the sender pushes, and it is sized rather than inherited. Nothing happens to a slow player
    /// until its own receive buffer is full, and that is a number of bytes rather than of seconds:
    /// twelve megabytes with libsrt's defaults, which this bitrate reaches in about ten seconds and
    /// the 600 kbit/s the rest of the suite sends would take a quarter of an hour to reach. A test at
    /// the lower rate would pass without the transport ever having been asked the question, which is
    /// the trap this figure exists to avoid.
    /// </summary>
    private const string Picture = "testsrc2=size=1280x720:rate=25";

    private const string Bitrate = "12M";

    /// <summary>
    /// What the stream is worth on the wire, and the one place a rate is written down: the reader
    /// below is paced against it and every figure printed is derived from it, because quoting a
    /// transport stream's rate twice is how a run ends up with two of them. Twelve megabits of video
    /// arrives as about this much transport stream, and each run prints what the replica actually
    /// received beside it so a drift is visible rather than assumed away.
    /// </summary>
    private const int BitsPerSecond = 12_288_000;

    /// <summary>
    /// How long the pattern runs. It has to outlast the whole run: <c>-re</c> stops pacing at the end
    /// of a file, and a sender that reaches it stops being a sender.
    /// </summary>
    private const int PatternSeconds = 120;

    /// <summary>
    /// How long each step is watched for. The buffers at both ends fill in about twenty seconds at
    /// this bitrate, so thirty is half again as long as the fill: what is measured after it is the
    /// transport's answer rather than its patience, and the two steps together still take two
    /// minutes of a shared machine rather than four.
    /// </summary>
    private static readonly TimeSpan Stalled = TimeSpan.FromSeconds(30);

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
        var pool = before.PoolThreads;
        var threads = before.Threads;

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

                // Both peaks, and separately: a process thread taken without a pool worker is the
                // shape this route had before #18, one dedicated thread per accepted viewer, and a
                // peak chosen on the pool figure alone would not see it.
                pool = Math.Max(pool, now.PoolThreads);
                threads = Math.Max(threads, now.Threads);

                output.WriteLine(
                    $"{(DateTime.UtcNow - started).TotalSeconds,-8:0}{slow.Count,-9}"
                    + $"{stream?.Viewers ?? 0,-9}{now.PoolThreads,-6}{now.Threads,-9}"
                    + $"{now.ResidentBytes / (1024 * 1024),-8}{since.Cores,-9:0.00}{since.Rig,-7:0.00}"
                    + $"{Skips(meters),-7:0}{since.KernelUdpErrors,-6}{alive}");
            }
        }

        var after = Vitals.Read(_processes);
        var final = await Carrying();

        var skips = Skips(meters);

        output.WriteLine($"skip-to-live: {skips:0}  <- the figure this test turns on");
        output.WriteLine($"before: {Describe(before)}");
        output.WriteLine($"peak:   {pool} pool workers, {threads} process threads");
        output.WriteLine($"after:  {Describe(after)}");
        output.WriteLine($"busiest threads: {after.Since(before, Stalled * Steps).Busiest(6)}");
        output.WriteLine($"stream: {final?.State} {final?.Packets} packets, {final?.Viewers} viewers, "
            + $"lost {final?.PacketsLost}, dropped {final?.PacketsDropped}, "
            + $"retransmitted {final?.Link?.PacketsRetransmitted}, "
            + $"receiving {final?.Link?.ReceiveRateMbps:0.00} Mbit/s against the "
            + $"{BitsPerSecond / 1_000_000d:0.00} the reader is paced against");
        output.WriteLine($"the transport said: {_said.About("viewer")}");
        output.WriteLine($"the players said: {SrtSenders.Complaints(slow)}");

        // The sharpest figure here, and the one that does not depend on thread-count noise at all. A
        // direct viewer is written to inline, so a send that waits stops this viewer's drain, and four
        // seconds of it fills the subscription and scores a skip. Zero skips therefore bounds every
        // single write below about four seconds - which is #20's premise, answered without counting a
        // thread at all.
        //
        // A small allowance rather than exactly zero: the queue is four seconds of the sender's
        // declared rate, and a collection pause or a genuinely late keyframe on a loaded machine can
        // cost one. What it excludes is a route where waiting is ordinary, which is what tens of skips
        // would be.
        Assert.True(
            skips <= 2,
            $"{skips:0} viewers were skipped to live, so a write to a direct player waited about four "
            + "seconds or more, which is what #20's send timeout exists for");

        // Half the slow players, and read as what it is: this excludes one thread per slow player, the
        // shape #4 measured on the relayed route, and it does not establish none. A pool grows on
        // queued work whether or not anything is blocked, and attaching ten players costs about three
        // workers and three threads by itself, so of the five threads of headroom this bound has the
        // attach transient spends three. What it can tell apart is "fewer than about half a thread
        // each" from "one each"; it cannot tell either of those from zero.
        var allowed = Steps * PerStep / 2;

        Assert.True(
            pool - before.PoolThreads <= allowed,
            $"the pool grew {pool - before.PoolThreads} workers for {Steps * PerStep} slow players "
            + $"(from {Describe(before)})");

        Assert.True(
            threads - before.Threads <= allowed,
            $"the process grew {threads - before.Threads} threads for {Steps * PerStep} slow players "
            + $"(from {Describe(before)})");

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

        using var sipping = Sipping.Open(ConsumptionPort, Name, ReadRate * BitsPerSecond / 8);

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

        var seconds = Math.Max(held.TotalSeconds, 1);

        output.WriteLine(
            $"the reader took {sipping.Bytes / 1024 / 1024} MB of a {BitsPerSecond / 1_000_000d:0.00} "
            + $"Mbit/s stream at {ReadRate:0.##} of it; lost {lost} and dropped {dropped} packets in "
            + $"{seconds:0} s, which is {(lost + dropped) / seconds:0} a second, resent "
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
            (lost + dropped) / seconds > 50,
            $"libsrt lost or dropped {(lost + dropped) / seconds:0} packets a second for a reader "
            + $"taking {ReadRate:0.##} of the stream, having delivered "
            + $"{sipping.Bytes / 1024 / 1024} MB. A rate this low is a link having a bad moment; what "
            + "this measures is a transport discarding most of a stream, which ran at about a "
            + "thousand a second, and its absence would mean the sender waited instead");

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

        private Sipping(SrtSocketStream player, double bytesASecond)
        {
            _player = player;

            _pump = new Thread(() => Read(bytesASecond))
            {
                IsBackground = true,
                Name = "slow player",
            };

            _pump.Start();
        }

        /// <summary>What this player has actually been given, which is the point of the exercise.</summary>
        public long Bytes => Interlocked.Read(ref _bytes);

        public bool Faulted => _player.Faulted;

        public static Sipping Open(int port, string name, double bytesASecond)
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

            return new Sipping(new SrtSocketStream(socket, writable: false), bytesASecond);
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

        private void Read(double bytesASecond)
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
                var wait = started + TimeSpan.FromSeconds(total / bytesASecond) - DateTime.UtcNow;

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
