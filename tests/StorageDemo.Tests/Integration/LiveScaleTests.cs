using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Api.Controllers;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// The rig, not a specification: ingest ramped to two hundred streams and then readers ramped to
/// five hundred on top of them, measuring at every step what each side delivered and what inside the
/// process was spending the machine.
///
/// It answers a question no assertion can, which is where this replica stops coping and what runs out
/// when it does. <see cref="LiveLoadTests"/> is the pass-or-fail companion: fifty streams, every
/// figure checked, run by every <c>dotnet test</c>. This one is skipped unless <c>LIVE_SCALE</c> is
/// set, because it takes minutes, saturates the machine, and its output is a table to read rather
/// than a green tick.
///
/// Three things make the numbers worth reading.
///
/// **The rig is not the subject.** A process per stream and a decoding player per viewer both cost
/// more than the service does, so at two hundred streams a naive rig measures itself: ten gigabytes
/// of resident ffmpeg and every core spent encoding. Senders here push ten streams each out of one
/// process, SRT readers take ten connections each and never decode, and the relayed readers are
/// in-process HTTP reads with a discard at the end. The load generator's own processor share is
/// reported in every row, because "the service is at its knee" and "the rig ran out of machine" look
/// identical in a delivered-bitrate column and nothing else tells them apart.
///
/// **The bottleneck is named, not inferred.** Every row carries the busiest threads by name out of
/// <c>/proc/self/task</c> (<see cref="Vitals"/>). The measured ceiling of this design is one libsrt
/// receive worker per bound port, which is a thread called <c>SRT:RcvQ:w</c> and cannot exceed one
/// core however many the machine has; a row where it sits at sixty percent is at the knee
/// <c>baseline.md</c> found, whatever the other columns say.
///
/// **Both reader routes are measured, because they are not the same cost.** A player on the
/// consumption port gets a socket and a dedicated thread; a relayed reader gets a thread-pool
/// continuation. Both get their own subscription and their own transport-stream muxer, which is the
/// per-viewer work that scales with the packet rate. Five hundred of each is a different shape of
/// failure and the table shows which.
///
/// Read a row the way <c>baseline.md</c> reads one: clean means the service is delivering what its
/// senders send, collapsed means it is not while still reporting every stream live. The verdict
/// column says which, and the kernel's UDP error counter is what makes it a fact rather than an
/// opinion.
/// </summary>
[Collection(LoadCollection.Name)]
public sealed class LiveScaleTests(ITestOutputHelper output) : IAsyncLifetime
{
    private const string Token = "scale-test-token";

    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";

    private const string NoFfmpegSrt = "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.";

    private const string NotAsked =
        "Set LIVE_SCALE=1 to run the scale rig. It takes minutes and saturates the machine.";

    /// <summary>The ingest ramp, in streams. Each step adds senders and measures again.</summary>
    private static readonly int[] StreamSteps = Steps("LIVE_SCALE_STREAMS", [50, 100, 150, 200]);

    /// <summary>The reader ramp, held against the last and largest stream step.</summary>
    private static readonly int[] ReaderSteps = Steps("LIVE_SCALE_READERS", [50, 150, 300, 500]);

    /// <summary>
    /// How many streams one sender process pushes. Ten by default: twenty processes for two hundred
    /// streams, which is what makes the rig affordable, at the cost of siblings sharing a fate.
    /// </summary>
    private static readonly int PerSender = Configured("LIVE_SCALE_PER_SENDER", 10);

    /// <summary>
    /// How many of the readers are real players on the consumption port rather than relayed HTTP
    /// reads, and so how much of the direct route is measured. They cost a process each ten, so this
    /// is the axis that is sampled rather than swept.
    /// </summary>
    private static readonly int Direct = Configured("LIVE_SCALE_DIRECT_READERS", 50);

    /// <summary>How long each measurement window is. Long enough to average a beat or seven.</summary>
    private static readonly int Window = Configured("LIVE_SCALE_WINDOW", 15);

    /// <summary>
    /// How long the pattern is, which has to outlast the whole run: <c>-re</c> stops pacing at the
    /// end of a file, so a sender that reaches it stops being a sender and the rig would be measuring
    /// its own arithmetic.
    /// </summary>
    private static readonly int PatternSeconds = Configured("LIVE_SCALE_PATTERN", 600);

    /// <summary>
    /// What the senders push. The default is the rig scripts' own pattern, so the rows here can be
    /// read beside the ones in <c>baseline.md</c>; a lower one is how the stream count is pushed
    /// further on a machine whose senders would otherwise be the limit.
    /// </summary>
    private static readonly string Picture =
        Environment.GetEnvironmentVariable("LIVE_SCALE_PICTURE") ?? "testsrc2=size=640x360:rate=25";

    /// <summary>
    /// What the encoder aims at, which with a large enough picture is how this rig asks its second
    /// question. Stream count and packet rate buy from the same budget on the receive worker, and a
    /// camera sends five times what the default pattern does: twenty camera-rate streams cost that
    /// thread more than a hundred of these.
    /// </summary>
    private static readonly string Bitrate =
        Environment.GetEnvironmentVariable("LIVE_SCALE_BITRATE") ?? "800k";

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-scale-tests",
        Guid.NewGuid().ToString("N"));

    private readonly List<Process> _processes = [];
    private readonly List<Relay> _relays = [];
    private readonly List<Row> _rows = [];

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private HttpClient _readers = null!;
    private int _ingestPort;
    private string _pattern = null!;
    private int _direct;
    private double _reference;

    private int ConsumptionPort => _ingestPort + 5;

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
            builder.UseSetting("Live:ConsumptionPort", ConsumptionPort.ToString(CultureInfo.InvariantCulture));

            // One ingest port, because the ceiling this rig is looking for is a single receive
            // worker's. Live:IngestPortCount is the answer to it, not part of the measurement.
            builder.UseSetting("Live:IngestPortCount", "1");
            builder.UseSetting("Live:MaxStreams", "0");
            builder.UseSetting("Live:FeedTimeoutSeconds", "5");
            builder.UseSetting("Live:GracePeriodSeconds", "10");
            builder.UseSetting("Live:PreviewIntervalSeconds", "2");
            builder.UseEnvironment("Production");
        });

        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        _readers = _factory.CreateClient();
        _readers.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        // A reader holds its response open for the whole run, and the default timeout covers the
        // body as well as the headers: without this every reader is torn down after a hundred
        // seconds and the rig quietly measures a reconnect storm of its own making.
        _readers.Timeout = Timeout.InfiniteTimeSpan;

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var relay in _relays)
        {
            await relay.DisposeAsync();
        }

        foreach (var process in _processes)
        {
            SrtSenders.Kill(process);
        }

        _client.Dispose();
        _readers.Dispose();
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
    public async Task Ingest_and_readers_are_ramped_until_something_gives()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("LIVE_SCALE") is not (null or ""), NotAsked);
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(FfmpegLibrary.InputProtocols().Contains("srt"), NoFfmpegSrt);

        using var meters = new Meters(_factory.Services.GetRequiredService<LiveMetrics>());

        SrtSenders.Render(_pattern, PatternSeconds, picture: Picture, bitrate: Bitrate);

        var source = SrtSenders.PayloadMbps(_pattern, PatternSeconds);

        // What a reader should see, which is not what a stream delivers. Ingest is counted in the
        // packets the demultiplexer publishes; a viewer is sent those packets muxed back into a
        // transport stream, headers and program tables included, so the two references differ by the
        // container's overhead and comparing a reader against the payload figure would flatter it.
        var wire = new FileInfo(_pattern).Length * 8 / (double)PatternSeconds / 1_000_000;

        Report($"pattern          {source:0.00} Mbit/s of payload per stream"
            + $" ({wire:0.00} Mbit/s on the wire), {Picture}");
        Report($"machine          {Environment.ProcessorCount} cores"
            + (Vitals.Available ? string.Empty : ", /proc not readable here so no thread breakdown"));
        Report($"rig              {PerSender} streams per sender process,"
            + $" {Direct} of the readers are real players on the consumption port");
        Report(string.Empty);
        Report(Row.Header);

        // ---- Ingest ------------------------------------------------------------------------
        var running = 0;

        foreach (var target in StreamSteps)
        {
            for (; running < target; running += PerSender)
            {
                var names = Enumerable
                    .Range(running, Math.Min(PerSender, target - running))
                    .Select(Name)
                    .ToArray();

                _processes.Add(SrtSenders.StartMultiSender(_ingestPort, names, _pattern));
            }

            running = target;

            // Bounded, and a step that never got everything on air is a finding rather than a
            // failure: the row says how many arrived and the run carries on to show what that
            // costs.
            var onAir = await Settle(
                async () => await Carrying() >= target,
                TimeSpan.FromSeconds(90),
                Carrying);

            Add(await Measure(meters, source, wire, target, onAir, readers: 0, attached: 0));
        }

        // ---- Readers, against the largest stream count -------------------------------------
        // The sweep is in relayed readers. The real players on the consumption port arrive once,
        // with the first step, and are held for every row after it: they cost a process each ten,
        // so the direct route is sampled rather than swept, and what it is there for is the
        // comparison - a socket and a dedicated thread against a thread-pool continuation, both
        // paying for their own subscription and their own muxer.
        var streams = StreamSteps[^1];

        // A relay sweep turned off while direct readers are asked for is still one step: it is how
        // the expensive route is measured on its own, since that is the one that costs a thread and a
        // socket per viewer rather than a continuation.
        var readerSteps = ReaderSteps.Length == 0 && Direct > 0 ? [0] : ReaderSteps;

        foreach (var target in readerSteps)
        {
            if (_direct == 0 && Direct > 0)
            {
                for (var index = 0; index < Direct; index += 10)
                {
                    var names = Enumerable
                        .Range(index, Math.Min(10, Direct - index))
                        .Select(offset => Name(offset % streams))
                        .ToArray();

                    _processes.Add(SrtSenders.StartCopyPlayers(ConsumptionPort, names));
                    _direct += names.Length;
                }
            }

            while (_relays.Count < target)
            {
                _relays.Add(Relay.Open(_readers, Name(_relays.Count % streams)));
            }

            var expected = target + _direct;

            var attached = await Settle(
                async () => await Viewers() >= expected,
                TimeSpan.FromSeconds(60),
                Viewers);

            Add(await Measure(meters, source, wire, streams, await Carrying(), expected, attached));
        }

        // ---- What broke, and where -------------------------------------------------------
        Report(string.Empty);

        foreach (var line in Verdict())
        {
            Report(line);
        }

        // The rig asserts almost nothing on purpose: its output is the table, and a row that reads
        // "collapsed" is the answer rather than a failure. What must hold is that the replica was
        // still answering afterwards - a service that fell over instead of degrading is a different
        // finding, and the one thing here that would be a defect.
        var final = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");

        Assert.NotNull(final);
        Assert.NotEmpty(_rows);
    }

    /// <summary>
    /// Keeps a row and prints it at once, rather than at the end. A ramp that is killed halfway - by
    /// a timeout, by the machine, by somebody watching it - has still measured everything up to the
    /// step it died on, and a table held back until the end would lose exactly that.
    /// </summary>
    private void Add(Row row)
    {
        _rows.Add(row);

        Report(row.ToString());
    }

    /// <summary>One measurement window: what arrived, what it cost, and what was busiest.</summary>
    private async Task<Row> Measure(
        Meters meters,
        double source,
        double wire,
        int streams,
        int onAir,
        int readers,
        int attached)
    {
        var before = Vitals.Read(_processes);
        var listedBefore = await Listing();
        var readBefore = _relays.Select(relay => relay.Bytes).ToArray();
        var beatsBefore = meters.Of("live.heartbeat.duration").Count;
        var lostBefore = meters.Total("live.packets.lost") + meters.Total("live.packets.dropped");
        var skipsBefore = meters.Count("live.overflows", "policy", "skip-to-live");

        var clock = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(Window));
        clock.Stop();

        var listedAfter = await Listing();
        var load = Vitals.Read(_processes).Since(before, clock.Elapsed);

        var delivered = listedAfter
            .Where(stream => listedBefore.ContainsKey(stream.Key))
            .Select(stream => Mbps(stream.Value.Bytes - listedBefore[stream.Key].Bytes, clock.Elapsed))
            .Order()
            .ToArray();

        var packets = listedAfter
            .Where(stream => listedBefore.ContainsKey(stream.Key))
            .Sum(stream => stream.Value.Packets - listedBefore[stream.Key].Packets);

        var served = _relays
            .Select((relay, index) => Mbps(relay.Bytes - readBefore[index], clock.Elapsed))
            .Order()
            .ToArray();

        var beats = meters
            .Of("live.heartbeat.duration")
            .Skip(beatsBefore)
            .Select(beat => beat.Value)
            .Order()
            .ToArray();

        var median = Median(delivered);

        // The first step is the calibration: whatever the senders manage when nothing is strained is
        // what a later step is compared against. The file's own rate is the other reference and both
        // are reported, because a rig pacing at ninety-odd percent of it - which multi-output senders
        // do - would otherwise read as a service losing seven percent of the media at every load.
        _reference = _reference > 0 ? _reference : median;

        return new Row(
            streams,
            onAir,
            readers,
            attached,
            _relays.Count,
            median,
            source,
            _reference,
            Median(served),
            wire,
            load,
            beats.Length == 0 ? 0 : beats[beats.Length / 2],
            beats.Length == 0 ? 0 : beats[^1],
            meters.Total("live.packets.lost") + meters.Total("live.packets.dropped") - lostBefore,
            packets,
            meters.Count("live.overflows", "policy", "skip-to-live") - skipsBefore,
            _relays.Count(relay => relay.Fault is not null));
    }

    /// <summary>
    /// Waits for a count to reach its target and reports what it actually reached, rather than
    /// failing. At the far end of a ramp the answer "only a hundred and eighty of two hundred ever
    /// came on air" is the measurement.
    /// </summary>
    private static async Task<int> Settle(
        Func<Task<bool>> reached,
        TimeSpan timeout,
        Func<Task<int>> count)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (await reached())
            {
                break;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        return await count();
    }

    /// <summary>Reads the table back and says what it found, in the order a reader would ask.</summary>
    private IEnumerable<string> Verdict()
    {
        var ingest = _rows.Where(row => row.Readers == 0).ToArray();
        var reading = _rows.Where(row => row.Readers > 0).ToArray();

        var cleanIngest = ingest.LastOrDefault(row => row.Clean);
        var firstBadIngest = ingest.FirstOrDefault(row => !row.Clean);

        yield return cleanIngest is null
            ? "ingest           not clean at any step measured"
            : $"ingest           clean to {cleanIngest.Streams} streams"
                + $" ({cleanIngest.Streams * cleanIngest.Delivered:0.#} Mbit/s delivered)"
                + (firstBadIngest is null
                    ? ", and never degraded"
                    : $"; {firstBadIngest.Streams} was {firstBadIngest.Verdict}"
                        + $" at {firstBadIngest.LossShare:P1} loss"
                        + $" and {firstBadIngest.Load.KernelUdpErrors ?? 0} kernel drops");

        var cleanReaders = reading.LastOrDefault(row => row.ReadersClean);
        var firstBadReaders = reading.FirstOrDefault(row => !row.ReadersClean);

        yield return cleanReaders is null
            ? "readers          no reader step kept up"
            : $"readers          {cleanReaders.Readers} kept up"
                + (cleanReaders.Relays == 0
                    ? " (direct players only, whose throughput is inside them rather than here)"
                    : $" ({cleanReaders.Relays * cleanReaders.Served:0.#} Mbit/s out over"
                        + $" {cleanReaders.Relays} relayed)")
                + (firstBadReaders is null
                    ? ", and none fell behind"
                    : $"; {firstBadReaders.Readers} fell behind at {firstBadReaders.Served:0.00}"
                        + $" of {firstBadReaders.Wire:0.00} Mbit/s each");

        var busiest = _rows[^1].Load;

        yield return $"busiest          {busiest.Busiest(6)} (summed per thread name)";
        yield return $"receive worker   {busiest.Share("SRT:RcvQ:w"):P0} of one core at the last step"
            + " (the ceiling is one core, and the measured knee about 60 %)";
        yield return $"cost             pod {busiest.Cores:0.0} cores, rig {busiest.Rig:0.0} cores,"
            + $" of {Environment.ProcessorCount}; {busiest.ResidentBytes / 1024.0 / 1024 / 1024:0.00} GiB"
            + $" resident over {busiest.Threads} threads";

        var errors = _rows.Sum(row => row.Load.KernelUdpErrors ?? 0);

        yield return $"kernel udp       {errors} receive errors across the run"
            + (errors == 0
                ? ", so nothing overflowed the shared receive buffer"
                : " - the collapse itself, wherever it appears above");
    }

    /// <param name="Delivered">Median per-stream ingest rate over the window.</param>
    /// <param name="Served">Median per-relayed-reader rate over the same window.</param>
    private sealed record Row(
        int Streams,
        int OnAir,
        int Readers,
        int Attached,
        int Relays,
        double Delivered,
        double Source,
        double Reference,
        double Served,
        double Wire,
        Vitals.Load Load,
        double Beat,
        double SlowestBeat,
        double Lost,
        long Packets,
        int Skips,
        int Faults)
    {
        public const string Header =
            "streams  air  readers  seen  Mbit/s in  of src  of rig  Mbit/s out  RcvQ:w  pod   rig"
            + "   RSS       thr      udp       lost  skip  verdict";

        /// <summary>
        /// The same reading <c>baseline.md</c> takes: clean is a service demultiplexing what was sent
        /// to it, collapsed is one that is not while still listing every stream as live.
        /// </summary>
        /// <summary>
        /// Clean is every stream on air, delivering what the same senders delivered at the first and
        /// smallest step, and doing it without the transport or the kernel losing anything. Against
        /// the file's nominal rate instead, the rig's own pacing would set the verdict; against the
        /// first step, scale does.
        ///
        /// Loss belongs in this and not only in a column of its own, because a stream can deliver its
        /// whole bitrate and still be broken: retransmits fill the gap, and a step that arrives at a
        /// hundred and eight percent of the rate before it is a receiver catching up rather than one
        /// coping. That row is the knee, and reading the rate alone would call it clean.
        /// </summary>
        public bool Clean => OnAir >= Streams
            && Delivered >= Reference * 0.9
            && LossShare < 0.001
            && (Load.KernelUdpErrors ?? 0) == 0;

        /// <summary>What the transport lost, against what actually arrived in the same window.</summary>
        public double LossShare => Packets + Lost <= 0 ? 0 : Lost / (Packets + Lost);

        public bool ReadersClean => Readers == 0
            || (Attached >= Readers && (Relays == 0 || Served >= Wire * 0.9));

        public string Verdict => Delivered < Reference * 0.5
            ? "collapsed"
            : Delivered < Reference * 0.9
                ? "marginal"
                : Clean ? "clean" : "lossy";

        public override string ToString()
            => string.Join(
                "  ",
                Streams.ToString(CultureInfo.InvariantCulture).PadLeft(7),
                OnAir.ToString(CultureInfo.InvariantCulture).PadLeft(3),
                Readers.ToString(CultureInfo.InvariantCulture).PadLeft(7),
                Attached.ToString(CultureInfo.InvariantCulture).PadLeft(4),
                Delivered.ToString("0.00", CultureInfo.InvariantCulture).PadLeft(9),
                (Source <= 0 ? 0 : Delivered / Source).ToString("P0", CultureInfo.InvariantCulture).PadLeft(6),
                (Reference <= 0 ? 0 : Delivered / Reference).ToString("P0", CultureInfo.InvariantCulture).PadLeft(6),
                (Relays == 0 ? "-" : Served.ToString("0.00", CultureInfo.InvariantCulture)).PadLeft(10),
                Load.Share("SRT:RcvQ:w").ToString("P0", CultureInfo.InvariantCulture).PadLeft(6),
                Load.Cores.ToString("0.0", CultureInfo.InvariantCulture).PadLeft(4),
                Load.Rig.ToString("0.0", CultureInfo.InvariantCulture).PadLeft(4),
                $"{Load.ResidentBytes / 1024.0 / 1024 / 1024:0.00} GiB".PadLeft(9),
                Load.Threads.ToString(CultureInfo.InvariantCulture).PadLeft(4),
                (Load.KernelUdpErrors?.ToString(CultureInfo.InvariantCulture) ?? "-").PadLeft(7),
                (Lost > 0 ? $"{Lost:0} ({LossShare:P1})" : "0").PadLeft(9),
                Skips.ToString(CultureInfo.InvariantCulture).PadLeft(4),
                Faults > 0 ? $"{Verdict}, {Faults} reader faults" : Verdict);
    }

    /// <summary>
    /// One relayed reader: the peer view route, read and discarded, which is what a viewer costs this
    /// replica with nothing on the rig's side but a copy into a buffer nobody looks at.
    ///
    /// It is the in-cluster hop rather than a player's own connection, so it measures the fan-out and
    /// the per-viewer muxer without a process or a decoder per reader. The two paths meet inside
    /// <c>LiveStreamCoordinator.Serve</c>, which is where a viewer is actually served; what this does
    /// not measure is the socket underneath a direct player, which is why some of the readers here
    /// are real ones.
    /// </summary>
    private sealed class Relay : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stopping = new();

        private long _bytes;

        private Relay(HttpClient client, string name)
            => Pump = Task.Run(() => ReadAsync(client, name, _stopping.Token));

        public Task Pump { get; }

        public Exception? Fault { get; private set; }

        public long Bytes => Interlocked.Read(ref _bytes);

        public static Relay Open(HttpClient client, string name) => new(client, name);

        public async ValueTask DisposeAsync()
        {
            await _stopping.CancelAsync();

            try
            {
                await Pump;
            }
            catch (Exception)
            {
                // A reader torn down mid-read is how every one of these ends.
            }

            _stopping.Dispose();
        }

        private async Task ReadAsync(HttpClient client, string name, CancellationToken stopping)
        {
            try
            {
                using var response = await client.GetAsync(
                    $"api/live/peer/view/{name}",
                    HttpCompletionOption.ResponseHeadersRead,
                    stopping);

                response.EnsureSuccessStatusCode();

                await using var body = await response.Content.ReadAsStreamAsync(stopping);

                var buffer = new byte[64 * 1024];

                int read;

                while ((read = await body.ReadAsync(buffer, stopping)) > 0)
                {
                    Interlocked.Add(ref _bytes, read);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                // Kept rather than thrown: a reader that failed is a column in the table, and a rig
                // that dies because one of five hundred readers did has measured nothing.
                Fault = ex;
            }
        }
    }

    private static string Name(int index) => $"scale/{index:0000}";

    private static int[] Steps(string variable, int[] fallback)
    {
        var configured = Environment.GetEnvironmentVariable(variable);

        if (string.IsNullOrWhiteSpace(configured))
        {
            return fallback;
        }

        var steps = configured
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(step => int.TryParse(step, CultureInfo.InvariantCulture, out var value) ? value : -1)
            .Where(step => step >= 0)
            .Order()
            .ToArray();

        // A single zero is how a phase is turned off, so one question can be asked at a time: a
        // bitrate sweep wants no readers, and a reader sweep wants the streams it already has.
        return steps is [0] ? [] : steps.Where(step => step > 0).DefaultIfEmpty(fallback[0]).ToArray();
    }

    private static int Configured(string variable, int fallback)
        => int.TryParse(
            Environment.GetEnvironmentVariable(variable),
            CultureInfo.InvariantCulture,
            out var configured) && configured > 0
                ? configured
                : fallback;

    private static double Mbps(long bytes, TimeSpan over)
        => over.TotalSeconds <= 0 ? 0 : bytes * 8 / over.TotalSeconds / 1_000_000;

    private static double Median(double[] sorted)
        => sorted.Length == 0 ? 0 : sorted[sorted.Length / 2];

    private async Task<Dictionary<string, LiveStream>> Listing()
    {
        try
        {
            var status = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");

            return status?.Streams.ToDictionary(stream => stream.Name) ?? [];
        }
        catch (Exception)
        {
            // At the far end of a ramp the API is competing with everything else in the process for
            // the pool. An unanswered listing is a measurement of that, not a reason to stop.
            return [];
        }
    }

    private async Task<int> Carrying()
        => (await Listing()).Values.Count(stream => stream is { State: LiveStreamState.Live, Packets: > 0 });

    private async Task<int> Viewers()
        => (await Listing()).Values.Sum(stream => stream.Viewers);

    private void Report(string line) => output.WriteLine(line);
}
