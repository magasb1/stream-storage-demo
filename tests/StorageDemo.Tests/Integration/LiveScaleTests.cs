using System.Diagnostics;
using System.Globalization;
using System.Net;
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
    private static readonly int Direct = Optional("LIVE_SCALE_DIRECT_READERS", 50);

    /// <summary>
    /// Whether to run the second half: previews audited, and a snapshot and a recording of every
    /// stream at once. On by default, because leaving it off is how the first version of this rig
    /// came to report a replica coasting.
    /// </summary>
    private static readonly bool Work =
        Environment.GetEnvironmentVariable("LIVE_SCALE_WORK") is not "0";

    /// <summary>
    /// How many of the relayed readers drain slowly, and how long each of their reads pauses for.
    ///
    /// The realistic failure, and the one worth a rig of its own: a viewer on a bad network reads
    /// slower than the stream arrives. The service writes to a viewer synchronously - libav's muxer
    /// has no asynchronous write callback, which is why the relay route opts back into synchronous IO
    /// - so a consumer that stops draining does not merely fall behind, it holds the thread that was
    /// writing to it. Whether that is bounded by the viewer's own queue or unbounded in the thread
    /// pool is the question, and one slow client per pod is not a hypothetical.
    /// </summary>
    private static readonly int SlowReaders = Optional("LIVE_SCALE_SLOW_READERS", 0);

    /// <summary>
    /// How many of the direct players read slower than the stream arrives, and how fast they read.
    ///
    /// The same failure on the other route, and it costs something else entirely: a direct player's
    /// writes block inline in libsrt rather than being handed to a queue, so the question here was
    /// threads and the answer measured in <see cref="LiveSlowPlayerTests"/> is that an ordinary
    /// player costs none of them - libsrt discards from the send buffer rather than making the send
    /// wait. What ten of them cost in memory could not be separated from ten muxers and ten queues at
    /// that scale, which is what this exists for: a hundred of them against a send buffer that holds
    /// about a second and a half of stream each, and a resident figure with a slope worth reading.
    ///
    /// Rounded up to whole processes, because the players come ten to a process and one process reads
    /// at one rate.
    /// </summary>
    private static readonly int SlowDirect = Optional("LIVE_SCALE_SLOW_DIRECT", 0);

    /// <summary>
    /// What those players read, as a percentage of real time: 30 is three tenths, which is #11's
    /// figure and the one the focused test measured at, so a rig row can be read beside it. A
    /// percentage rather than a fraction because the environment is read as an integer.
    /// </summary>
    private static readonly double SlowDirectRate = Configured("LIVE_SCALE_SLOW_DIRECT_RATE", 30) / 100d;

    private static readonly int SlowDelayMs = Configured("LIVE_SCALE_SLOW_DELAY_MS", 2000);

    /// <summary>How long each recording runs, once every stream has been asked for one.</summary>
    private static readonly int RecordSeconds = Configured("LIVE_SCALE_RECORD", 45);

    /// <summary>
    /// The preview cadence the replica is configured with, so the freshness check can wait long
    /// enough to be sure rather than long enough to be hopeful.
    /// </summary>
    private const int PreviewInterval = 2;

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
    private HttpClient _storms = null!;
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
            builder.UseSetting(
                "Live:PreviewIntervalSeconds",
                PreviewInterval.ToString(CultureInfo.InvariantCulture));
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

        _storms = _factory.CreateClient();
        _storms.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        // Two hundred snapshots at once are two hundred decodes with nothing in front of them, and
        // the default hundred seconds would turn a slow replica into a rig that gave up on it. Five
        // minutes, so the measurement is the latency rather than the timeout.
        _storms.Timeout = TimeSpan.FromMinutes(5);

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
        _storms.Dispose();
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
        Report("columns          pod/rig are shares of one core; RcvQ:w is the busiest libsrt receive"
            + " worker, whose ceiling is one core");
        Report("                 pool/queued are the whole process's, this rig included, so a deep"
            + " queue says this process wants workers, not that the service does");
        Report($"machine          {Environment.ProcessorCount} cores"
            + (Vitals.Available ? string.Empty : ", /proc not readable here so no thread breakdown"));
        Report($"rig              {PerSender} streams per sender process,"
            + $" {Direct} of the readers are real players on the consumption port"
            + (SlowReaders > 0
                ? $", {SlowReaders} relayed readers pause {SlowDelayMs} ms per read"
                : string.Empty));
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

                    // The slow ones first, so a sample small enough to be all of them is all of them -
                    // the same order the relayed readers are given their pause in below.
                    _processes.Add(SrtSenders.StartCopyPlayers(
                        ConsumptionPort,
                        names,
                        index < SlowDirect ? SlowDirectRate : null));
                    _direct += names.Length;
                }
            }

            while (_relays.Count < target)
            {
                // The slow ones first, so a step small enough to be all of them is all of them.
                var slowly = _relays.Count < SlowReaders
                    ? TimeSpan.FromMilliseconds(SlowDelayMs)
                    : TimeSpan.Zero;

                _relays.Add(Relay.Open(_readers, Name(_relays.Count % streams), slowly));
            }

            var expected = target + _direct;

            var attached = await Settle(
                async () => await Viewers() >= expected,
                TimeSpan.FromSeconds(60),
                Viewers);

            Add(await Measure(meters, source, wire, streams, await Carrying(), expected, attached));
        }

        // ---- Everything else a replica is asked to do, at that load -------------------------
        if (Work)
        {
            await Working(meters, source, wire, streams);
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
    /// The rest of what a replica does, at the load the ramps ended on: the previews it is supposed
    /// to be keeping current, a snapshot of every stream at once, and a recording of every stream at
    /// once.
    ///
    /// This half exists because the ramps flattered the service. Ingest and fan-out are the two
    /// cheapest things this process does - they move bytes and never decode - so a rig that measures
    /// only those two reports a replica coasting at a third of its cores, and every expensive thing
    /// it does sits outside the measurement. A preview is a decode per keyframe and a JPEG encode per
    /// interval, per stream, always on. A snapshot is a container opened, decoded and encoded, per
    /// request. A recording is a muxer, a file, an object store and a document write, per stream.
    ///
    /// The previews are audited rather than assumed. They fail quietly by design: the decoder's own
    /// subscription is skip-to-live, so a decoder that cannot keep up drops packets rather than
    /// blocking, and <c>JpegEncoder.Encode</c> returning null leaves the old picture in place and
    /// counts nothing at all. A stale preview is therefore invisible in every figure the service
    /// publishes, which is exactly why it is checked here by fetching the same picture twice.
    /// </summary>
    private async Task Working(Meters meters, double source, double wire, int streams)
    {
        Report(string.Empty);

        // ---- The previews, which nothing else here would notice were stale -----------------
        var listing = await Listing();
        var pictured = listing.Values.Count(stream => stream.HasPreview);

        var wall = await Storm(streams, index => _client.GetAsync($"/api/live/preview/{Name(index)}"));

        // A sample rather than all of them: this holds two copies of every picture it compares, and
        // the question - are the harvesters keeping up - is answered by forty as well as by two
        // hundred. Two preview intervals apart, so a harvester that is keeping up has certainly
        // encoded again, and one that has not is behind rather than merely unlucky.
        var sample = Enumerable.Range(0, Math.Min(40, streams)).ToArray();
        var first = await Task.WhenAll(sample.Select(index => Tile(Name(index))));

        await Task.Delay(TimeSpan.FromSeconds(PreviewInterval * 2 + 1));

        var second = await Task.WhenAll(sample.Select(index => Tile(Name(index))));

        // Zipped rather than indexed by stream number: the two arrays are in the sample's order, which
        // is only the same thing while the sample happens to start at zero.
        var moved = first
            .Zip(second)
            .Count(pair => pair.First is { Length: > 0 }
                && pair.Second is { Length: > 0 }
                && !pair.First.SequenceEqual(pair.Second));

        // What this catches is a frozen preview, not a slow one: two intervals is long enough that a
        // harvester keeping its cadence has certainly encoded again, and also long enough that one
        // running at half cadence passes. Proving the cadence itself would mean sampling every
        // interval and is a different measurement.
        Report($"previews         {pictured} of {streams} streams hold a picture;"
            + $" {moved} of {sample.Length} sampled changed at least once in {PreviewInterval * 2 + 1}s"
            + " (a frozen preview fails this; one at half cadence does not)");
        Report($"wall of tiles    {streams} previews fetched at once: {wall}");

        // ---- A snapshot of every stream, at once -------------------------------------------
        // The heaviest thing a caller can ask for, and the one with no queue in front of it: each is
        // a container muxed out of the buffer, opened, decoded and JPEG-encoded, and nothing limits
        // how many run at the same time.
        Latencies snapshots = default;

        Add(await Measure(
            meters,
            source,
            wire,
            streams,
            await Carrying(),
            _relays.Count + _direct,
            await Viewers(),
            during: async () => snapshots = await Storm(
                streams,
                index => _storms.PostAsync($"api/live/snapshot/{Name(index)}", null)),
            note: $"{streams} snapshots at once"));

        Report($"snapshots        {snapshots}");
        Report($"                 outcomes {Describe(meters.Tally("live.snapshots", "outcome"))}");

        // ---- A recording of every stream, at once ------------------------------------------
        Latencies recordings = default;

        Add(await Measure(
            meters,
            source,
            wire,
            streams,
            await Carrying(),
            _relays.Count + _direct,
            await Viewers(),
            during: async () => recordings = await Storm(
                streams,
                index => _storms.PostAsJsonAsync(
                    $"api/live/record/{Name(index)}",
                    new RecordRequest(RecordSeconds))),
            note: $"{streams} recordings started"));

        Report($"recordings       {recordings}");

        // Waited for rather than read, because the gauge is republished once a beat: read straight
        // after the storm it still holds the count from before any of them started.
        var active = 0d;

        await Settle(
            async () => (active = meters.Value("live.recordings.active") ?? 0) >= streams,
            TimeSpan.FromSeconds(30),
            () => Task.FromResult((int)(meters.Value("live.recordings.active") ?? 0)));

        // Measured while they run, because the gauge is the only thing that says how many are
        // actually writing rather than how many were accepted.
        Add(await Measure(
            meters,
            source,
            wire,
            streams,
            await Carrying(),
            _relays.Count + _direct,
            await Viewers(),
            note: $"{active:0} recordings running"));

        // Anchored on the recordings actually stopping, not on the rig getting round to asking. The
        // first version of this measured from the end of the second window above, which at the default
        // settings is thirty seconds into a forty-five-second recording: the figure it produced was
        // three quarters recording time and was read - by me, in a write-up that had to be corrected -
        // as a queue of document writes. What is wanted is the tail after the last recorder finished.
        var stopped = Stopwatch.StartNew();

        await Settle(
            async () => (meters.Value("live.recordings.active") ?? 0) == 0,
            TimeSpan.FromSeconds(RecordSeconds + 60),
            () => Task.FromResult((int)(meters.Value("live.recordings.active") ?? 0)));

        stopped.Stop();

        var documents = 0;
        var waited = Stopwatch.StartNew();

        await Settle(
            async () => (documents = await Captures()) >= streams,
            TimeSpan.FromMinutes(5),
            async () => await Captures());

        waited.Stop();

        // A beat of slack either side: the gauge that says the recordings have stopped is republished
        // once a beat, so the tail is accurate to about two seconds and should be read that way rather
        // than as a rate to three figures.
        Report($"documents        {documents} of {streams} recordings became documents,"
            + $" the last {waited.Elapsed.TotalSeconds:0.0}s after the final recorder stopped"
            + $" (each ran {RecordSeconds}s; the gauge reached zero {stopped.Elapsed.TotalSeconds:0.0}s"
            + " into the wait for it, and is a beat coarse)");
        Report($"                 outcomes {Describe(meters.Tally("live.recordings", "outcome"))}"
            + $", {meters.Total("live.recorded.bytes") / 1024.0 / 1024:0} MiB stored");
    }

    /// <summary>
    /// Runs one request per index, all at once, and reports what each one cost. The point is the
    /// distribution rather than the total: a mean hides the two requests in two hundred that took
    /// thirty seconds, and those are the ones a person notices.
    /// </summary>
    private static async Task<Latencies> Storm<T>(int count, Func<int, Task<T>> call)
        where T : HttpResponseMessage
    {
        var timings = new double[count];
        var codes = new HttpStatusCode?[count];
        var clock = Stopwatch.StartNew();

        await Task.WhenAll(Enumerable.Range(0, count).Select(async index =>
        {
            var started = clock.Elapsed;

            try
            {
                using var response = await call(index);

                codes[index] = response.StatusCode;
            }
            catch (Exception)
            {
                // A request that never came back is the finding, so it is counted rather than thrown:
                // one failure in two hundred must not end the measurement of the other hundred and
                // ninety-nine.
                codes[index] = null;
            }

            timings[index] = (clock.Elapsed - started).TotalSeconds;
        }));

        clock.Stop();

        Array.Sort(timings);

        return new Latencies(
            count,
            codes.Count(code => code is >= HttpStatusCode.OK and < HttpStatusCode.MultipleChoices),
            codes.Count(code => code is null),
            timings.Length == 0 ? 0 : timings[timings.Length / 2],
            timings.Length == 0 ? 0 : timings[(int)(timings.Length * 0.95)],
            timings.Length == 0 ? 0 : timings[^1],
            clock.Elapsed.TotalSeconds);
    }

    private readonly record struct Latencies(
        int Asked,
        int Answered,
        int Faulted,
        double Median,
        double P95,
        double Slowest,
        double Wall)
    {
        public override string ToString()
            => $"{Answered} of {Asked} answered"
                + (Faulted > 0 ? $", {Faulted} never came back" : string.Empty)
                + $"; {Median:0.00}s median, {P95:0.00}s p95, {Slowest:0.00}s slowest,"
                + $" {Wall:0.0}s for all of them";
    }

    private static string Describe(IReadOnlyDictionary<string, int> tally)
        => tally.Count == 0
            ? "none"
            : string.Join(", ", tally.OrderByDescending(entry => entry.Value).Select(e => $"{e.Value} {e.Key}"));

    /// <summary>One preview's bytes, or empty where there is no picture to serve.</summary>
    private async Task<byte[]> Tile(string name)
    {
        try
        {
            using var response = await _client.GetAsync($"/api/live/preview/{name}");

            return response.IsSuccessStatusCode ? await response.Content.ReadAsByteArrayAsync() : [];
        }
        catch (Exception)
        {
            return [];
        }
    }

    /// <summary>Stored recordings, which is how many of the ones asked for actually became files.</summary>
    private async Task<int> Captures()
    {
        try
        {
            var documents = await _client.GetFromJsonAsync<List<DocumentResponse>>("/api/documents") ?? [];

            return documents.Count(document => document.FileName.EndsWith(".ts", StringComparison.Ordinal));
        }
        catch (Exception)
        {
            return 0;
        }
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
        int attached,
        Func<Task>? during = null,
        string note = "")
    {
        var before = Vitals.Read(_processes);
        var listedBefore = await Listing();
        var readBefore = _relays.Select(relay => relay.Bytes).ToArray();
        var beatsBefore = meters.Of("live.heartbeat.duration").Count;
        var lostBefore = meters.Total("live.packets.lost") + meters.Total("live.packets.dropped");
        var skipsBefore = meters.Count("live.overflows", "policy", "skip-to-live");

        var queued = 0L;
        using var sampling = new CancellationTokenSource();

        // The queue is sampled rather than read at the end, because starvation is a transient: two
        // hundred items waiting in the middle of a storm is the finding, and by the time the storm
        // has drained the queue is empty again and the row would say nothing happened.
        var sampler = Task.Run(async () =>
        {
            while (!sampling.IsCancellationRequested)
            {
                queued = Math.Max(queued, ThreadPool.PendingWorkItemCount);

                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), sampling.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        });

        var clock = Stopwatch.StartNew();

        // The window is a wait when the question is "what does this load cost at rest", and a body of
        // work when it is "what does that work cost while the load runs". Both are measured the same
        // way, which is the only way the two answers can be compared.
        //
        // A body still gets the full window even when it finishes sooner, because what the ingest
        // columns are made of - the registry's per-stream byte counts - is republished once a beat.
        // Two hundred snapshots answered in a tenth of a second would otherwise be measured against
        // a window in which no beat happened at all, and the row would read as a pod delivering
        // nothing. The latency figures beside the row are where a short storm is actually described.
        await Task.WhenAll(during?.Invoke() ?? Task.CompletedTask, Task.Delay(TimeSpan.FromSeconds(Window)));

        clock.Stop();

        await sampling.CancelAsync();
        await sampler;

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
            queued,
            note,
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

        if (_relays.FirstOrDefault(relay => relay.Fault is not null)?.Fault is { } fault)
        {
            // Printed rather than only counted: a relayed reader that took a 500 from the service and
            // one whose socket was torn down at teardown are the same integer in the table, and this
            // is the route the viewer findings are measured on.
            yield return $"reader fault     {_relays.Count(relay => relay.Fault is not null)} of"
                + $" {_relays.Count} relayed readers faulted, first: {fault.GetType().Name}:"
                + $" {fault.Message}";
        }

        var busiest = _rows[^1].Load;

        yield return $"busiest          {busiest.Busiest(6)} (summed per thread name)";
        yield return $"receive worker   {busiest.Share("SRT:RcvQ:w"):P0} of one core at the last step"
            + " (the ceiling is one core, and the measured knee about 60 %)";
        var starved = _rows.Where(row => row.Queued > 0).ToArray();

        yield return starved.Length == 0
            ? "thread pool      never queued: no row had work items waiting for a worker"
            : $"thread pool      queued up to {starved.Max(row => row.Queued)} work items"
                + $" ({starved.OrderByDescending(row => row.Queued).First().Note}),"
                + $" and the pool grew to {_rows.Max(row => row.Load.PoolThreads)} workers";

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
        long Queued,
        string Note,
        int Faults)
    {
        public const string Header =
            "streams  air  readers  seen  Mbit/s in  of src  of rig  Mbit/s out  RcvQ:w  pod   rig"
            + "   RSS       thr  pool  queued      udp   lost+drop  skip  verdict     what";

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
        /// The first row is therefore its own reference and can only come out clean or lossy. That is
        /// what calibration means, and it is also the assumption to check first if a whole table reads
        /// clean: a first step that was already sender-limited grades every row after it against a
        /// figure the rig, not the service, was holding down. The "of src" column is the guard - a
        /// first row far below the file's own rate is the warning.
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

        /// <summary>
        /// What the transport lost or dropped, against what arrived in the same window.
        ///
        /// The two are added together here and <see cref="LiveMetrics"/> is at pains to say they are
        /// different failures - never arrived against arrived too late - and that adding them loses
        /// what decides the fix. That holds for diagnosis; this figure is for the verdict, where both
        /// mean the same thing: media the pod was sent and could not deliver. Which of the two it was
        /// is in <c>GET /api/live</c> per stream, and in the run's own lost-and-dropped line.
        /// </summary>
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
                Load.PoolThreads.ToString(CultureInfo.InvariantCulture).PadLeft(4),
                Queued.ToString(CultureInfo.InvariantCulture).PadLeft(6),
                (Load.KernelUdpErrors?.ToString(CultureInfo.InvariantCulture) ?? "-").PadLeft(7),
                (Lost > 0 ? $"{Lost:0} ({LossShare:P1})" : "0").PadLeft(9),
                Skips.ToString(CultureInfo.InvariantCulture).PadLeft(4),
                Faults > 0 ? $"{Verdict}, {Faults} reader faults" : Verdict,
                Note);
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

        private Relay(HttpClient client, string name, TimeSpan slowly)
            => Pump = Task.Run(() => ReadAsync(client, name, slowly, _stopping.Token));

        public Task Pump { get; }

        public Exception? Fault { get; private set; }

        public long Bytes => Interlocked.Read(ref _bytes);

        public static Relay Open(HttpClient client, string name, TimeSpan slowly = default)
            => new(client, name, slowly);

        public async ValueTask DisposeAsync()
        {
            await _stopping.CancelAsync();

            try
            {
                // Bounded, because a read on the test server's response body that does not observe the
                // token would otherwise hang teardown with nothing around it to time out - and this
                // runs five hundred times. A reader that will not stop is abandoned to the collector,
                // which costs a hung task in a process that is about to end.
                await Pump.WaitAsync(TimeSpan.FromSeconds(10));
            }
            catch (Exception)
            {
                // A reader torn down mid-read is how every one of these ends, and a timeout here is
                // the abandonment described above rather than a failure worth reporting.
            }

            _stopping.Dispose();
        }

        private async Task ReadAsync(
            HttpClient client,
            string name,
            TimeSpan slowly,
            CancellationToken stopping)
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

                    if (slowly > TimeSpan.Zero)
                    {
                        await Task.Delay(slowly, stopping);
                    }
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

    /// <summary>
    /// Like <see cref="Configured"/>, but zero means zero rather than unset. It matters for the
    /// reader counts: a run isolating one route asks for none of the other, and treating that as
    /// "unconfigured" silently gives it fifty of them - which it did, in the first measurements taken
    /// with this rig, and the write-up had to say so.
    /// </summary>
    private static int Optional(string variable, int fallback)
        => int.TryParse(
            Environment.GetEnvironmentVariable(variable),
            CultureInfo.InvariantCulture,
            out var configured) && configured >= 0
                ? configured
                : fallback;

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
