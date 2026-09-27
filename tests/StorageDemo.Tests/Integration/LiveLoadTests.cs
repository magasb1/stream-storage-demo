using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using StorageDemo.Api.Controllers;
using StorageDemo.Api.Observability;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// Runs alone, and nothing else runs while it does.
///
/// Fifty encoders, fifty recordings and a wall of previews take every core the machine has, and the
/// rest of the live suite measures grace periods against a wall clock - which is why the ONNX tests
/// already have a collection of their own, for the same reason. Run in parallel with anything and
/// both halves lie: this test reads a machine it is sharing, and its neighbours time out.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LoadCollection
{
    public const string Name = "live-load";
}

/// <summary>
/// One replica under the load it is sized for, doing everything it does at once, with the meter as
/// the subject rather than an aside.
///
/// Every other test here proves a mechanism on one or two streams. This one proves the figures: that
/// what a pod publishes about itself is true while fifty encoders push, viewers come and go, every
/// stream is snapshotted and recorded, and ten feeds die - five of them with somebody watching. It
/// is the test <c>docs/observability.md</c> is written against, and it pins that page's own
/// fifty-stream row: clean at fifty, nothing refused, no recording overflowed, the beat keeping up
/// and the kernel losing nothing.
///
/// Two disciplines make it worth running rather than merely long.
///
/// **The meter is checked against the truth it claims to describe**, never against itself: bytes
/// counted by the hub against the bytes the registry reports per stream, the owned gauge against the
/// streams actually held, the recorded-byte counter against the documents in storage. A dashboard's
/// number is worth exactly that agreement.
///
/// **No tag may carry a stream name**, which fifty streams can show and one cannot. A name reaching
/// a tag is the single mistake that turns this meter into thousands of retained time series, and this
/// is the only test in the suite with enough names for the leak to be visible.
///
/// It is deliberately one test. "All at the same time" is the scenario; split into six, each would
/// pay the ramp again and none of them would be the thing being asked about.
///
/// How to read it as a rig. The senders replay a pre-encoded transport stream with <c>-c copy</c>,
/// because encoding the pattern inside each of fifty senders costs about a third of a core each and
/// measures the test machine rather than the service - the same finding, and the same fix, as
/// <c>scripts/load-senders.sh</c>'s <c>PATTERN</c>. Figures are reported through
/// <c>ITestOutputHelper</c> in the shape <c>.scratch/scale-to-1000/baseline.md</c> uses, so a run on
/// real hardware is a row in that table rather than a pass. <c>LIVE_LOAD_STREAMS</c> raises the
/// count for such a run; the thresholds below separate a clean pod from a collapsed one - the
/// baseline's collapse delivered a fifth of what was sent it - rather than grading the machine.
/// </summary>
[Collection(LoadCollection.Name)]
public sealed class LiveLoadTests(ITestOutputHelper output) : IAsyncLifetime
{
    private const string Token = "load-test-token";

    private const string NoLibsrt = "libsrt is not installed. Run scripts/fetch-libsrt.sh.";

    private const string NoFfmpegSrt = "This FFmpeg has no SRT. Run scripts/fetch-ffmpeg.sh.";

    /// <summary>
    /// Fifty: the load the rig scripts default to, and the load the observability page's first alert
    /// was measured at. <c>LIVE_LOAD_STREAMS</c> raises it to use this test as the rig. Everything
    /// below is derived from it, and the shape wants about twenty at the least - fewer, and the sets
    /// that are watched, closed, reopened and killed begin to overlap.
    /// </summary>
    private static readonly int Streams = Configured("LIVE_LOAD_STREAMS", 50);

    /// <summary>How many streams get a viewer in the first round: a fifth of them.</summary>
    private static readonly int Watched = Math.Max(4, Streams / 5);

    /// <summary>Of those, how many viewers leave again - and how many then arrive on fresh streams.</summary>
    private static readonly int Churned = Watched / 2;

    /// <summary>
    /// How long the pattern is. Longer than the whole run, because <c>-re</c> stops pacing at the
    /// end of the file: a sender that reaches it either stops or sends as fast as it can, and both
    /// would be measured as something the service did.
    /// </summary>
    private const int PatternSeconds = 300;

    /// <summary>
    /// The picture the senders push: the same busier, larger pattern <c>scripts/load-senders.sh</c>
    /// uses, so a run here can be read beside the rows in <c>baseline.md</c> that were measured with
    /// that script. The small pattern every other test sends compresses to a fifth of a megabit,
    /// which is fifty sockets rather than fifty streams of media.
    /// </summary>
    private const string Picture = "testsrc2=size=640x360:rate=25";

    /// <summary>
    /// How long each recording runs. Long enough that a heartbeat is certain to land while all of
    /// them are going, which is the only way the active-recordings gauge is read at its peak rather
    /// than on the way up.
    /// </summary>
    private const int RecordSeconds = 15;

    private readonly string _root = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-load-tests",
        Guid.NewGuid().ToString("N"));

    /// <summary>One per stream, by index, so the test can kill a chosen feed.</summary>
    private readonly List<Process> _senders = [];

    /// <summary>Every caller this test started, senders and viewers alike, for the teardown.</summary>
    private readonly List<Process> _callers = [];

    private WebApplicationFactory<Program> _factory = null!;
    private HttpClient _client = null!;
    private int _ingestPort;
    private string _pattern = null!;

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
            builder.UseSetting("Live:RecordingDirectory", Path.Combine(_root, "recordings"));

            // One ingest port, which is the shape every figure in baseline.md was measured on:
            // libsrt runs one receive worker thread per bound port, and that thread is what a
            // replica runs out of first. Binding more would hide the ceiling this test is about.
            builder.UseSetting("Live:IngestPortCount", "1");

            // Unlimited, so that live.rejects reading zero means the pod took everything rather
            // than that it was only ever asked for what it was allowed.
            builder.UseSetting("Live:MaxStreams", "0");

            // The production feed timeout, and a grace period only shortened. At two seconds a
            // healthy stream on a busy machine is declared interrupted by the rig rather than by
            // anything the service did, and this test's whole subject is a busy machine.
            builder.UseSetting("Live:FeedTimeoutSeconds", "5");
            builder.UseSetting("Live:GracePeriodSeconds", "10");

            // The default, stated rather than inherited: the preview is the dominant per-stream cost
            // in this service (baseline.md, defect 3), so it is part of what is measured here and
            // should not move quietly.
            builder.UseSetting("Live:PreviewIntervalSeconds", "2");
            builder.UseEnvironment("Production");
        });

        _client = _factory.CreateClient();
        _client.DefaultRequestHeaders.Add("X-Storage-Token", Token);

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var caller in _callers)
        {
            SrtSenders.Kill(caller);
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
    public async Task Fifty_streams_with_viewers_snapshots_and_recordings_at_once_are_carried_and_measured()
    {
        Assert.SkipUnless(Srt.IsAvailable, NoLibsrt);
        Assert.SkipUnless(FfmpegLibrary.InputProtocols().Contains("srt"), NoFfmpegSrt);

        // Said out loud rather than left to arithmetic: below about twenty, the sets that are
        // watched, closed, reopened and killed start to overlap and the test would be asserting
        // something other than what it says.
        Assert.SkipWhen(
            Streams < 20,
            $"LIVE_LOAD_STREAMS is {Streams}; this shape needs at least twenty streams.");

        // Both listeners are attached before anything is pushed. A counter is an event: one started
        // afterwards sees nothing of what it already counted, however often it was added to.
        using var meters = new Meters(_factory.Services.GetRequiredService<LiveMetrics>());
        using var requests = new Meters(_factory.Services.GetRequiredService<ApiMetrics>());

        // The kernel's counter is a total since boot for the whole network namespace, so what this
        // run did to it is a difference. It is the first thing the observability page says to alert
        // on, and the only signal that saw the 250-stream collapse while every stream still
        // reported itself healthy.
        var udpBefore = LiveMetrics.KernelUdpReceiveErrors();

        // ---- Fifty encoders ----------------------------------------------------------------
        SrtSenders.Render(_pattern, PatternSeconds, picture: Picture);

        // What one sender will hand a demultiplexer, and therefore what a healthy stream should be
        // seen to deliver. It is the pattern's payload rather than its file size on purpose: the hub
        // counts the bytes of the packets it publishes, so transport-stream headers, the program
        // tables and any padding are not in the figure and would make a clean pod look like one
        // delivering four fifths of what was sent to it.
        var source = SrtSenders.PayloadMbps(_pattern, PatternSeconds);
        var run = Stopwatch.StartNew();

        Report($"load             {Streams} streams x {source:0.00} Mbit/s, one ingest port");

        for (var index = 0; index < Streams; index++)
        {
            _senders.Add(Track(
                SrtSenders.StartSender(_ingestPort, $"#!::r={Name(index)},m=publish", file: _pattern)));
        }

        var carrying = 0;

        await Until(
            async () => (carrying = await Carrying()) == Streams,
            TimeSpan.FromMinutes(3),
            () => $"only {carrying} of {Streams} streams were carrying packets"
                + $": {SrtSenders.Complaints(_senders.Take(3))}");

        Report($"on air           {run.Elapsed.TotalSeconds:0.0} s for {Streams} streams"
            + $" ({Streams / run.Elapsed.TotalSeconds:0.#} accepts/s)");

        // Every name taken once, by this replica, and nothing turned away. A refusal here would be
        // the rig's own doing - two senders sharing a name - and a resume would mean a sender
        // reconnected, which is another test's subject.
        Assert.Equal(Streams, (int)meters.Total("live.accepts"));
        Assert.Equal(0, meters.Total("live.rejects"));
        Assert.Equal(Streams, meters.Tally("live.claims", "outcome")["taken"]);

        // Published by the heartbeat rather than by the claim, so it arrives a beat behind the
        // streams; waiting for it is also what proves the beat is what publishes it.
        await Until(
            () => Task.FromResult(meters.Value("live.streams.owned") == Streams),
            TimeSpan.FromSeconds(30),
            () => $"the gauge reported {meters.Value("live.streams.owned")} streams owned rather than {Streams}");

        Assert.Equal(0, meters.Value("live.streams.interrupted"));

        // ---- The wall of tiles -------------------------------------------------------------
        // A preview is a decode, so a stream is on air before it has a picture, and how long fifty
        // of them take to get one is the cost of the tile wall rather than of the ingest. Waited for
        // on the listing rather than by polling the route, because a miss is itself a counted
        // download and the count below is meant to be fifty tiles fetched once.
        var pictured = 0;

        await Until(
            async () => (pictured = (await Listing()).Values.Count(stream => stream.HasPreview)) == Streams,
            TimeSpan.FromMinutes(2),
            () => $"only {pictured} of {Streams} streams ever had a preview");

        Report($"previews         {run.Elapsed.TotalSeconds:0.0} s from the first sender"
            + $" until all {Streams} tiles held a picture");

        // What a client showing fifty live tiles does, and the reason a preview is counted as a
        // download of its own kind: this is the heaviest read the REST surface takes and it is not
        // a file download.
        var previews = await Task.WhenAll(
            Enumerable.Range(0, Streams).Select(index => _client.GetAsync($"/api/live/preview/{Name(index)}")));

        Assert.All(previews, preview => Assert.Equal(HttpStatusCode.OK, preview.StatusCode));

        Assert.Equal(
            Streams,
            requests
                .Of("api.documents.downloads")
                .Count(download => Meters.Tag(download, "kind") == "preview" && Meters.Tag(download, "outcome") == "served"));

        Assert.True(requests.Total("api.documents.download.bytes") > 0, "the previews carried no bytes");

        // ---- What is actually arriving -----------------------------------------------------
        // The honest overload signal. A collapsed pod lists every stream live with packets and bytes
        // rising while a fifth of the media arrives; the figure that says so is how much each stream
        // delivers against what its sender sent. Sampled per stream from the registry and in
        // aggregate from the meter over the same window, because a dashboard is worth exactly the
        // agreement between the two.
        var before = await Listing();
        var bytesBefore = meters.Total("live.bytes");
        var packetsBefore = meters.Total("live.packets");

        var sampled = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(20));
        sampled.Stop();

        var after = await Listing();
        var delivered = after
            .Where(stream => before.ContainsKey(stream.Key))
            .Select(stream => Mbps(stream.Value.Bytes - before[stream.Key].Bytes, sampled.Elapsed))
            .Order()
            .ToArray();

        var median = delivered[delivered.Length / 2];
        var registry = delivered.Sum();
        var metered = Mbps((long)(meters.Total("live.bytes") - bytesBefore), sampled.Elapsed);

        Report($"delivered        {median:0.00} of {source:0.00} Mbit/s per stream"
            + $" ({median / source:P0} of source, median), {registry:0.#} Mbit/s over {delivered.Length} streams");
        Report($"meter agreement  {metered:0.#} Mbit/s counted by the hub, {registry:0.#} reported per stream");

        // Not a grade of the machine. A clean pod delivers its senders' payload; the baseline's
        // collapsed rows delivered a fifth of it and its marginal ones under a half, so this sits
        // between those answers with a beat of sampling skew to spare.
        Assert.True(
            median > source * 0.75,
            $"the pod is past its knee: {median:0.00} of {source:0.00} Mbit/s delivered per stream");

        // The two figures are the same bytes counted in two places - by the hub as it publishes them
        // and by the heartbeat as it reads each stream - so they agree or one of them is describing
        // something else. A tenth is what the beat's own sampling can move them by.
        Assert.True(
            Math.Abs(metered - registry) < registry * 0.1,
            $"the meter counted {metered:0.#} Mbit/s where the registry reported {registry:0.#}");

        Assert.True(meters.Total("live.packets") > packetsBefore, "the packet counter stood still");

        // ---- Subscribers, opening and closing ----------------------------------------------
        // A viewer arrives, a viewer leaves, another arrives somewhere else, all while the pod
        // carries everything above. The interesting failure for a count touched from every
        // connection's own thread is the arithmetic under churn, and the census is where a leak
        // shows: a viewer counted and never subtracted is a number that only climbs.
        var viewers = new Dictionary<int, Process>();

        for (var index = 0; index < Watched; index++)
        {
            viewers[index] = Watch(index);
        }

        await Until(
            () => Task.FromResult(meters.Value("live.viewers") == Watched),
            TimeSpan.FromMinutes(1),
            () => $"the census reported {meters.Value("live.viewers")} viewers rather than {Watched}");

        // Each on its own stream, which is what the per-stream figure is for: the census says how
        // much this replica is fanning out, and GET /api/live says who is watching what.
        var watched = await Listing();

        Assert.All(Enumerable.Range(0, Watched), index => Assert.Equal(1, watched[Name(index)].Viewers));

        for (var index = 0; index < Churned; index++)
        {
            SrtSenders.Kill(viewers[index]);
            viewers.Remove(index);
        }

        await Until(
            () => Task.FromResult(meters.Value("live.viewers") == Watched - Churned),
            TimeSpan.FromMinutes(1),
            () => $"the census kept {meters.Value("live.viewers")} viewers after {Churned} of them left");

        for (var index = Watched; index < Watched + Churned; index++)
        {
            viewers[index] = Watch(index);
        }

        await Until(
            () => Task.FromResult(meters.Value("live.viewers") == Watched),
            TimeSpan.FromMinutes(1),
            () => $"the census reported {meters.Value("live.viewers")} viewers after {Churned} more arrived");

        // ---- A snapshot and a recording on every stream, at once ---------------------------
        var snapshots = await Task.WhenAll(
            Enumerable
                .Range(0, Streams)
                .Select(index => _client.PostAsync($"/api/live/snapshot/{Name(index)}", null)));

        Assert.All(snapshots, snapshot => Assert.Equal(HttpStatusCode.OK, snapshot.StatusCode));

        var started = await Task.WhenAll(
            Enumerable
                .Range(0, Streams)
                .Select(index => _client.PostAsJsonAsync(
                    $"/api/live/record/{Name(index)}",
                    new RecordRequest(RecordSeconds))));

        Assert.All(started, recording => Assert.Equal(HttpStatusCode.Accepted, recording.StatusCode));

        // All of them running together, read off the gauge rather than off the requests that started
        // them: fifty accepted requests and fifty recordings actually writing are different claims,
        // and the second is what a pod's disk and queues are sized for.
        await Until(
            () => Task.FromResult(meters.Value("live.recordings.active") == Streams),
            TimeSpan.FromSeconds(40),
            () => $"the gauge reported {meters.Value("live.recordings.active")} recordings rather than {Streams}");

        var stills = new List<DocumentResponse>();

        await Until(
            async () => (stills = await Captures(".jpg")).Count == Streams,
            TimeSpan.FromMinutes(2),
            () => $"only {stills.Count} of {Streams} snapshots became documents");

        var clips = new List<DocumentResponse>();

        await Until(
            async () => (clips = await Captures(".ts")).Count == Streams,
            TimeSpan.FromMinutes(3),
            () => $"only {clips.Count} of {Streams} recordings became documents");

        // One of each per stream, named after the stream it came from, so fifty captures are fifty
        // findable files rather than a pile of them.
        Assert.All(
            Enumerable.Range(0, Streams),
            index =>
            {
                Assert.Single(stills, still => still.FileName.StartsWith($"{Flattened(index)}-", StringComparison.Ordinal));
                Assert.Single(clips, clip => clip.FileName.StartsWith($"{Flattened(index)}-", StringComparison.Ordinal));
            });

        Assert.All(clips, clip => Assert.Equal("video/mp2t", clip.ContentType));
        Assert.All(clips, clip => Assert.True(clip.Size > 0, $"{clip.FileName} is empty"));

        var captured = meters.Tally("live.snapshots", "outcome");

        Report($"snapshots        {captured.GetValueOrDefault("stored")} decoded fresh"
            + $", {captured.GetValueOrDefault("preview")} from the harvester's older picture");

        // A snapshot that could capture nothing at all is the failure. Falling back to the
        // harvester's smaller, older picture is not - that is a stream which has sent no recent
        // keyframe - but it is reported, because a run where most of them fall back is a pod whose
        // decoders are behind rather than a pod taking snapshots.
        Assert.Equal(0, captured.GetValueOrDefault("none"));
        Assert.Equal(Streams, captured.Values.Sum());

        // Every recording whole. A truncated one here is not a flake to re-run: it is the recorder's
        // queue overflowing under this load, which is the second thing the observability page says
        // to alert on, and it has a short document behind it.
        Assert.Equal(Streams, meters.Tally("live.recordings", "outcome")["stored"]);
        Assert.Equal(
            0,
            meters.Of("live.overflows").Count(overflow => Meters.Tag(overflow, "policy") == "fail"));

        Report($"fan-out          {meters.Of("live.overflows").Count(o => Meters.Tag(o, "policy") == "skip-to-live")}"
            + " viewers skipped to live");

        // The byte counter against the documents it describes. It is added to as each part is
        // stored, so a six-hour recording reports while it runs; at the end of fifty of them the sum
        // is what is in storage, or it is measuring something else.
        Assert.Equal(clips.Sum(clip => clip.Size), (long)meters.Total("live.recorded.bytes"));

        // ---- Feeds that exit, with an audience and without ---------------------------------
        // Two sets, because what they would expose differs. A stream nobody watches tears down a
        // hub with its own subscribers; a stream somebody watches tears down one holding a viewer's
        // subscription, and that viewer has to be subtracted from a count it was added to on another
        // thread. Both should end identically, and the streams either side should not notice.
        var killedWatched = Enumerable.Range(Churned, Watched - Churned).ToArray();
        var killedAlone = Enumerable.Range(Streams - Churned, Churned).ToArray();
        var killed = killedWatched.Concat(killedAlone).ToArray();

        var bystander = Name(Watched + Churned + 1);
        var carried = (await Listing())[bystander].Packets;

        foreach (var index in killed)
        {
            SrtSenders.Kill(_senders[index]);
        }

        // Interrupted first rather than gone: a tile that vanishes and returns is worse than one
        // showing a state, and a reconnect inside this window would be the same stream resuming.
        await Until(
            async () => await Listing() is { } listing
                && killed.All(index => listing.TryGetValue(Name(index), out var stream)
                    && stream.State == LiveStreamState.Interrupted),
            TimeSpan.FromSeconds(40),
            () => $"the {killed.Length} dead feeds never all read as interrupted");

        await Until(
            async () => await Listing() is { } listing && killed.All(index => !listing.ContainsKey(Name(index))),
            TimeSpan.FromSeconds(60),
            () => $"the {killed.Length} dead feeds were still listed after the grace period");

        // Ended the same way whether or not anybody was watching, and ended once each.
        Assert.Equal(killed.Length, meters.Tally("live.streams.ended", "reason")["expired"]);

        // And the gauge follows, which is the number an autoscaler would be acting on.
        await Until(
            () => Task.FromResult(meters.Value("live.streams.owned") == Streams - killed.Length),
            TimeSpan.FromSeconds(30),
            () => $"the gauge reported {meters.Value("live.streams.owned")} owned"
                + $" after {killed.Length} feeds died");

        // The viewers of the dead streams went with them, and only those. The five that were
        // watching something that survived are still counted, and their players are still running.
        await Until(
            () => Task.FromResult(meters.Value("live.viewers") == viewers.Count - killedWatched.Length),
            TimeSpan.FromMinutes(1),
            () => $"the census reported {meters.Value("live.viewers")} viewers after"
                + $" {killedWatched.Length} watched streams died");

        // A dying stream's neighbours never knew. This is what the load is for: at fifty streams a
        // teardown holding a shared lock, or a fan-out stalling behind a dying subscriber, shows up
        // here as a bystander that stopped carrying.
        var bystanderNow = (await Listing())[bystander];

        Assert.Equal(LiveStreamState.Live, bystanderNow.State);
        Assert.True(
            bystanderNow.Packets > carried,
            $"a bystanding stream stopped carrying: {bystanderNow.Packets} packets, was {carried}");

        foreach (var viewer in viewers.Values)
        {
            SrtSenders.Kill(viewer);
        }

        await Until(
            () => Task.FromResult(meters.Value("live.viewers") == 0),
            TimeSpan.FromMinutes(1),
            () => $"the census kept {meters.Value("live.viewers")} viewers after every one of them left");

        // One session per viewer served, every one of them direct: this replica owns every stream,
        // so nothing was relayed. Counted where a viewer is actually subscribed rather than where an
        // attach is decided, which is what stops a retrying attach reading as an audience.
        Assert.Equal(Watched + Churned, meters.Total("live.viewer.sessions"));
        Assert.All(meters.Of("live.viewer.sessions"), session => Assert.Equal("direct", Meters.Tag(session, "route")));

        // ---- What the beat cost, and what the kernel saw -----------------------------------
        var beats = meters.Of("live.heartbeat.duration").Select(beat => beat.Value).Order().ToArray();
        var slowest = beats[^1];
        var typical = beats[beats.Length / 2];
        var lost = meters.Total("live.packets.lost");
        var dropped = meters.Total("live.packets.dropped");
        var udp = udpBefore is { } start && LiveMetrics.KernelUdpReceiveErrors() is { } end
            ? end - start
            : (long?)null;

        Report($"heartbeat        {typical * 1000:0} ms median, {slowest * 1000:0} ms slowest"
            + $", over {beats.Length} passes");
        Report($"transport        {lost:0} lost and {dropped:0} dropped of {meters.Total("live.packets"):0} packets");
        Report($"kernel udp       {(udp is { } errors ? errors.ToString(CultureInfo.InvariantCulture) : "not readable here")}"
            + " receive errors over the run");

        // The beat is two seconds, and a pass slower than the beat leaves every other replica
        // reading a stale registry - which is how two pods come to admit one name. Fifty streams
        // must not be what does that. The median is the claim; the slowest pass is reported, because
        // one long pass on a contended machine says nothing.
        Assert.True(typical < 2, $"the median heartbeat pass took {typical:0.00}s against a two-second beat");

        // Nothing was swallowed. All three stages are caught so that a registry blip cannot take the
        // ingest port down, which is exactly why they are counted rather than only logged.
        Assert.Equal(0, meters.Total("live.heartbeat.failures"));

        // Loss and drop are per-stream health and on a loopback at this load they should be noise.
        // A percent of the traffic is not noise: it is a receive path at its knee, and the
        // per-stream figures in GET /api/live are what name the streams it is hurting.
        Assert.True(
            lost + dropped < meters.Total("live.packets") * 0.01,
            $"{lost + dropped:0} of {meters.Total("live.packets"):0} packets were lost or dropped");

        // Linux only, and the one figure here that is not the service's opinion of itself. Zero over
        // a whole run at fifty streams is what the rig measured; anything above it is the shared
        // receive buffer overflowing, which no per-socket counter can see.
        if (udp is { } receiveErrors)
        {
            Assert.True(
                receiveErrors == 0,
                $"{receiveErrors} kernel UDP receive errors during the run. The counter is the whole"
                + " network namespace's, so on a machine doing other UDP work some of these may not"
                + " be this pod's; sustained against a stream count is the collapse itself.");
        }

        // ---- The rule only fifty streams can check -----------------------------------------
        // Nothing carries a stream name, and every tag is one of the six keys the observability page
        // documents. Fifty streams times twenty instruments is the thousand-time-series mistake the
        // rule exists to prevent, and every name here begins with the same word.
        var tags = meters.Read().SelectMany(measurement => measurement.Tags).ToArray();

        Assert.All(
            tags,
            tag => Assert.Contains(tag.Key, (string[])["port", "reason", "outcome", "route", "policy", "stage"]));

        Assert.All(
            tags,
            tag => Assert.DoesNotContain(
                "load",
                tag.Value?.ToString() ?? string.Empty,
                StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>The name a sender presents, and the only place the numbering is decided.</summary>
    private static string Name(int index) => $"load/{index:0000}";

    /// <summary>The same name as a document carries it: a file name may not hold a slash.</summary>
    private static string Flattened(int index) => Name(index).Replace('/', '-');

    private static int Configured(string variable, int fallback)
        => int.TryParse(
            Environment.GetEnvironmentVariable(variable),
            CultureInfo.InvariantCulture,
            out var configured) && configured > 0
                ? configured
                : fallback;

    private static double Mbps(long bytes, TimeSpan over)
        => over.TotalSeconds <= 0 ? 0 : bytes * 8 / over.TotalSeconds / 1_000_000;

    private Process Track(Process caller)
    {
        _callers.Add(caller);

        return caller;
    }

    private Process Watch(int index)
        => Track(SrtSenders.StartPlayer(ConsumptionPort, $"#!::r={Name(index)},m=request"));

    private async Task<Dictionary<string, LiveStream>> Listing()
    {
        var status = await _client.GetFromJsonAsync<LiveStatusResponse>("/api/live");

        return status?.Streams.ToDictionary(stream => stream.Name) ?? [];
    }

    private async Task<int> Carrying()
        => (await Listing()).Values.Count(stream => stream is { State: LiveStreamState.Live, Packets: > 0 });

    /// <summary>Stored captures of one kind, which is how a snapshot and a recording are found again.</summary>
    private async Task<List<DocumentResponse>> Captures(string extension)
    {
        var documents = await _client.GetFromJsonAsync<List<DocumentResponse>>("/api/documents") ?? [];

        return [.. documents.Where(document => document.FileName.EndsWith(extension, StringComparison.Ordinal))];
    }

    private void Report(string line) => output.WriteLine(line);

    /// <param name="describe">
    /// What to say when it never came true, read after the last attempt, so a failure carries the
    /// figure it was waiting on rather than sending the next reader back to the source.
    /// </param>
    private static async Task Until(Func<Task<bool>> condition, TimeSpan timeout, Func<string> describe)
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

        Assert.Fail($"{describe()} within {timeout}");
    }
}
