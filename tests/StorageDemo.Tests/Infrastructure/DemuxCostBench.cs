using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Integration;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// What one demultiplexed packet costs the thread that demultiplexes it, measured off a file.
///
/// It exists because the scale rig named that thread and could not say what it was spending on.
/// <c>.scratch/scale-to-1000/ingest-and-readers.md</c> measured 1.8 cores of
/// <c>.NET Long Runni</c> against the libsrt receive worker's 0.24 at two hundred camera-rate
/// streams, which says ingest density is bounded by per-packet demultiplexing cost but not by which
/// part of it. A rig run cannot answer that: every thread there is scheduler-bound, and that rig's
/// own repeatability is about a tenth of a core against effects worth a hundredth, so a change
/// worth a microsecond or two per packet is below its noise by an order of magnitude.
///
/// Off a file rather than off a socket, deliberately. The same picture the scale rig pushes
/// (<see cref="Picture"/> at <see cref="Bitrate"/>) is rendered once and demultiplexed as fast as
/// the processor allows, so what is measured is libav's demuxer, the copy out of it and the hub -
/// everything the per-stream thread does - with the transport, the pacing and the contention taken
/// out. The figures are a cost per packet and not a throughput, and they are not comparable to a
/// rig row; they are comparable to each other, which is the whole point.
///
/// Eight configurations. Every one of them opens the file the way the production pump opens a URL
/// (<see cref="OpenLikeProduction"/>), because an open costs milliseconds and a difference in how
/// two rows open is charged to whatever else differs between them:
/// <list type="bullet">
/// <item><c>floor</c> - <c>av_read_frame</c> and nothing else. libav's own cost.</item>
/// <item><c>copy</c> - the floor plus the per-packet <c>byte[]</c> and the copy into it, retained by
/// nobody. The difference from the floor is what the allocation costs when it dies immediately.</item>
/// <item><c>full</c> - the real <see cref="StreamDemuxer"/> into a real <see cref="StreamHub"/> with
/// the production thirty-second buffer and no subscribers, which is the ingest-only shape the rig's
/// collapsed row was measured in. The difference from <c>copy</c> is the hub, the buffer, and the
/// collector having to promote every packet the buffer retains rather than dropping it in gen0 -
/// which is the part of the allocation's cost that a per-packet stopwatch would never see.</item>
/// <item><c>mirror</c>, <c>uninit</c> and <c>pool-max</c> - one loop
/// (<see cref="Variant(string, int, Allocation)"/>) run three times, differing in the one line that
/// obtains the array and nothing else. <c>mirror</c> allocates as the pump does, so the gap between
/// it and <c>full</c> is what duplicating the loop cost rather than what any change is worth;
/// <c>uninit</c> skips the zeroing; <c>pool-max</c> rents and returns immediately. Comparing the
/// three to each other rather than to <c>full</c> is what makes the allocation the only variable.
/// </item>
/// <item><c>avio-64k</c> and <c>avio-1316</c> - the same full pump reading through
/// <see cref="AvioReader"/> over a stream that answers in at most that many bytes. 1316 is libsrt's
/// default payload size and therefore the granularity a real ingest actually reads at, because
/// libsrt delivers whole messages and <see cref="SrtSocketStream"/> passes one up per read. The
/// difference between the two is what that granularity costs on this side of the P/Invoke; it does
/// not include libsrt's own cost per message, which needs a socket and is not measured here. These
/// two differ from each other in one argument, so they are the cleanest pair here.</item>
/// </list>
///
/// Processor time rather than wall clock, taken from the whole process rather than the one thread:
/// the retention is what makes an allocation expensive, and the threads that pay for retention are
/// the collector's, so a metric stopping at the demultiplexing thread's own stack would miss exactly
/// the part being measured. What that tolerates is other processes on the box, not other threads in
/// this one - which is why each configuration runs alone on a thread of its own. Enough passes over
/// the pattern to allocate something over a gigabyte, because at one pass nothing is collected at
/// all and a configuration that is never collected says nothing about collection.
///
/// Three things the run reports about itself before the rows, so that what the figures can carry is
/// measured rather than left to a reader to assume: what an open costs, because it is milliseconds
/// against a packet's microseconds and so sits inside every row; what the processor clock's own
/// resolution is, which on this kernel turns out to be microseconds rather than the ten-millisecond
/// clock tick <c>/proc</c> would give and therefore is not the limit here - the replicate spread of
/// about half a microsecond a packet is; and what the runtime's zeroing of one array costs on its
/// own, which is what makes a null result on <c>uninit</c> mean something.
///
/// One limit it cannot report. Reading a file back to back allocates about 500 MB/s against the
/// rig's measured 7.1 MB/s, so gen1 collections here are few per replicate and gen2 zero: promotion
/// is the least-sampled part of these figures. That understates what retention costs, which is the
/// direction that cannot flatter a pool, but it does mean the ceiling below is this live set's
/// rather than a pod's.
/// </summary>
[Collection(LoadCollection.Name)]
public sealed unsafe class DemuxCostBench(ITestOutputHelper output)
{
    private const string NotAsked =
        "This measures a cost per packet rather than asserting anything. Set DEMUX_COST=1 to run it.";

    /// <summary>What the scale rig pushes for a camera-rate stream, so the packet mix is that one.</summary>
    private const string Picture = "testsrc2=size=1280x720:rate=30";

    private const string Bitrate = "4000k";

    /// <summary>
    /// Long enough that the rolling buffer fills its thirty-second window and starts evicting.
    /// A buffer still filling never evicts anything, and a heap where nothing ever dies is not the
    /// one the service runs on.
    /// </summary>
    private const int PatternSeconds = 45;

    /// <summary>
    /// How many times each configuration reads the pattern. Sixty passes is about 1.3 GB allocated
    /// and eighty thousand packets, which is enough that the collector runs repeatedly and its cost
    /// lands in the figures instead of being deferred past the end of the measurement.
    /// </summary>
    private const int Passes = 60;

    /// <summary>
    /// Replicates per configuration, reported individually. The spread between them is the only
    /// honest statement about the noise on a shared four-core box; a mean on its own would hide it.
    /// </summary>
    private const int Replicates = 3;

    /// <summary>
    /// The probe budget every configuration opens with, held once rather than built per pass so the
    /// open contributes no allocation of its own to the figures.
    /// </summary>
    private static readonly LiveOptions ProbeDefaults = new();

    /// <summary>How a configuration's per-packet array is obtained. The only difference between three of them.</summary>
    private enum Allocation
    {
        /// <summary>What the pump does now: <c>new byte[]</c>, zeroed by the runtime before the copy overwrites it.</summary>
        Zeroed,

        /// <summary>The same array without the zeroing. No pool, no reference count, no oversize.</summary>
        Uninitialized,

        /// <summary>Rented and returned at once - an upper bound rather than a pool. See <see cref="Variant"/>.</summary>
        Pooled,
    }

    private void Report(string line) => output.WriteLine(line);

    [Fact]
    public void One_packet_through_the_pump_costs()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable("DEMUX_COST") is not (null or ""), NotAsked);

        FfmpegLibrary.EnsureLoaded();

        var directory = Directory.CreateTempSubdirectory("demux-cost").FullName;

        try
        {
            var pattern = Path.Combine(directory, "camera-rate.ts");

            SrtSenders.Render(pattern, PatternSeconds, picture: Picture, bitrate: Bitrate);

            Report($"pattern          {new FileInfo(pattern).Length / 1024 / 1024} MiB of {Picture}"
                + $" at {Bitrate}, {PatternSeconds} s, read {Passes} times per replicate");
            Report($"machine          {Environment.ProcessorCount} cores,"
                + $" server GC {System.Runtime.GCSettings.IsServerGC},"
                + $" {(Debugger.IsAttached ? "debugger attached" : "no debugger")}");
            Report("columns          cpu/pkt is whole-process processor time over packets published,"
                + " so the collector's threads are in it");
            Report(string.Empty);

            // Discarded. The first pass through any of these paths pays for jitting all of them and
            // for whatever the renderer left outside the page cache.
            Measure("warm-up", pattern, path => Full(path, passes: 1));

            Report(OpenCost(pattern));
            Report(ResolutionFloor());
            Report(ZeroingCost());
            Report(string.Empty);

            var readings = new Dictionary<string, List<Run>>();

            foreach (var (name, run) in Configurations())
            {
                var replicates = readings[name] = [];

                for (var replicate = 0; replicate < Replicates; replicate++)
                {
                    var reading = Measure(name, pattern, run);

                    replicates.Add(reading);
                    Report(reading.Describe(name));
                }
            }

            Report(string.Empty);
            Report(BucketRounding(readings["pool-max"]));

            // The comparison is between configurations reading the same packets. If they did not,
            // every difference above is an artefact and the reader has no way to tell from the rows.
            SameWorkEverywhere(readings);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static (string Name, Func<string, Run> Run)[] Configurations() =>
    [
        ("floor", path => WithoutHub(path, Passes, copy: false)),
        ("copy", path => WithoutHub(path, Passes, copy: true)),
        ("full", path => Full(path, Passes)),
        ("mirror", path => Variant(path, Passes, Allocation.Zeroed)),
        ("uninit", path => Variant(path, Passes, Allocation.Uninitialized)),
        ("pool-max", path => Variant(path, Passes, Allocation.Pooled)),
        ("avio-64k", path => ThroughAvio(path, Passes, 64 * 1024)),
        ("avio-1316", path => ThroughAvio(path, Passes, Srt.LiveDefaultPayloadSize)),
    ];

    /// <summary>
    /// Every configuration has to have read the same packets, or the per-packet costs above are not
    /// comparable and the whole point of the run is gone. A configuration that published nothing
    /// would otherwise print <c>0 pkt</c> and pass quietly.
    /// </summary>
    private static void SameWorkEverywhere(Dictionary<string, List<Run>> readings)
    {
        var counts = readings
            .SelectMany(entry => entry.Value.Select(run => (entry.Key, run.Packets)))
            .ToArray();

        Assert.DoesNotContain(counts, reading => reading.Packets == 0);
        Assert.Single(counts.Select(reading => reading.Packets).Distinct());
    }

    /// <summary>One configuration's reading.</summary>
    private readonly record struct Run(long Packets, long Bytes, long Allocated, long Large)
    {
        public TimeSpan Processor { get; init; }

        public TimeSpan Paused { get; init; }

        public int Gen0 { get; init; }

        public int Gen1 { get; init; }

        public int Gen2 { get; init; }

        /// <summary>Total of every packet's own size, which is what the media actually is.</summary>
        public long Exact { get; init; }

        /// <summary>Total length of the arrays those packets were put in, which is what memory they cost.</summary>
        public long Held { get; init; }

        /// <summary>The biggest single packet, which is what decides whether the large object heap is involved at all.</summary>
        public long Largest { get; init; }

        public string Describe(string name) => string.Format(
            CultureInfo.InvariantCulture,
            "{0,-10} {1,7} pkt  {2,7:0.0} B/pkt alloc  {3,7:0.00} us/pkt cpu"
                + "  gc {4,4}/{5,3}/{6,2}  paused {7,7:0.0} ms ({8,4:0.0} % of cpu)"
                + "  {9} held  {10} over 85 KiB{11}",
            name,
            Packets,
            Packets == 0 ? 0 : Allocated / (double)Packets,
            Packets == 0 ? 0 : Processor.TotalMicroseconds / Packets,
            Gen0,
            Gen1,
            Gen2,
            Paused.TotalMilliseconds,
            Processor == TimeSpan.Zero ? 0 : 100 * Paused.TotalMilliseconds / Processor.TotalMilliseconds,
            Held == 0 ? "     -" : string.Format(CultureInfo.InvariantCulture, "x{0:0.000}", Held / (double)Exact),
            Large < 0 ? "not counted" : Large.ToString(CultureInfo.InvariantCulture),
            Largest == 0 ? string.Empty : string.Format(
                CultureInfo.InvariantCulture,
                ", largest {0} B",
                Largest));
    }

    /// <summary>
    /// What the bucket rounding really costs, in bytes rather than in a rounded column.
    ///
    /// This is the figure a proposal to pool has to carry on the memory side, and the run prints the
    /// two totals it is a ratio of so that the next person can check it rather than trust it.
    /// </summary>
    private static string BucketRounding(List<Run> pooled)
    {
        var exact = pooled.Select(run => run.Exact).Distinct().Single();
        var held = pooled.Select(run => run.Held).Distinct().Single();

        return string.Format(
            CultureInfo.InvariantCulture,
            "bucket rounding  {0} B of media in {1} B of rented array, x{2:0.0000}"
                + " ({3:0.0} % more memory held for the same window)",
            exact,
            held,
            held / (double)exact,
            (100.0 * held / exact) - 100);
    }

    /// <summary>
    /// What opening the file costs per pass, reported so a reader can subtract it rather than have
    /// it hidden inside every row.
    ///
    /// It matters because an open is milliseconds where a packet is microseconds: at this pattern's
    /// 1,350 packets a pass, a millisecond of open is three quarters of a microsecond a packet. Every
    /// configuration here opens the same way for that reason, and this line says how much of each
    /// row is the open rather than the reading.
    /// </summary>
    private string OpenCost(string pattern)
    {
        var run = Measure("open", pattern, path =>
        {
            long packets = 0;

            for (var pass = 0; pass < Passes; pass++)
            {
                AVFormatContext* format = null;

                try
                {
                    OpenLikeProduction(pattern, &format);
                    packets += format->nb_streams;
                }
                finally
                {
                    if (format is not null)
                    {
                        ffmpeg.avformat_close_input(&format);
                    }
                }
            }

            return new Run(packets, 0, 0, -1);
        });

        return string.Format(
            CultureInfo.InvariantCulture,
            "open+probe       {0:0.00} ms per pass, which over a pass of packets is {1:0.00} us/pkt"
                + " inside every row below",
            run.Processor.TotalMilliseconds / Passes,
            run.Processor.TotalMicroseconds / (Passes * 1350.0));
    }

    /// <summary>
    /// What the runtime's zeroing of a packet-sized array costs on its own, away from the demuxer.
    ///
    /// This exists to keep the <c>uninit</c> row honest. If that row shows no gain, the reading is
    /// worth only as much as this instrument's ability to have seen one, and the way to establish
    /// that is to measure the same two allocations with nothing else in the loop at all. Each array
    /// is fully overwritten by a copy of its own length, exactly as the pump overwrites it, because
    /// zeroing that the copy is about to overwrite in the same cache lines is not the same
    /// proposition as zeroing measured alone.
    /// </summary>
    private static string ZeroingCost()
    {
        const int arrays = 200_000;

        var size = 16_734;
        var source = new byte[size];
        var ring = new byte[16][];

        double Time(bool zeroed)
        {
            var clock = Stopwatch.StartNew();

            fixed (byte* from = source)
            {
                for (var index = 0; index < arrays; index++)
                {
                    var data = zeroed ? new byte[size] : GC.AllocateUninitializedArray<byte>(size);

                    Marshal.Copy((IntPtr)from, data, 0, size);

                    // Some kept alive, so this is an allocation the collector has to deal with
                    // rather than one it can drop immediately.
                    ring[index % ring.Length] = data;
                }
            }

            GC.KeepAlive(ring);

            return clock.Elapsed.TotalMicroseconds / arrays;
        }

        // Both ways once first, discarded: the first of the two would otherwise pay for jitting both.
        Time(zeroed: true);
        Time(zeroed: false);

        var zeroedCost = Time(zeroed: true);
        var uninitialisedCost = Time(zeroed: false);

        return string.Format(
            CultureInfo.InvariantCulture,
            "zeroing          a {0} B array allocated and overwritten costs {1:0.00} us zeroed"
                + " against {2:0.00} us uninitialised, so skipping the zeroing is worth {3:0.00} us",
            size,
            zeroedCost,
            uninitialisedCost,
            zeroedCost - uninitialisedCost);
    }

    /// <summary>
    /// The smallest difference this instrument can see, measured rather than assumed.
    ///
    /// <see cref="Process.TotalProcessorTime"/> comes from <c>/proc</c> on Linux and advances in
    /// clock ticks, so a reading is quantised however long a tick is. Sampling it across a busy loop
    /// finds the tick, and the tick over a replicate's packet count is the floor under every figure
    /// in the rows below.
    /// </summary>
    private static string ResolutionFloor()
    {
        var process = Process.GetCurrentProcess();
        var steps = new List<double>();
        var last = process.TotalProcessorTime;
        var spin = Stopwatch.StartNew();

        while (spin.ElapsedMilliseconds < 200)
        {
            var now = process.TotalProcessorTime;

            if (now != last)
            {
                steps.Add((now - last).TotalMicroseconds);
                last = now;
            }
        }

        var tick = steps.Count == 0 ? 0 : steps.Min();

        return string.Format(
            CultureInfo.InvariantCulture,
            "resolution       processor time advances in {0:0} us steps, so a replicate of {1} packets"
                + " reads to +-{2:0.00} us/pkt",
            tick,
            Passes * 1350,
            tick / (Passes * 1350.0));
    }

    /// <summary>
    /// Runs one configuration on a thread of its own with the heap emptied first, so its allocation
    /// figure is its own and it does not inherit somebody else's garbage to collect.
    /// </summary>
    private static Run Measure(string name, string pattern, Func<string, Run> run)
    {
        Settle();

        var process = Process.GetCurrentProcess();
        var processor = process.TotalProcessorTime;
        var paused = GC.GetTotalPauseDuration();
        var (gen0, gen1, gen2) = (GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2));

        var measured = default(Run);

        // Nothing here is allowed to escape the worker: an exception on a thread started by hand
        // takes the test host down with it rather than failing the test, so it is caught, carried
        // back and rethrown with its stack intact.
        ExceptionDispatchInfo? failure = null;

        var thread = new Thread(
            () =>
            {
                try
                {
                    measured = run(pattern);
                }
                catch (Exception error)
                {
                    failure = ExceptionDispatchInfo.Capture(error);
                }
            },
            maxStackSize: 1024 * 1024)
        {
            Name = name,
        };

        thread.Start();
        thread.Join();

        failure?.Throw();

        // Read before settling: a forced collection after the run would charge this configuration
        // for freeing everything it still holds, which the service never pays for all at once.
        var taken = process.TotalProcessorTime - processor;

        return measured with
        {
            Processor = taken,
            Paused = GC.GetTotalPauseDuration() - paused,
            Gen0 = GC.CollectionCount(0) - gen0,
            Gen1 = GC.CollectionCount(1) - gen1,
            Gen2 = GC.CollectionCount(2) - gen2,
        };
    }

    private static void Settle()
    {
        for (var pass = 0; pass < 2; pass++)
        {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }
    }

    /// <summary>
    /// Opens the pattern exactly as <see cref="StreamDemuxer.Run(string, IReadOnlyDictionary{string, string}, StreamHub, CancellationToken)"/>
    /// opens a URL: the same probe budget from <see cref="LiveOptions"/>, and the container probed
    /// rather than declared, because that overload probes.
    ///
    /// Every configuration in this class goes through here, including the ones that do not use
    /// <see cref="StreamDemuxer"/> at all. An open of this file costs about twenty milliseconds
    /// against a pass of packets costing twenty thousand, so two rows that open differently differ
    /// by around a microsecond a packet before either has read anything - which, in a comparison
    /// whose whole subject is a few microseconds a packet, would be charged to the wrong thing.
    /// </summary>
    private static void OpenLikeProduction(string pattern, AVFormatContext** format)
    {
        AVDictionary* options = null;

        try
        {
            ffmpeg.av_dict_set(
                &options,
                "analyzeduration",
                ((long)(ProbeDefaults.ProbeSeconds * 1_000_000)).ToString(CultureInfo.InvariantCulture),
                0);

            ffmpeg.av_dict_set(
                &options,
                "probesize",
                ProbeDefaults.ProbeBytes.ToString(CultureInfo.InvariantCulture),
                0);

            if (ffmpeg.avformat_open_input(format, pattern, null, &options) < 0)
            {
                throw new InvalidOperationException($"Could not open '{pattern}'.");
            }

            if (ffmpeg.avformat_find_stream_info(*format, null) < 0)
            {
                throw new InvalidOperationException($"'{pattern}' carries no stream information.");
            }
        }
        finally
        {
            ffmpeg.av_dict_free(&options);
        }
    }

    /// <summary>
    /// libav's own cost, and optionally the packet copy the pump makes on top of it. No hub, so
    /// nothing retains a packet and every array dies in gen0.
    /// </summary>
    private static Run WithoutHub(string pattern, int passes, bool copy)
    {
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        long packets = 0;
        long bytes = 0;
        long large = 0;
        long largest = 0;

        // Kept alive across the loop so the copy cannot be optimised away as a dead store, and so
        // one packet's array is still live while the next is allocated - which is what the real
        // pump does, since the hub is holding the previous one when the next arrives.
        byte[]? held = null;

        for (var pass = 0; pass < passes; pass++)
        {
            AVFormatContext* format = null;
            AVPacket* packet = null;

            try
            {
                OpenLikeProduction(pattern, &format);

                packet = ffmpeg.av_packet_alloc();

                while (ffmpeg.av_read_frame(format, packet) >= 0)
                {
                    packets++;
                    bytes += packet->size;
                    largest = Math.Max(largest, packet->size);

                    // Whether any packet reaches the large object heap, which a pool would have to
                    // answer for separately: an array this size is not collected with the rest.
                    if (packet->size >= 85_000)
                    {
                        large++;
                    }

                    if (copy)
                    {
                        var data = new byte[packet->size];
                        Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);
                        held = data;
                    }

                    ffmpeg.av_packet_unref(packet);
                }
            }
            finally
            {
                if (packet is not null)
                {
                    ffmpeg.av_packet_free(&packet);
                }

                if (format is not null)
                {
                    ffmpeg.avformat_close_input(&format);
                }
            }
        }

        GC.KeepAlive(held);

        return new Run(packets, bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, large)
        {
            Exact = bytes,
            Held = copy ? bytes : 0,
            Largest = largest,
        };
    }

    /// <summary>
    /// The production path, reading the file through libav's own file protocol. One hub across every
    /// pass, so the buffer reaches the steady state it runs in rather than filling once.
    /// </summary>
    private static Run Full(string pattern, int passes) => WithHub(
        pattern,
        passes,
        (demuxer, path, hub) => demuxer.Run(path, null, hub, CancellationToken.None));

    /// <summary>
    /// <see cref="StreamDemuxer"/>'s loop, duplicated so that the one line obtaining the array can be
    /// swapped without a seam existing in production code for a measurement's sake. Everything else -
    /// the open, the layout, the rescale, the hub, the buffer - is what the pump does.
    ///
    /// <see cref="Allocation.Zeroed"/> is the pump as it stands, and exists to be compared against
    /// <c>full</c>: if the two disagree, this loop is not a faithful copy and the other two rows mean
    /// nothing. The comparisons that answer the question are then between the three variants, which
    /// differ from each other in one line and in nothing else at all.
    ///
    /// <see cref="Allocation.Pooled"/> returns the array the moment the hub has taken it - while the
    /// rolling buffer and every subscriber are still holding it, which in a real service is the
    /// corruption this whole question is about. That is deliberate: it is what makes this an upper
    /// bound. A pool that is returned to immediately never starves, never counts a reference and
    /// never waits for the last holder, so whatever separates it from <see cref="Allocation.Zeroed"/>
    /// is strictly more than a correct pool could give back, not an estimate of it. The bucket
    /// rounding is real though - the rented array is the next size up the pool keeps - so what the
    /// buffer is holding is what a pooled packet would really occupy.
    ///
    /// <see cref="Allocation.Uninitialized"/> is the one of the three that could actually ship: an
    /// array of exactly the packet's size, not zeroed, fully overwritten by the copy on the next line
    /// before anything can see it. No reference count, no oversize, no change of contract.
    /// </summary>
    private static Run Variant(string pattern, int passes, Allocation allocation)
    {
        var pool = ArrayPool<byte>.Shared;
        var options = new LiveOptions();

        using var hub = new StreamHub("cost", options, NullLogger.Instance);

        var allocated = GC.GetAllocatedBytesForCurrentThread();
        long exact = 0;
        long held = 0;

        for (var pass = 0; pass < passes; pass++)
        {
            AVFormatContext* format = null;
            AVPacket* packet = null;

            try
            {
                OpenLikeProduction(pattern, &format);

                var layout = StreamLayout.From(format);

                hub.Adopt(layout);

                var reference = layout.ReferenceTimeBase;
                var lastReferencePts = 0L;

                packet = ffmpeg.av_packet_alloc();

                while (ffmpeg.av_read_frame(format, packet) >= 0)
                {
                    try
                    {
                        if (packet->stream_index < 0 || packet->stream_index >= layout.Count)
                        {
                            continue;
                        }

                        // The one line that differs. The switch itself runs in all three
                        // configurations, so it cannot favour any of them.
                        var data = allocation switch
                        {
                            Allocation.Zeroed => new byte[packet->size],
                            Allocation.Uninitialized => GC.AllocateUninitializedArray<byte>(packet->size),
                            _ => pool.Rent(packet->size),
                        };

                        Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);

                        exact += packet->size;
                        held += data.Length;

                        if (packet->pts != ffmpeg.AV_NOPTS_VALUE)
                        {
                            lastReferencePts = ffmpeg.av_rescale_q(
                                packet->pts,
                                layout.TimeBase(packet->stream_index),
                                reference);
                        }

                        hub.Publish(
                            new MediaPacket(
                                packet->stream_index,
                                data,
                                packet->pts,
                                packet->dts,
                                packet->duration,
                                (packet->flags & ffmpeg.AV_PKT_FLAG_KEY) != 0),
                            lastReferencePts);

                        if (allocation == Allocation.Pooled)
                        {
                            // See this method's summary: returning here is wrong for a service and
                            // right for an upper bound.
                            pool.Return(data);
                        }
                    }
                    finally
                    {
                        ffmpeg.av_packet_unref(packet);
                    }
                }
            }
            finally
            {
                if (packet is not null)
                {
                    ffmpeg.av_packet_free(&packet);
                }

                if (format is not null)
                {
                    ffmpeg.avformat_close_input(&format);
                }
            }
        }

        return new Run(hub.Packets, hub.Bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, Large: -1)
        {
            Exact = exact,
            Held = held,
        };
    }

    /// <summary>
    /// The production path reading through <see cref="AvioReader"/> over a stream that answers in
    /// chunks of at most <paramref name="chunk"/> bytes, which is how an SRT ingest reads.
    /// </summary>
    private static Run ThroughAvio(string pattern, int passes, int chunk) => WithHub(
        pattern,
        passes,
        (demuxer, path, hub) =>
        {
            using var reader = new AvioReader(new ChunkedFileStream(path, chunk));

            return demuxer.Run(reader.Context, hub, CancellationToken.None);
        });

    private static Run WithHub(string pattern, int passes, Func<StreamDemuxer, string, StreamHub, DemuxOutcome> read)
    {
        var options = new LiveOptions();
        var demuxer = new StreamDemuxer(Options.Create(options), NullLogger<StreamDemuxer>.Instance);

        using var hub = new StreamHub("cost", options, NullLogger.Instance);

        var allocated = GC.GetAllocatedBytesForCurrentThread();

        for (var pass = 0; pass < passes; pass++)
        {
            var outcome = read(demuxer, pattern, hub);

            if (outcome != DemuxOutcome.FeedEnded)
            {
                throw new InvalidOperationException($"The pattern demultiplexed as {outcome}.");
            }
        }

        // Large-object packets are not counted here: the hub does not keep a packet's size to
        // itself and asking it for one would mean a public member that exists for a benchmark. The
        // floor reads the same file and reports them.
        return new Run(hub.Packets, hub.Bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, Large: -1)
        {
            Exact = hub.Bytes,
            Held = hub.Bytes,
        };
    }

    /// <summary>
    /// A file read in chunks of a fixed size, standing in for what libsrt hands up: whole messages
    /// of at most the socket's payload size, one per read, however large a buffer it is given.
    /// </summary>
    private sealed class ChunkedFileStream(string path, int chunk) : Stream
    {
        private readonly FileStream _file = File.OpenRead(path);

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => _file.Length;

        public override long Position
        {
            get => _file.Position;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
            => _file.Read(buffer[..Math.Min(buffer.Length, chunk)]);

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            _file.Dispose();

            base.Dispose(disposing);
        }
    }
}
