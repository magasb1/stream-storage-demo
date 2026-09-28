using System.Buffers;
using System.Diagnostics;
using System.Globalization;
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
/// part of it. A rig run cannot answer that: every thread there is scheduler-bound, so a change
/// worth a microsecond or two per packet disappears into the noise of four contended cores.
///
/// Off a file rather than off a socket, deliberately. The same picture the scale rig pushes
/// (<see cref="Picture"/> at <see cref="Bitrate"/>) is rendered once and demultiplexed as fast as
/// the processor allows, so what is measured is libav's demuxer, the copy out of it and the hub -
/// everything the per-stream thread does - with the transport, the pacing and the contention taken
/// out. The figures are a cost per packet and not a throughput, and they are not comparable to a
/// rig row; they are comparable to each other, which is the whole point.
///
/// Six configurations, so the allocation can be separated from what surrounds it:
/// <list type="bullet">
/// <item><c>floor</c> - <c>av_read_frame</c> and nothing else. libav's own cost.</item>
/// <item><c>copy</c> - the floor plus the per-packet <c>byte[]</c> and the copy into it, retained by
/// nobody. The difference from the floor is what the allocation costs when it dies immediately.</item>
/// <item><c>full</c> - the real <see cref="StreamDemuxer"/> into a real <see cref="StreamHub"/> with
/// the production thirty-second buffer and no subscribers, which is the ingest-only shape the rig's
/// collapsed row was measured in. The difference from <c>copy</c> is the hub, the buffer, and the
/// collector having to promote every packet the buffer retains rather than dropping it in gen0 -
/// which is the part of the allocation's cost that a per-packet stopwatch would never see.</item>
/// <item><c>pool-max</c> - <c>full</c> with the allocation gone and nothing correct in its place, so
/// the difference between the two is the whole of what pooling could ever give back. See
/// <see cref="PooledCeiling"/> for why it is an upper bound rather than a prototype.</item>
/// <item><c>avio-64k</c> and <c>avio-1316</c> - the same full pump reading through
/// <see cref="AvioReader"/> over a stream that answers in at most that many bytes. 1316 is libsrt's
/// default payload size and therefore the granularity a real ingest actually reads at, because
/// libsrt delivers whole messages and <see cref="SrtSocketStream"/> passes one up per read. The
/// difference between the two is what that granularity costs on this side of the P/Invoke; it does
/// not include libsrt's own cost per message, which needs a socket and is not measured here.</item>
/// </list>
///
/// Processor time rather than wall clock, taken from the whole process so the collector's own
/// threads are counted: the retention is what makes the allocation expensive, and a metric that
/// stopped at the demultiplexing thread's own stack would miss exactly that. It also means this can
/// be run on a box with other work on it, which wall clock could not. Enough passes
/// over the pattern to allocate something over a gigabyte, because at one pass nothing is collected
/// at all and a configuration that is never collected says nothing about collection.
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

            foreach (var (name, run) in Configurations())
            {
                for (var replicate = 0; replicate < Replicates; replicate++)
                {
                    Report(Measure(name, pattern, run).Describe(name));
                }
            }
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
        ("pool-max", path => PooledCeiling(path, Passes)),
        ("avio-64k", path => ThroughAvio(path, Passes, 64 * 1024)),
        ("avio-1316", path => ThroughAvio(path, Passes, Srt.LiveDefaultPayloadSize)),
    ];

    /// <summary>One configuration's reading.</summary>
    private readonly record struct Run(long Packets, long Bytes, long Allocated, long Large)
    {
        public TimeSpan Processor { get; init; }

        public TimeSpan Paused { get; init; }

        public int Gen0 { get; init; }

        public int Gen1 { get; init; }

        public int Gen2 { get; init; }

        public string Describe(string name) => string.Format(
            CultureInfo.InvariantCulture,
            "{0,-10} {1,7} pkt  {2,6:0.0} GiB  {3,7:0.0} B/pkt alloc  {4,7:0.00} us/pkt cpu"
                + "  gc {5,4}/{6,3}/{7,2}  paused {8,7:0.0} ms ({9,4:0.0} % of cpu)  {10} over 85 KiB",
            name,
            Packets,
            Bytes / 1024.0 / 1024 / 1024,
            Packets == 0 ? 0 : Allocated / (double)Packets,
            Packets == 0 ? 0 : Processor.TotalMicroseconds / Packets,
            Gen0,
            Gen1,
            Gen2,
            Paused.TotalMilliseconds,
            Processor == TimeSpan.Zero ? 0 : 100 * Paused.TotalMilliseconds / Processor.TotalMilliseconds,
            Large < 0 ? "not counted" : Large.ToString(CultureInfo.InvariantCulture));
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
        var thread = new Thread(() => measured = run(pattern), maxStackSize: 1024 * 1024) { Name = name };

        thread.Start();
        thread.Join();

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
    /// libav's own cost, and optionally the packet copy the pump makes on top of it. No hub, so
    /// nothing retains a packet and every array dies in gen0.
    /// </summary>
    private static Run WithoutHub(string pattern, int passes, bool copy)
    {
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        long packets = 0;
        long bytes = 0;
        long large = 0;

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
                if (ffmpeg.avformat_open_input(&format, pattern, ffmpeg.av_find_input_format("mpegts"), null) < 0)
                {
                    throw new InvalidOperationException($"Could not open '{pattern}'.");
                }

                if (ffmpeg.avformat_find_stream_info(format, null) < 0)
                {
                    throw new InvalidOperationException($"'{pattern}' carries no stream information.");
                }

                packet = ffmpeg.av_packet_alloc();

                while (ffmpeg.av_read_frame(format, packet) >= 0)
                {
                    packets++;
                    bytes += packet->size;

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

        return new Run(packets, bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, large);
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
    /// The best a pool could ever do, which is the figure a proposal to build one has to beat.
    ///
    /// It is <see cref="StreamDemuxer"/>'s loop with <c>new byte[]</c> replaced by
    /// <see cref="ArrayPool{T}"/> and the array returned the moment the hub has taken it - while the
    /// rolling buffer and every subscriber are still holding it, which in a real service is the
    /// corruption this whole question is about. That is deliberate: it is what makes this an upper
    /// bound. A pool that is returned to immediately never starves, never counts a reference and
    /// never waits for the last holder, so whatever separates this row from <c>full</c> is strictly
    /// more than a correct pool could give back, not an estimate of it. The bucket rounding is real
    /// though - the rented array is the next power of two up - so the buffer accounts for the bytes
    /// a pooled packet would really occupy.
    ///
    /// The loop is duplicated here rather than reached through a seam on the pump, because a seam
    /// would be production code existing for a measurement, and this is the measurement that decides
    /// whether any production code should change at all.
    /// </summary>
    private static Run PooledCeiling(string pattern, int passes)
    {
        var pool = ArrayPool<byte>.Shared;
        var options = new LiveOptions();

        using var hub = new StreamHub("cost", options, NullLogger.Instance);

        var allocated = GC.GetAllocatedBytesForCurrentThread();

        for (var pass = 0; pass < passes; pass++)
        {
            AVFormatContext* format = null;
            AVPacket* packet = null;

            try
            {
                if (ffmpeg.avformat_open_input(&format, pattern, ffmpeg.av_find_input_format("mpegts"), null) < 0
                    || ffmpeg.avformat_find_stream_info(format, null) < 0)
                {
                    throw new InvalidOperationException($"Could not open '{pattern}'.");
                }

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

                        var data = pool.Rent(packet->size);
                        Marshal.Copy((IntPtr)packet->data, data, 0, packet->size);

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

                        // See this method's summary: returning here is wrong for a service and right
                        // for an upper bound.
                        pool.Return(data);
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

        return new Run(hub.Packets, hub.Bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, Large: -1);
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
        return new Run(hub.Packets, hub.Bytes, GC.GetAllocatedBytesForCurrentThread() - allocated, Large: -1);
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
