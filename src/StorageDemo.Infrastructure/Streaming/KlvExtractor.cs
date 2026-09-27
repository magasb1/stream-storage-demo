using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// The packet subscriber that is always attached, keeping a stream's MISB metadata current.
/// </summary>
public sealed class KlvExtractor(StreamHub hub, ILogger logger)
{
    /// <summary>
    /// Bounded by count, not time: 64 packets is about six seconds at the usual 10 Hz and two and a
    /// half at a per-frame 25 Hz, either of which outlasts the decode a worker is aligning to.
    /// </summary>
    private const int RingSize = 64;

    private readonly Lock _gate = new();
    private readonly KlvSample?[] _ring = new KlvSample?[RingSize];
    private int _next;
    private int _count;

    /// <summary>Whether the current layout carries a KLV stream at all.</summary>
    public bool Present => hub.Layout is { KlvIndex: >= 0 };

    public DateTimeOffset? LastPacketAt { get; private set; }

    /// <summary>Packets that were ST 0601 but failed their checksum, and were dropped.</summary>
    public long Rejected { get; private set; }

    /// <summary>Every packet this extractor has decoded, for the life of the hub.</summary>
    public long Packets { get; private set; }

    public KlvSample? Latest
    {
        get { lock (_gate) { return _count == 0 ? null : _ring[(_next - 1 + RingSize) % RingSize]; } }
    }

    /// <summary>The marking on the newest decoded packet, or null when there is none to show.</summary>
    public string? Classification => Latest?.Fields?.Classification;

    /// <summary>
    /// The synchronous sample closest to a presentation time on the reference clock, or null when
    /// the ring holds none.
    /// </summary>
    public KlvSample? Nearest(long referencePts)
    {
        lock (_gate)
        {
            KlvSample? best = null;

            foreach (var sample in _ring)
            {
                if (sample?.ReferencePts is { } pts
                    && (best is null || Math.Abs(pts - referencePts) < Math.Abs(best.ReferencePts!.Value - referencePts)))
                {
                    best = sample;
                }
            }

            return best;
        }
    }

    /// <summary>Runs until the hub closes or the token is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested && !hub.Closed)
            {
                var layout = hub.Layout;

                if (layout is null || layout.KlvIndex < 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(layout is null ? 200 : 1000), cancellationToken);
                    continue;
                }

                await ExtractAsync(layout, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "The KLV extractor for '{Name}' stopped", hub.Name);
        }
    }

    private async Task ExtractAsync(StreamLayout layout, CancellationToken cancellationToken)
    {
        // Skip-to-live: a backlog of metadata is worthless to anyone asking what the platform is
        // doing now, and a metadata stream is a few packets a second, so this never actually fills.
        using var subscription = hub.Subscribe(
            capacity: RingSize,
            OverflowPolicy.SkipToLive,
            streamIndexes: [layout.KlvIndex]);

        var timeBase = layout.TimeBase(layout.KlvIndex);

        await foreach (var packet in subscription.Packets.ReadAllAsync(cancellationToken))
        {
            if (!ReferenceEquals(hub.Layout, layout))
            {
                return;
            }

            var fields = Misb0601.Decode(packet.Data);

            if (fields is null && Misb0601.IsUasDatalink(packet.Data))
            {
                Rejected++;
                continue;
            }

            var synchronous = packet.Pts != ffmpeg.AV_NOPTS_VALUE;

            var sample = new KlvSample(
                synchronous ? ffmpeg.av_rescale_q(packet.Pts, timeBase, layout.ReferenceTimeBase) : null,
                synchronous ? KlvAlignment.PresentationTimestamp : KlvAlignment.Timestamp,
                DateTimeOffset.UtcNow,
                fields,
                packet.Data);

            lock (_gate)
            {
                Packets++;
                _ring[_next] = sample;
                _next = (_next + 1) % RingSize;
                _count = Math.Min(_count + 1, RingSize);
                LastPacketAt = sample.ReceivedAt;
            }
        }
    }
}
