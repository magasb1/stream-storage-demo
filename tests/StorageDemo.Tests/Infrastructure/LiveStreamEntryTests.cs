using Microsoft.Extensions.Logging.Abstractions;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The arithmetic behind a stream's contribution to the meter.
///
/// It is the one part of the measurement that can be wrong rather than merely absent: the hub keeps
/// running totals, because that is what survives a reconnect, and a counter wants the interval. Add
/// a total every beat and a stream that ran for an hour reports its traffic eighteen hundred times.
/// </summary>
public sealed class LiveStreamEntryTests
{
    [Fact]
    public async Task What_a_stream_carried_is_reported_as_an_interval_rather_than_a_total()
    {
        await using var entry = Entry(out var hub);

        Publish(hub, packets: 2, bytes: 3);

        var first = entry.TakeFeed(lost: 4, dropped: 2);

        Assert.Equal(2, first.Packets);
        Assert.Equal(6, first.Bytes);

        // The transport's own figures pass straight through: libsrt cleared them when the caller
        // read them, so they are already an interval and are not this class's to difference.
        Assert.Equal(4, first.PacketsLost);
        Assert.Equal(2, first.PacketsDropped);

        Publish(hub, packets: 1, bytes: 3);

        var second = entry.TakeFeed(0, 0);

        Assert.Equal(1, second.Packets);
        Assert.Equal(3, second.Bytes);
    }

    /// <summary>
    /// Describing a stream twice inside one beat is ordinary - a claim describes it, and so does a
    /// manual creation - and it must not count the same packets twice. Nothing arrived in between,
    /// so the second reading is nothing.
    /// </summary>
    [Fact]
    public async Task Asking_twice_in_one_beat_reports_nothing_the_second_time()
    {
        await using var entry = Entry(out var hub);

        Publish(hub, packets: 5, bytes: 10);

        Assert.Equal(5, entry.TakeFeed(0, 0).Packets);

        var again = entry.TakeFeed(0, 0);

        Assert.Equal(0, again.Packets);
        Assert.Equal(0, again.Bytes);
        Assert.Equal(0, again.KlvPackets);
    }

    private static LiveStreamEntry Entry(out StreamHub hub)
    {
        hub = new StreamHub("metered", new LiveOptions(), NullLogger.Instance);

        // No layout is ever adopted, so the decoder and the KLV extractor both sit in their waiting
        // loop and nothing here touches libav.
        return new LiveStreamEntry(
            hub,
            new Harvester(320, 4, TimeSpan.FromSeconds(2)),
            NullLogger.Instance,
            manual: false,
            manualUrl: null);
    }

    private static void Publish(StreamHub hub, int packets, int bytes)
    {
        for (var i = 0; i < packets; i++)
        {
            hub.Publish(new MediaPacket(0, new byte[bytes], i, i, 1, IsKeyframe: false), i);
        }
    }
}
