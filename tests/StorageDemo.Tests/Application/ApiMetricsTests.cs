using StorageDemo.Api.Observability;

namespace StorageDemo.Tests.Application;

/// <summary>What the request surfaces publish that the framework does not publish for them.</summary>
public sealed class ApiMetricsTests
{
    [Fact]
    public void An_open_server_stream_is_counted_while_it_is_open_and_not_afterwards()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        var stream = metrics.Streaming(ServerStreams.LiveStreams);

        stream.Sent();
        stream.Sent();

        Assert.Equal(1, meters.Total("api.grpc.streams.active"));
        Assert.Equal(2, meters.Total("api.grpc.stream.messages"));
        Assert.Equal(
            [new KeyValuePair<string, object?>("rpc", "WatchLiveStreams")],
            meters.Of("api.grpc.stream.messages")[0].Tags);

        stream.Dispose();

        // The up-down counter's measurements sum to zero once the call has ended, which is what a
        // reader sees as "nothing open".
        Assert.Equal(0, meters.Total("api.grpc.streams.active"));
    }

    /// <summary>
    /// An up-down counter has no way back from a double decrement: one handle disposed twice would
    /// leave the replica reporting fewer open streams than zero for as long as the process ran, and
    /// a <c>using</c> inside a method that also returns the handle is an easy way to do it.
    /// </summary>
    [Fact]
    public void Disposing_a_counted_stream_twice_does_not_take_the_count_negative()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        var stream = metrics.Streaming(ServerStreams.Changes);

        stream.Dispose();
        stream.Dispose();

        Assert.Equal(0, meters.Total("api.grpc.streams.active"));
    }

    [Fact]
    public void Uploads_carry_the_surface_and_the_outcome_and_their_size()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        metrics.Uploaded("grpc", "stored", 4096);

        Assert.Equal(
            [
                new KeyValuePair<string, object?>("surface", "grpc"),
                new KeyValuePair<string, object?>("outcome", "stored"),
            ],
            meters.Tags("api.documents.uploads"));

        Assert.Equal(4096, meters.Total("api.documents.upload.bytes"));
    }

    /// <summary>
    /// A rejected upload is still an upload attempt and still has a surface, and it has no size to
    /// report - which must be no measurement rather than a zero, or the byte counter would carry a
    /// series that only ever says nothing happened.
    /// </summary>
    [Fact]
    public void A_rejected_upload_is_counted_without_a_size()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        metrics.Uploaded("rest", "rejected", 0);

        Assert.Equal(1, meters.Total("api.documents.uploads"));
        Assert.Empty(meters.Of("api.documents.upload.bytes"));
    }

    /// <summary>
    /// The three things this surface serves are one instrument with a tag, because a wall of a
    /// thousand live tiles and a thousand file downloads are nothing like the same load and would
    /// otherwise be one number.
    /// </summary>
    [Fact]
    public void Downloads_tell_a_document_a_thumbnail_and_a_preview_apart()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        metrics.Downloaded("grpc", "document", "served");
        metrics.Downloaded("grpc", "thumbnail", "missing");
        metrics.Downloaded("rest", "preview", "served");
        metrics.DownloadedBytes("rest", "preview", 12_000);

        Assert.Equal(
            ["document", "thumbnail", "preview"],
            meters.Of("api.documents.downloads")
                .Select(recording => recording.Tags.Single(tag => tag.Key == "kind").Value)
                .ToArray());

        Assert.Equal(12_000, meters.Total("api.documents.download.bytes"));
    }

    [Fact]
    public void A_forwarded_call_carries_which_hop_it_was_and_how_it_went()
    {
        using var metrics = new ApiMetrics();
        using var meters = new Meters(metrics);

        metrics.Peered("media", "unreachable");

        Assert.Equal(
            [
                new KeyValuePair<string, object?>("kind", "media"),
                new KeyValuePair<string, object?>("outcome", "unreachable"),
            ],
            meters.Tags("api.peer.calls"));
    }
}
