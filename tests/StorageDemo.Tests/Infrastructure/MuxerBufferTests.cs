using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The buffer a viewer's muxer writes into, and the array it borrows to do it.
///
/// Worth its own tests because of what it holds rather than what it does: the array is rented from
/// the shared pool, and the whole safety of that arrangement is that nothing writes to it after it
/// has been handed back. libav writes through this buffer from inside its own callback, so the
/// failure it protects against is one viewer's bytes landing in an array another request has since
/// been given - which would be a silent wrong answer rather than a crash.
/// </summary>
public sealed class MuxerBufferTests
{
    [Fact]
    public async Task What_was_written_is_drained_once_and_in_order()
    {
        using var buffer = new MuxerBuffer();
        using var destination = new MemoryStream();

        buffer.Write("head"u8);
        buffer.Write("tail"u8);

        Assert.Equal(8, buffer.Pending);

        await buffer.DrainToAsync(destination, TestContext.Current.CancellationToken);

        Assert.Equal("headtail", System.Text.Encoding.UTF8.GetString(destination.ToArray()));

        // Drained means emptied: a second drain of the same packet's bytes would send them twice.
        Assert.Equal(0, buffer.Pending);

        await buffer.DrainToAsync(destination, TestContext.Current.CancellationToken);

        Assert.Equal(8, destination.Length);
    }

    /// <summary>
    /// A keyframe at a contribution bitrate is bigger than libav's own write buffer, so the array
    /// has to grow, and what was already in it has to survive the growth.
    /// </summary>
    [Fact]
    public async Task A_packet_larger_than_the_buffer_grows_it_and_keeps_what_was_there()
    {
        using var buffer = new MuxerBuffer();
        using var destination = new MemoryStream();

        var large = new byte[512 * 1024];

        Array.Fill(large, (byte)7);

        buffer.Write("head"u8);
        buffer.Write(large);

        await buffer.DrainToAsync(destination, TestContext.Current.CancellationToken);

        var written = destination.ToArray();

        Assert.Equal(4 + large.Length, written.Length);
        Assert.Equal("head", System.Text.Encoding.UTF8.GetString(written, 0, 4));
        Assert.All(written[4..], byte_ => Assert.Equal(7, byte_));
    }

    /// <summary>
    /// The array goes back to the pool on disposal, so a write afterwards has to be refused rather
    /// than land in whatever rented it next. Nothing in the service can reach this - a viewer's
    /// buffer is declared before the muxer that writes through it, so it outlives the writer - but
    /// the rule the declaration order relies on is worth having stated somewhere that fails.
    /// </summary>
    [Fact]
    public async Task A_disposed_buffer_refuses_the_bytes_rather_than_taking_them()
    {
        var buffer = new MuxerBuffer();
        using var destination = new MemoryStream();

        buffer.Write("some"u8);
        buffer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => buffer.Write("more"u8));

        await Assert.ThrowsAsync<ObjectDisposedException>(
            async () => await buffer.DrainToAsync(destination, TestContext.Current.CancellationToken));

        // Disposing twice is what an exception on the way out of a viewer does, and the array can
        // only be returned to the pool once.
        buffer.Dispose();

        Assert.Equal(0, destination.Length);
    }
}
