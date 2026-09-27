using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The valve on the blocking libav section: how many calls may be inside the analyzer at once.
///
/// Counted rather than timed. Every call is handed a stream that announces itself as the analyzer
/// starts reading it and then holds, so what the analyzer admitted while all of them were held is a
/// number and not an interval. Two hundred unbounded snapshots on four cores answered in under a
/// second, which is exactly why a test that watched the clock would have proved nothing.
///
/// It observes the gate at the spill, the first thing inside it. A bound narrowed to the decode
/// alone would fail these, which is deliberate: the temp file is part of what the width costs.
///
/// What it cannot show is that nothing else in the process decodes without coming through here, or
/// that one per processor is the right width for any particular deployment. It shows that the
/// number the options ask for is the number that runs, and that a call which waited still finishes.
/// </summary>
public sealed class MediaAnalyzerConcurrencyTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task No_more_calls_are_inside_libav_at_once_than_the_options_allow(int limit)
        => Assert.Equal(
            limit,
            await WidthAsync(new MediaOptions { MaxConcurrentDecodes = limit }, callers: 8));

    /// <summary>One per processor, which is what a deployment that configures nothing gets.</summary>
    [Fact]
    public async Task A_deployment_that_configures_nothing_decodes_one_call_per_processor()
        => Assert.Equal(
            Environment.ProcessorCount,
            await WidthAsync(new MediaOptions(), callers: Environment.ProcessorCount * 2));

    /// <summary>
    /// Starts <paramref name="callers"/> calls at once, each of which stops inside the analyzer, and
    /// answers how many of them got that far.
    ///
    /// Everyone is then let go and has to finish. A permit that leaked would leave the rest waiting
    /// on one that never comes, so that wait is the assertion that the gate opens again as well as
    /// closing.
    /// </summary>
    private static async Task<int> WidthAsync(MediaOptions options, int callers)
    {
        var analyzer = new LibavMediaAnalyzer(
            Options.Create(options),
            NullLogger<LibavMediaAnalyzer>.Instance);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inside = 0;

        var calls = Enumerable
            .Range(0, callers)
            .Select(_ => analyzer.LatestFrameAsync(
                new AnnouncingStream(() => Interlocked.Increment(ref inside), release.Task),
                "snapshot.ts"))
            .ToArray();

        // Waits for the count to stop growing rather than for a chosen moment, so an analyzer with
        // no bound is reported as the number it actually admitted rather than as a timeout. The
        // deadline is there only so that nothing arriving at all still ends the test.
        var width = 0;
        var settled = 0;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);

        while (settled < 10 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));

            var now = Volatile.Read(ref inside);

            settled = now == width && now > 0 ? settled + 1 : 0;
            width = now;
        }

        release.SetResult();

        await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(callers, Volatile.Read(ref inside));

        return width;
    }

    /// <summary>
    /// A stream that says when the analyzer begins reading it and then does not answer until it is
    /// let go.
    ///
    /// What it eventually hands over is nothing at all. A call that decodes no picture has still
    /// been through the gate, which is the whole of what is being counted, and generating real media
    /// would put the ffmpeg command line inside a test about arithmetic.
    /// </summary>
    private sealed class AnnouncingStream(Action announce, Task release) : Stream
    {
        private bool _announced;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken)
        {
            if (!_announced)
            {
                _announced = true;
                announce();

                await release.WaitAsync(cancellationToken);
            }

            return 0;
        }

        /// <summary>
        /// Nothing reads this synchronously: the spill is a <c>CopyToAsync</c>. Throwing rather than
        /// blocking on the asynchronous path, so a spill that ever became synchronous fails here
        /// instead of hanging the whole run.
        /// </summary>
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
