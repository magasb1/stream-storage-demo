using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The valve in front of libav: how many calls may be inside the analyzer at once.
///
/// Counted rather than timed. Every call is handed a stream that announces itself as the analyzer
/// starts reading it and then holds, so what the analyzer admitted while all of them were held is a
/// number and not an interval. Two hundred unbounded snapshots on four cores answered in under a
/// second, which is why a test that watched the clock would have proved nothing.
///
/// It observes the gate at the spill, the first thing inside it. A bound narrowed to the decode
/// alone would fail these, which is deliberate: the temp file is part of what the width costs.
///
/// What it cannot show is that nothing else in the process decodes - a live stream's own preview is
/// harvested outside this gate entirely - or that one per processor is the right width for any
/// particular deployment. It shows that the number the options ask for is the number that runs, and
/// that a call which waited still finishes.
/// </summary>
public sealed class MediaAnalyzerConcurrencyTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public Task No_more_calls_are_inside_libav_at_once_than_the_options_allow(int limit)
        => AdmitsAsync(new MediaOptions { MaxConcurrentDecodes = limit }, expected: limit, callers: 8);

    /// <summary>
    /// One per processor, which is what a deployment that configures nothing gets, and never fewer
    /// than two however few processors it was given.
    /// </summary>
    [Fact]
    public Task A_deployment_that_configures_nothing_decodes_one_call_per_processor()
    {
        var expected = Math.Max(2, Environment.ProcessorCount);

        return AdmitsAsync(new MediaOptions(), expected, callers: expected * 2);
    }

    /// <summary>
    /// Starts <paramref name="callers"/> calls at once, each of which stops inside the analyzer, and
    /// asserts that exactly <paramref name="expected"/> of them got that far.
    ///
    /// Waiting for the gate to fill needs no clock: a call that is admitted announces itself before
    /// it can block, so the last admission has already happened by the time the calls are all
    /// started. Proving that nothing further got in does need one, because it is an absence. If the
    /// gate never fills at all - what a libav that would not load looks like - the calls themselves
    /// are awaited for their own exception rather than left to run that clock down.
    ///
    /// Everyone is then let go and has to finish. A permit that leaked would leave the rest waiting
    /// on one that never comes, so that wait is the assertion that the gate opens again as well as
    /// closing.
    /// </summary>
    private static async Task AdmitsAsync(MediaOptions options, int expected, int callers)
    {
        var analyzer = new LibavMediaAnalyzer(
            Options.Create(options),
            NullLogger<LibavMediaAnalyzer>.Instance);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var filled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var overflowed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var inside = 0;

        void Announce()
        {
            var now = Interlocked.Increment(ref inside);

            if (now == expected)
            {
                filled.TrySetResult();
            }
            else if (now > expected)
            {
                overflowed.TrySetResult();
            }
        }

        var calls = Enumerable
            .Range(0, callers)
            .Select(_ => analyzer.LatestFrameAsync(
                new AnnouncingStream(Announce, release.Task),
                "snapshot.ts"))
            .ToArray();

        var answered = Task.WhenAll(calls);

        if (await Task.WhenAny(filled.Task, answered).WaitAsync(TimeSpan.FromSeconds(30)) != filled.Task)
        {
            // Nothing is holding the gate and yet it never filled, so every call answered without
            // ever reading its stream. Awaiting them reports why - a libav that would not load
            // throws here - rather than failing on a count that is only the symptom of it.
            await answered;

            Assert.Fail($"only {Volatile.Read(ref inside)} of {callers} calls reached the analyzer");
        }

        // The one wait with a clock in it, because a bound is an absence: a call that should be
        // queued behind the held ones has this long to slip past them. Generous rather than tuned -
        // with no bound at all, every caller is admitted before the first of them can block, so this
        // has already failed by the time it is reached.
        var escaped = await Task.WhenAny(overflowed.Task, Task.Delay(TimeSpan.FromSeconds(1)));

        Assert.False(
            escaped == overflowed.Task,
            $"{Volatile.Read(ref inside)} calls were inside the analyzer at once, not {expected}");

        release.SetResult();

        await answered.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(callers, Volatile.Read(ref inside));
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
