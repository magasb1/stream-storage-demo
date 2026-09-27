using System.Diagnostics;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Streaming;
using StorageDemo.Tests.Infrastructure;

namespace StorageDemo.Tests.Integration;

/// <summary>
/// What a viewer that will not take the bytes costs this replica.
///
/// The relay route is the one every viewer that reached the wrong pod is served through, and it is
/// the one that used to write to a viewer synchronously: a consumer that stops draining did not
/// merely fall behind, it held the thread that was writing to it, one thread-pool worker per slow
/// viewer with nothing bounding how many. That is a ceiling on viewers per pod measured in threads
/// rather than in bandwidth, and it was reached at two hundred of them.
///
/// Cheap on purpose: one stream, a couple of dozen readers that pause between reads, and a
/// measurement of what the process is holding while they do. The scale rig measures the same thing
/// at five hundred readers and takes minutes; this asks the one question that has an answer either
/// way in about twenty seconds.
/// </summary>
public sealed class LiveSlowViewerTests(ITestOutputHelper output) : IAsyncLifetime
{
    /// <summary>
    /// How many readers pause between reads. Enough that a thread each would stand out well past
    /// anything else this process is doing, few enough that the whole test is one stream and a
    /// handful of seconds.
    /// </summary>
    private const int Readers = 24;

    /// <summary>
    /// How long each read pauses for, which is what makes these readers slow: the stream sends
    /// about seventy-five kilobytes a second and a reader taking sixty-four of them every three is
    /// a fifth of that. The figure the measurements in the issue were taken at.
    /// </summary>
    private static readonly TimeSpan Slowly = TimeSpan.FromSeconds(3);

    /// <summary>
    /// How long the readers are left stalled before the process is asked what it is holding. The
    /// thread pool injects roughly a worker or two a second once its own are all blocked, so this
    /// has to be long enough for that to be visible rather than pending.
    /// </summary>
    private static readonly TimeSpan Stalled = TimeSpan.FromSeconds(8);

    private readonly LiveReplicas _replicas = new();

    private int _ingest;

    public ValueTask InitializeAsync()
    {
        _ingest = SrtSenders.FreePort();

        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => _replicas.DisposeAsync();

    [Fact]
    public async Task Viewers_that_will_not_take_the_bytes_do_not_each_hold_a_thread()
    {
        Assert.SkipUnless(Srt.IsAvailable, LiveReplicas.NoLibsrt);
        Assert.SkipUnless(LiveReplicas.HasSrt(), LiveReplicas.NoFfmpegSrt);

        const string name = "slow-audience";

        var a = _replicas.Start("pod-a", _ingest);

        _replicas.Send(_ingest, name);

        await LiveReplicas.Until(
            async () => await _replicas.Registry.GetAsync(name) is { State: LiveStreamState.Live, Packets: > 0 },
            TimeSpan.FromSeconds(40),
            "the stream never went live");

        using var client = _replicas.Client(a);

        // Read after the stream is up and before a reader attaches, so the baseline includes
        // everything ingest, the heartbeat and the test host itself cost.
        var before = Threads();

        var readers = Enumerable
            .Range(0, Readers)
            .Select(_ => SlowReader.Open(client, name, Slowly))
            .ToList();

        try
        {
            await LiveReplicas.Until(
                async () => await _replicas.Registry.GetAsync(name) is { Viewers: >= Readers },
                TimeSpan.FromSeconds(20),
                $"the replica never served all {Readers} readers");

            await Task.Delay(Stalled);

            var after = Threads();

            output.WriteLine($"before: {before}");
            output.WriteLine($"after {Readers} slow readers: {after}");

            // A third of the readers, not zero. A pool that grows a worker for a reason of its own
            // while this runs is ordinary, and the failure this exists to catch is not subtle: one
            // thread per slow viewer, which at this count is twenty-four of them.
            var allowed = Readers / 3;

            Assert.True(
                after.Pool - before.Pool <= allowed,
                $"the pool grew {after.Pool - before.Pool} workers for {Readers} slow readers "
                + $"({before} then {after})");

            Assert.True(
                after.Process - before.Process <= allowed,
                $"the process grew {after.Process - before.Process} threads for {Readers} slow "
                + $"readers ({before} then {after})");

            // The bound is only worth anything if the readers were being served while it held: a
            // replica that had turned them all away would pass every assertion above.
            Assert.All(readers, reader => Assert.True(
                reader.Bytes > 0,
                $"a slow reader received nothing; it reported {reader.Fault?.Message ?? "no fault"}"));
        }
        finally
        {
            foreach (var reader in readers)
            {
                await reader.DisposeAsync();
            }
        }
    }

    private static Counts Threads()
    {
        using var self = Process.GetCurrentProcess();

        return new Counts(ThreadPool.ThreadCount, self.Threads.Count);
    }

    /// <param name="Pool">Thread-pool workers, which is what a synchronous write to a viewer takes.</param>
    /// <param name="Process">Every thread the process holds, pool workers included.</param>
    private readonly record struct Counts(int Pool, int Process)
    {
        public override string ToString() => $"{Pool} pool workers, {Process} process threads";
    }

    /// <summary>
    /// A relayed viewer on a bad network: it reads, then waits, and never catches up with what the
    /// stream is sending.
    /// </summary>
    private sealed class SlowReader : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stopping = new();

        private long _bytes;

        private SlowReader(HttpClient client, string name, TimeSpan slowly)
            => Pump = Task.Run(() => ReadAsync(client, name, slowly, _stopping.Token));

        public Task Pump { get; }

        public Exception? Fault { get; private set; }

        public long Bytes => Interlocked.Read(ref _bytes);

        public static SlowReader Open(HttpClient client, string name, TimeSpan slowly)
            => new(client, name, slowly);

        public async ValueTask DisposeAsync()
        {
            await _stopping.CancelAsync();

            try
            {
                await Pump;
            }
            catch (Exception)
            {
                // A reader torn down mid-read is how every one of these ends.
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

                    await Task.Delay(slowly, stopping);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                // Kept rather than thrown: a reader that failed is something for the assertions to
                // report, and a test that dies because one of twenty-four readers did has measured
                // nothing.
                Fault = ex;
            }
        }
    }
}
