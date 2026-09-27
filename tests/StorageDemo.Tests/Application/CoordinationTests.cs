using StorageDemo.Core.Coordination;
using StorageDemo.Core.Documents;

namespace StorageDemo.Tests.Application;

/// <summary>
/// The in-memory implementations of the two pieces that have to be shared once the service runs as
/// more than one replica.
/// </summary>
public sealed class CoordinationTests
{
    [Fact]
    public async Task A_lock_is_granted_once_and_refused_while_it_is_held()
    {
        var locks = new InMemoryLock();

        await using var first = await locks.TryAcquireAsync("scan", TimeSpan.FromMinutes(1));
        Assert.NotNull(first);

        // The second caller is told no rather than made to wait: a skipped scan is the right
        // answer.
        Assert.Null(await locks.TryAcquireAsync("scan", TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Releasing_a_lock_lets_the_next_caller_have_it()
    {
        var locks = new InMemoryLock();

        var first = await locks.TryAcquireAsync("scan", TimeSpan.FromMinutes(1));
        Assert.NotNull(first);
        await first.DisposeAsync();

        await using var second = await locks.TryAcquireAsync("scan", TimeSpan.FromMinutes(1));
        Assert.NotNull(second);
    }

    [Fact]
    public async Task Different_names_do_not_block_each_other()
    {
        var locks = new InMemoryLock();

        await using var scan = await locks.TryAcquireAsync("scan", TimeSpan.FromMinutes(1));
        await using var other = await locks.TryAcquireAsync("something-else", TimeSpan.FromMinutes(1));

        Assert.NotNull(scan);
        Assert.NotNull(other);
    }

    [Fact]
    public async Task Queued_work_comes_back_in_the_order_it_went_in()
    {
        var queue = new InMemoryAnalysisQueue();

        await queue.EnqueueAsync(Request("first"));
        await queue.EnqueueAsync(Request("second"));

        var received = await TakeAsync(queue, 2);

        Assert.Equal(["first", "second"], received.Select(r => r.FileName).ToArray());
    }

    [Fact]
    public async Task Each_item_is_handed_to_one_reader_only()
    {
        var queue = new InMemoryAnalysisQueue();
        await queue.EnqueueAsync(Request("only-once"));

        var received = await TakeAsync(queue, 1);
        Assert.Single(received);

        // Nothing is left for a second reader, which is what stops two replicas doing the same
        // work.
        Assert.Empty(await TakeAsync(queue, 1, TimeSpan.FromMilliseconds(150)));
    }

    private static AnalysisRequest Request(string fileName)
        => new(Guid.NewGuid(), $"documents/{fileName}", fileName, "video/mp4");

    /// <summary>Reads up to a count, giving up after a short wait so a test cannot hang.</summary>
    private static async Task<List<AnalysisRequest>> TakeAsync(
        IAnalysisQueue queue,
        int count,
        TimeSpan? timeout = null)
    {
        using var window = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        var received = new List<AnalysisRequest>();

        try
        {
            await foreach (var request in queue.DequeueAllAsync(window.Token))
            {
                received.Add(request);

                if (received.Count == count)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }

        return received;
    }
}
