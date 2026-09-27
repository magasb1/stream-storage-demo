using System.Collections.Concurrent;

namespace StorageDemo.Core.Coordination;

/// <summary>
/// Mutual exclusion within one process, which is all there is to coordinate when the service runs
/// as a single instance.
/// </summary>
public sealed class InMemoryLock : IDistributedLock
{
    private readonly ConcurrentDictionary<string, byte> _held = new();

    public Task<IAsyncDisposable?> TryAcquireAsync(
        string name,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IAsyncDisposable?>(
            _held.TryAdd(name, 0) ? new Handle(() => _held.TryRemove(name, out _)) : null);

    private sealed class Handle(Action release) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            release();
            return ValueTask.CompletedTask;
        }
    }
}
