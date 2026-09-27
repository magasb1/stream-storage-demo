namespace StorageDemo.Core.Coordination;

/// <summary>Mutual exclusion across every replica of the service.</summary>
public interface IDistributedLock
{
    /// <param name="ttl">
    /// How long the lock survives without being released, so a replica that dies holding it does
    /// not block the others forever.
    /// </param>
    Task<IAsyncDisposable?> TryAcquireAsync(string name, TimeSpan ttl, CancellationToken cancellationToken = default);
}
