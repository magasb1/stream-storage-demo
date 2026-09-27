namespace StorageDemo.Infrastructure.Monitoring;

/// <summary>
/// Tells the monitor that something in the store probably changed, so it can rescan now instead of
/// at the next tick.
/// </summary>
public sealed class StorageChangeSignal
{
    private readonly SemaphoreSlim _signal = new(0, 1);

    /// <summary>Coalescing: a hundred events between two passes still mean one pass.</summary>
    public void Trigger()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    /// <summary>True when something signalled, false when the wait timed out.</summary>
    public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        => _signal.WaitAsync(timeout, cancellationToken);
}
