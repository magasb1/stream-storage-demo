namespace StorageDemo.Core.Documents;

/// <summary>Broadcasts document changes so clients do not have to poll.</summary>
public interface IChangeFeed
{
    void Publish(DocumentChange change);

    /// <summary>Registers immediately, before anything is read.</summary>
    IChangeSubscription Subscribe();
}

public interface IChangeSubscription : IDisposable
{
    IAsyncEnumerable<DocumentChange> ReadAllAsync(CancellationToken cancellationToken);
}
