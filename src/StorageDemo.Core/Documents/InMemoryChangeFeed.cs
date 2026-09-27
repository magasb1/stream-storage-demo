using System.Collections.Concurrent;
using System.Threading.Channels;

namespace StorageDemo.Core.Documents;

/// <summary>Fan-out inside one process.</summary>
public sealed class InMemoryChangeFeed : IChangeFeed
{
    private readonly ConcurrentDictionary<Guid, Channel<DocumentChange>> _subscribers = new();

    public void Publish(DocumentChange change)
    {
        foreach (var subscriber in _subscribers.Values)
        {
            subscriber.Writer.TryWrite(change);
        }
    }

    public IChangeSubscription Subscribe()
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateBounded<DocumentChange>(new BoundedChannelOptions(256)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
        });

        _subscribers[id] = channel;

        return new Subscription(channel, () => _subscribers.TryRemove(id, out _));
    }
}

file sealed class Subscription(Channel<DocumentChange> channel, Action unsubscribe) : IChangeSubscription
{
    public IAsyncEnumerable<DocumentChange> ReadAllAsync(CancellationToken cancellationToken)
        => channel.Reader.ReadAllAsync(cancellationToken);

    public void Dispose() => unsubscribe();
}
