using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Messaging;

/// <summary>
/// Fan-out through Redis pub/sub, so every replica sees every change and a client connected to one
/// replica still hears about an upload handled by another.
/// </summary>
public sealed class RedisChangeFeed(
    IConnectionMultiplexer connection,
    IOptions<MessagingOptions> options,
    ILogger<RedisChangeFeed> logger) : IChangeFeed
{
    private readonly RedisChannel _channel = RedisChannel.Literal(options.Value.Redis.Channel);

    public void Publish(DocumentChange change)
    {
        try
        {
            connection.GetSubscriber().Publish(_channel, JsonSerializer.Serialize(change));
        }
        catch (RedisException ex)
        {
            // A change that nobody hears about costs a client a stale tile until its next refresh.
            logger.LogWarning(ex, "Could not publish a change to Redis");
        }
    }

    public IChangeSubscription Subscribe()
        => new Subscription(connection.GetSubscriber().Subscribe(_channel), logger);

    private sealed class Subscription(ChannelMessageQueue queue, ILogger logger) : IChangeSubscription
    {
        public async IAsyncEnumerable<DocumentChange> ReadAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ChannelMessage message;
                try
                {
                    message = await queue.ReadAsync(cancellationToken);
                }
                catch (ChannelClosedException)
                {
                    yield break;
                }

                DocumentChange? change = null;
                try
                {
                    change = JsonSerializer.Deserialize<DocumentChange>(message.Message.ToString());
                }
                catch (JsonException ex)
                {
                    // Another version of the application, or something else on the channel.
                    logger.LogWarning(ex, "Ignored an unreadable change message");
                }

                if (change is not null)
                {
                    yield return change;
                }
            }
        }

        public void Dispose() => queue.Unsubscribe();
    }
}
