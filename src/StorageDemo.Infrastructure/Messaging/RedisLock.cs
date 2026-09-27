using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StorageDemo.Core.Coordination;

namespace StorageDemo.Infrastructure.Messaging;

/// <summary>
/// The usual Redis lock: SET NX PX to take it, and a compare-and-delete script to release it.
/// </summary>
public sealed class RedisLock(IConnectionMultiplexer connection, ILogger<RedisLock> logger) : IDistributedLock
{
    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
        """;

    public async Task<IAsyncDisposable?> TryAcquireAsync(
        string name,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        var database = connection.GetDatabase();
        var key = new RedisKey($"storagedemo:lock:{name}");

        // Identifies this holder, so only this holder can release it.
        var token = Guid.NewGuid().ToString("N");

        try
        {
            if (!await database.StringSetAsync(key, token, ttl, When.NotExists))
            {
                return null;
            }
        }
        catch (RedisException ex)
        {
            // Redis being unreachable should not make every replica assume it may proceed.
            logger.LogWarning(ex, "Could not take the {LockName} lock", name);
            return null;
        }

        return new Handle(database, key, token, logger);
    }

    private sealed class Handle(IDatabase database, RedisKey key, string token, ILogger logger) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try
            {
                await database.ScriptEvaluateAsync(ReleaseScript, [key], [token]);
            }
            catch (RedisException ex)
            {
                // It expires on its own; the next pass simply waits out the remaining time.
                logger.LogWarning(ex, "Could not release a lock; leaving it to expire");
            }
        }
    }
}
