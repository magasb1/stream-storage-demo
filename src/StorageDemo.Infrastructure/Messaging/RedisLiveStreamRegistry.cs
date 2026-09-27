using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Messaging;

/// <summary>
/// Live streams in a Redis hash, one field per name, so any replica can list them, find one, and
/// see which replica owns it.
/// </summary>
public sealed class RedisLiveStreamRegistry(
    IConnectionMultiplexer connection,
    ILogger<RedisLiveStreamRegistry> logger) : ILiveStreamRegistry
{
    private const string Key = "storagedemo:live:streams";

    public async Task UpsertAsync(LiveStream stream, CancellationToken cancellationToken = default)
    {
        try
        {
            await connection.GetDatabase().HashSetAsync(
                Key,
                stream.Name,
                JsonSerializer.Serialize(stream));
        }
        catch (RedisException ex)
        {
            // The stream keeps running; it is only invisible to the other replicas.
            logger.LogWarning(ex, "Could not record live stream '{Name}'", stream.Name);
        }
    }

    public async Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        try
        {
            await connection.GetDatabase().HashDeleteAsync(Key, name);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Could not remove live stream '{Name}'", name);
        }
    }

    public async Task<LiveStream?> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        try
        {
            var value = await connection.GetDatabase().HashGetAsync(Key, name);

            return value.IsNullOrEmpty ? null : Deserialize(value);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Could not read live stream '{Name}'", name);
            return null;
        }
    }

    public async Task<IReadOnlyList<LiveStream>> ListAsync(CancellationToken cancellationToken = default)
    {
        HashEntry[] entries;

        try
        {
            entries = await connection.GetDatabase().HashGetAllAsync(Key);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Could not list live streams");
            return [];
        }

        var streams = new List<LiveStream>(entries.Length);
        var unreadable = new List<RedisValue>();

        foreach (var entry in entries)
        {
            var stream = Deserialize(entry.Value);

            if (stream is null)
            {
                unreadable.Add(entry.Name);
                continue;
            }

            streams.Add(stream);
        }

        if (unreadable.Count > 0)
        {
            // Tidying on read, so nothing has to run a sweeper for a handful of records.
            _ = connection.GetDatabase().HashDeleteAsync(Key, [.. unreadable]);
        }

        return [.. streams.OrderBy(stream => stream.Name, StringComparer.Ordinal)];
    }

    private LiveStream? Deserialize(RedisValue value)
    {
        if (value.IsNullOrEmpty)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<LiveStream>(value.ToString());
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Discarded an unreadable live stream record");
            return null;
        }
    }
}
