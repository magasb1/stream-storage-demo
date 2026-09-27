using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Infrastructure.Messaging;

/// <summary>
/// Configured sources in a Redis hash, one field per name, so every replica reads the same list and
/// a source outlives the pod that was serving it.
/// </summary>
public sealed class RedisLiveSourceStore(
    IConnectionMultiplexer connection,
    ILogger<RedisLiveSourceStore> logger) : ILiveSourceStore
{
    private const string Key = "storagedemo:live:sources";

    public async Task SaveAsync(LiveSource source, CancellationToken cancellationToken = default)
    {
        await Write(
            () => connection.GetDatabase().HashSetAsync(Key, source.Name, JsonSerializer.Serialize(source)),
            $"save live source '{source.Name}'");
    }

    public async Task RemoveAsync(string name, CancellationToken cancellationToken = default)
    {
        await Write(
            () => connection.GetDatabase().HashDeleteAsync(Key, name),
            $"remove live source '{name}'");
    }

    private static async Task Write(Func<Task> write, string what)
    {
        try
        {
            await write();
        }
        catch (RedisException ex)
        {
            throw new PersistenceException($"Could not {what}.", ex);
        }
    }

    public async Task<LiveSource?> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        try
        {
            var value = await connection.GetDatabase().HashGetAsync(Key, name);

            return value.IsNullOrEmpty ? null : Deserialize(value);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Could not read live source '{Name}'", name);
            return null;
        }
    }

    public async Task<IReadOnlyList<LiveSource>> ListAsync(CancellationToken cancellationToken = default)
    {
        HashEntry[] entries;

        try
        {
            entries = await connection.GetDatabase().HashGetAllAsync(Key);
        }
        catch (RedisException ex)
        {
            logger.LogWarning(ex, "Could not list live sources");
            return [];
        }

        var sources = new List<LiveSource>(entries.Length);
        var unreadable = new List<RedisValue>();

        foreach (var entry in entries)
        {
            var source = Deserialize(entry.Value);

            if (source is null)
            {
                unreadable.Add(entry.Name);
                continue;
            }

            sources.Add(source);
        }

        if (unreadable.Count > 0)
        {
            // Tidying on read, so nothing has to run a sweeper for a handful of records.
            _ = connection.GetDatabase().HashDeleteAsync(Key, [.. unreadable]);
        }

        return [.. sources.OrderBy(source => source.Name, StringComparer.Ordinal)];
    }

    private LiveSource? Deserialize(RedisValue value)
    {
        if (value.IsNullOrEmpty)
        {
            return null;
        }

        try
        {
            return JsonSerializer.Deserialize<LiveSource>(value.ToString());
        }
        catch (JsonException ex)
        {
            logger.LogWarning(ex, "Discarded an unreadable live source record");
            return null;
        }
    }
}
