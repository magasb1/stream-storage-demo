using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Messaging;

/// <summary>
/// A Redis list shared by every replica: any pod can produce the thumbnail for a file another pod
/// received, and a pod that dies with work still queued does not take that work with it.
/// </summary>
public sealed class RedisAnalysisQueue(
    IConnectionMultiplexer connection,
    IOptions<MessagingOptions> options,
    ILogger<RedisAnalysisQueue> logger) : IAnalysisQueue
{
    private readonly RedisKey _key = options.Value.Redis.AnalysisQueueKey;
    private readonly TimeSpan _pollInterval = TimeSpan.FromMilliseconds(options.Value.Redis.PollMilliseconds);

    public async Task EnqueueAsync(AnalysisRequest request, CancellationToken cancellationToken = default)
    {
        try
        {
            await connection.GetDatabase().ListRightPushAsync(_key, JsonSerializer.Serialize(request));
        }
        catch (RedisException ex)
        {
            // The document is already stored and listed; it just goes without a preview for now.
            logger.LogWarning(ex, "Could not queue {StorageKey} for analysis", request.StorageKey);
        }
    }

    public async IAsyncEnumerable<AnalysisRequest> DequeueAllAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var database = connection.GetDatabase();

        while (!cancellationToken.IsCancellationRequested)
        {
            RedisValue value;
            try
            {
                value = await database.ListLeftPopAsync(_key);
            }
            catch (RedisException ex)
            {
                logger.LogWarning(ex, "Could not read the analysis queue");
                value = RedisValue.Null;
            }

            if (value.IsNullOrEmpty)
            {
                await Task.Delay(_pollInterval, cancellationToken);
                continue;
            }

            AnalysisRequest? request = null;
            try
            {
                request = JsonSerializer.Deserialize<AnalysisRequest>(value.ToString());
            }
            catch (JsonException ex)
            {
                // Written by another version, or something else using the key.
                logger.LogWarning(ex, "Discarded an unreadable analysis request");
            }

            if (request is not null)
            {
                yield return request;
            }
        }
    }
}
