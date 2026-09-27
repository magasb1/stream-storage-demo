using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Messaging;

public sealed class MessagingOptions
{
    public const string SectionName = "Messaging";

    /// <summary>"InMemory" or "Redis".</summary>
    [Required(AllowEmptyStrings = false)]
    public string Provider { get; init; } = "InMemory";

    public RedisOptions Redis { get; init; } = new();
}

public sealed class RedisOptions
{
    /// <summary>StackExchange.Redis configuration string, for example "redis:6379".</summary>
    public string ConnectionString { get; init; } = string.Empty;

    public string Channel { get; init; } = "storagedemo:changes";

    /// <summary>The list every replica pushes analysis work to and pops it from.</summary>
    public string AnalysisQueueKey { get; init; } = "storagedemo:analysis";

    /// <summary>How often an idle worker looks for queued work.</summary>
    [Range(50, 60_000)]
    public int PollMilliseconds { get; init; } = 1000;
}
