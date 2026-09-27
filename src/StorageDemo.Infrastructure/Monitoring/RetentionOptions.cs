using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Monitoring;

/// <summary>
/// How long what a live stream produced is kept, and when a registry entry counts as abandoned.
/// </summary>
public sealed class RetentionOptions
{
    public const string SectionName = "Retention";

    public bool Enabled { get; init; }

    /// <summary>How often the pass runs.</summary>
    [Range(1, 86_400)]
    public int IntervalSeconds { get; init; } = 3600;

    /// <summary>How long a recording or a snapshot is kept, from when it was created.</summary>
    [Range(0, 3650)]
    public int MaxAgeDays { get; init; }

    /// <summary>
    /// How stale an owner's heartbeat may be before its registry entry is treated as abandoned and
    /// removed.
    /// </summary>
    [Range(1, 1440)]
    public int AbandonedEntryMinutes { get; init; } = 10;
}
