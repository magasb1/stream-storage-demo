using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;

namespace StorageDemo.Core.Documents;

public sealed record RetentionResult(int Documents, int Entries)
{
    public bool AnyChanges => Documents + Entries > 0;
}

/// <summary>Expires what a live stream produced, and removes registry entries whose owner is gone.</summary>
public sealed class RetentionSweeper(
    IDocumentService documents,
    ILiveStreamRegistry registry,
    ILogger<RetentionSweeper> logger)
{
    /// <summary>
    /// What every document a live stream produces carries, written by the recorder and by the
    /// snapshot path.
    /// </summary>
    private const string StreamKey = "Live stream";

    private const string StartedKey = "Recording started";

    /// <param name="maxAge">Zero keeps every document, leaving only the registry to sweep.</param>
    public async Task<RetentionResult> SweepAsync(
        TimeSpan maxAge,
        TimeSpan abandonedAfter,
        CancellationToken cancellationToken = default)
    {
        var streams = await registry.ListAsync(cancellationToken);
        var now = DateTimeOffset.UtcNow;

        var expired = maxAge > TimeSpan.Zero
            ? await ExpireAsync(streams, now - maxAge, cancellationToken)
            : 0;

        var removed = 0;
        foreach (var stream in streams.Where(s => now - s.Heartbeat > abandonedAfter))
        {
            await registry.RemoveAsync(stream.Name, cancellationToken);
            removed++;

            logger.LogWarning(
                "Removed the abandoned registry entry for '{Name}', owned by {Owner}, last seen {Heartbeat}",
                stream.Name,
                stream.Owner,
                stream.Heartbeat);
        }

        return new RetentionResult(expired, removed);
    }

    private async Task<int> ExpireAsync(
        IReadOnlyList<LiveStream> streams,
        DateTimeOffset cutoff,
        CancellationToken cancellationToken)
    {
        // A recording is open whenever its stream carries a RecordingStatus, and the registry is
        // shared, so the replica sweeping learns this from the registry rather than from the pod
        // holding the socket.
        var open = streams
            .Where(stream => stream.Recording is not null)
            .Select(stream => OpenKey(stream.Name, Moment(stream.Recording!.StartedAt)))
            .ToHashSet(StringComparer.Ordinal);

        var expired = 0;

        foreach (var document in await documents.GetAllAsync(cancellationToken))
        {
            if (!document.Metadata.TryGetValue(StreamKey, out var stream) || document.CreatedAt > cutoff)
            {
                continue;
            }

            if (document.Metadata.TryGetValue(StartedKey, out var started)
                && open.Contains(OpenKey(stream, started)))
            {
                // It appeared with its first part and has been growing ever since, so it is older
                // than the cutoff long before it is finished.
                logger.LogDebug(
                    "'{Name}' is still being recorded as {DocumentId}; leaving it",
                    stream,
                    document.Id);

                continue;
            }

            // Bytes then row, which DeleteAsync does: parts, thumbnail, then the document.
            await documents.DeleteAsync(document.Id, cancellationToken);
            expired++;

            logger.LogInformation(
                "Expired {DocumentId} ({FileName}) from '{Name}', created {CreatedAt}",
                document.Id,
                document.FileName,
                stream,
                document.CreatedAt);
        }

        return expired;
    }

    /// <summary>
    /// The recorder writes its start time into the document's metadata in this format, so the two
    /// sides of the comparison are the same string rather than two parses that could disagree.
    /// </summary>
    private static string Moment(DateTimeOffset at) => at.ToString("u");

    private static string OpenKey(string name, string startedAt) => $"{name}\n{startedAt}";
}
