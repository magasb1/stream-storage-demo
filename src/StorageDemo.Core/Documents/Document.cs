namespace StorageDemo.Core.Documents;

/// <summary>One piece of a document that was written in pieces, and how big it is.</summary>
public sealed record DocumentPart(string Key, long Size);

/// <summary>Metadata for a stored file.</summary>
public sealed class Document
{
    public Guid Id { get; init; }

    public required string FileName { get; init; }

    public required string StorageKey { get; init; }

    public string? ContentType { get; init; }

    public long Size { get; init; }

    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Key of the generated preview image, or null when the type has no thumbnail.</summary>
    public string? ThumbnailKey { get; init; }

    /// <summary>
    /// Whatever the media probe could read: dimensions, duration, codecs, bitrate, embedded tags.
    /// </summary>
    public IReadOnlyDictionary<string, string> Metadata { get; init; } =
        new Dictionary<string, string>();

    /// <summary>
    /// The pieces this document is made of, in order, when it was written in pieces rather than in
    /// one go.
    /// </summary>
    public IReadOnlyList<DocumentPart> Parts { get; init; } = [];

    /// <summary>True when the bytes are the pieces rather than a single object.</summary>
    public bool Segmented => Parts.Count > 0;
}
