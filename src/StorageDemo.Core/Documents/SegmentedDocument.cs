using Microsoft.Extensions.Logging;
using StorageDemo.Core.Storage;

namespace StorageDemo.Core.Documents;

/// <summary>
/// A document being written a piece at a time.
///
/// It exists for one reason: a recording that runs for hours cannot wait until it ends to be
/// stored. Holding it on one pod's disk for six hours means six hours of disk, and losing that pod
/// means losing all six. So each few minutes is uploaded as it completes and the local file is
/// deleted, which bounds disk to a handful of pieces and means what has already been recorded
/// survives the pod that recorded it. How many is the writer's business: a recorder keeps a piece or
/// two in hand so that an upload never stops it capturing, which is what its own notes explain.
///
/// Each piece is uploaded through the ordinary storage path, as an ordinary object. Nothing here
/// needs multipart upload or anything else only one provider has, which is the property the whole
/// repository exists to demonstrate.
///
/// The document row appears with the first piece rather than at the end. The design said a
/// document should appear only when there is a file, because a half-written one that cannot be
/// downloaded is a lie. A piece that has been uploaded is not half-written, and for a camera the
/// alternative is losing everything recorded before a restart, so it appears and grows.
/// </summary>
public sealed class SegmentedDocument
{
    /// <summary>
    /// Pieces live outside the prefix the storage monitor scans, exactly as thumbnails do. Left
    /// under it, each piece would be imported as a document of its own and the recording would
    /// appear as a hundred five-minute files.
    /// </summary>
    public const string Prefix = "recordings/";

    private readonly IFileStorage _storage;
    private readonly IDocumentRepository _repository;
    private readonly IChangeFeed _changeFeed;
    private readonly ILogger _logger;
    private readonly List<DocumentPart> _parts = [];
    private readonly IReadOnlyDictionary<string, string> _metadata;
    private readonly string _extension;

    private bool _announced;

    internal SegmentedDocument(
        IFileStorage storage,
        IDocumentRepository repository,
        IChangeFeed changeFeed,
        ILogger logger,
        string fileName,
        string? contentType,
        IReadOnlyDictionary<string, string> metadata)
    {
        _storage = storage;
        _repository = repository;
        _changeFeed = changeFeed;
        _logger = logger;
        _metadata = metadata;
        _extension = Path.GetExtension(fileName);

        Id = Guid.NewGuid();
        FileName = fileName;
        ContentType = contentType;

        // Names the document rather than pointing at bytes: the pieces carry those. Nothing reads
        // an object at this key, and the reconciler knows not to look for one.
        StorageKey = $"{Prefix}{Id}/{fileName}";
    }

    public Guid Id { get; }

    public string FileName { get; }

    public string? ContentType { get; }

    public string StorageKey { get; }

    public long Size => _parts.Sum(part => part.Size);

    public int Count => _parts.Count;

    /// <summary>
    /// Stores one more piece and makes it part of the document straight away.
    ///
    /// Whoever opens the document between two pieces gets everything recorded so far, joined, and
    /// can seek anywhere in it. That is what makes a recording still running look no different
    /// from one that has finished, apart from being shorter.
    /// </summary>
    public async Task AppendAsync(
        Stream piece,
        long size,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        var key = $"{Prefix}{Id}/{_parts.Count:D5}{_extension}";

        await _storage.SaveAsync(key, piece, ContentType, cancellationToken);

        _parts.Add(new DocumentPart(key, size));

        await _repository.UpsertAsync(ToDocument(metadata), cancellationToken);

        _changeFeed.Publish(new DocumentChange(
            _announced ? ChangeKind.Updated : ChangeKind.Added,
            Id,
            StorageKey,
            FileName));

        _announced = true;

        _logger.LogInformation(
            "Stored piece {Part} of {DocumentId} ({FileName}), {Size} bytes",
            _parts.Count,
            Id,
            FileName,
            size);
    }

    /// <summary>
    /// The document as it stands. Null before anything has been stored, because a recording that
    /// captured nothing should leave nothing.
    /// </summary>
    public Document? Current(IReadOnlyDictionary<string, string>? metadata = null)
        => _parts.Count == 0 ? null : ToDocument(metadata);

    /// <summary>
    /// The last word on the document: whatever the writer knows now that it has finished, such as
    /// how long it actually ran. Null when nothing was ever stored.
    /// </summary>
    public async Task<Document?> CompleteAsync(
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken cancellationToken = default)
    {
        if (_parts.Count == 0)
        {
            return null;
        }

        var document = ToDocument(metadata);

        await _repository.UpsertAsync(document, cancellationToken);
        _changeFeed.Publish(new DocumentChange(ChangeKind.Updated, Id, StorageKey, FileName));

        return document;
    }

    /// <summary>Removes every piece, for a recording that turned out to be worth nothing.</summary>
    public async Task DiscardAsync(CancellationToken cancellationToken = default)
    {
        foreach (var part in _parts)
        {
            await _storage.DeleteAsync(part.Key, cancellationToken);
        }

        if (_announced)
        {
            await _repository.DeleteAsync(Id, cancellationToken);
            _changeFeed.Publish(new DocumentChange(ChangeKind.Removed, Id, StorageKey, FileName));
        }

        _parts.Clear();
    }

    private Document ToDocument(IReadOnlyDictionary<string, string>? extra) => new()
    {
        Id = Id,
        FileName = FileName,
        StorageKey = StorageKey,
        ContentType = ContentType,
        Size = Size,
        CreatedAt = Created,
        Metadata = extra is null ? _metadata : Merge(_metadata, extra),
        Parts = [.. _parts],
    };

    private DateTimeOffset Created { get; } = DateTimeOffset.UtcNow;

    private static Dictionary<string, string> Merge(
        IReadOnlyDictionary<string, string> first,
        IReadOnlyDictionary<string, string> second)
    {
        var merged = new Dictionary<string, string>(first);

        foreach (var (key, value) in second)
        {
            merged[key] = value;
        }

        return merged;
    }
}
