using LiteDB;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Database.LiteDb;

/// <summary>LiteDB is synchronous and file-based; calls are wrapped so the interface stays async.</summary>
public sealed class LiteDbDocumentRepository(ILiteDatabase database) : IDocumentRepository
{
    public const string CollectionName = "documents";

    private readonly ILiteCollection<DocumentRecord> _documents =
        database.GetCollection<DocumentRecord>(CollectionName);

    public Task<Document?> GetAsync(Guid id, CancellationToken cancellationToken = default)
        => Run(() => _documents.FindById(id)?.ToDomain(), cancellationToken);

    public Task<IReadOnlyList<Document>> GetAllAsync(CancellationToken cancellationToken = default)
        => Run<IReadOnlyList<Document>>(
            () => _documents.FindAll().Select(r => r.ToDomain()).ToList(),
            cancellationToken);

    public Task AddAsync(Document document, CancellationToken cancellationToken = default)
        => Run(() => _documents.Insert(DocumentRecord.FromDomain(document)), cancellationToken);

    public Task DeleteAsync(Guid id, CancellationToken cancellationToken = default)
        => Run(() => _documents.Delete(id), cancellationToken);

    public Task UpsertAsync(Document document, CancellationToken cancellationToken = default)
        => Run(() => _documents.Upsert(DocumentRecord.FromDomain(document)), cancellationToken);

    private static Task<T> Run<T>(Func<T> action, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            return Task.FromResult(action());
        }
        catch (LiteException ex)
        {
            throw new PersistenceException("LiteDB operation failed.", ex);
        }
    }
}

internal sealed class DocumentRecord
{
    [BsonId]
    public Guid Id { get; set; }

    public string FileName { get; set; } = string.Empty;

    public string StorageKey { get; set; } = string.Empty;

    public string? ContentType { get; set; }

    public long Size { get; set; }

    /// <summary>Stored as ISO-8601 text: LiteDB has no DateTimeOffset and would drop the offset.</summary>
    public string CreatedAt { get; set; } = string.Empty;

    public string? ThumbnailKey { get; set; }

    /// <summary>Media metadata is free-form, so it is stored as JSON rather than as columns.</summary>
    public string? MetadataJson { get; set; }

    /// <summary>The pieces of a document written a piece at a time, in order.</summary>
    public string? PartsJson { get; set; }

    public static DocumentRecord FromDomain(Document document) => new()
    {
        Id = document.Id,
        FileName = document.FileName,
        StorageKey = document.StorageKey,
        ContentType = document.ContentType,
        Size = document.Size,
        CreatedAt = document.CreatedAt.ToString("O"),
        ThumbnailKey = document.ThumbnailKey,
        MetadataJson = DocumentMetadata.Serialize(document.Metadata),
        PartsJson = DocumentMetadata.SerializeParts(document.Parts),
    };

    public Document ToDomain() => new()
    {
        Id = Id,
        FileName = FileName,
        StorageKey = StorageKey,
        ContentType = ContentType,
        Size = Size,
        CreatedAt = DateTimeOffset.Parse(CreatedAt, null, System.Globalization.DateTimeStyles.RoundtripKind),
        ThumbnailKey = ThumbnailKey,
        Metadata = DocumentMetadata.Deserialize(MetadataJson),
        Parts = DocumentMetadata.DeserializeParts(PartsJson),
    };
}
