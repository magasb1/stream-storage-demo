namespace StorageDemo.Core.Documents;

public interface IDocumentRepository
{
    Task<Document?> GetAsync(Guid id, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<Document>> GetAllAsync(CancellationToken cancellationToken = default);

    Task AddAsync(Document document, CancellationToken cancellationToken = default);

    /// <summary>Insert or replace.</summary>
    Task UpsertAsync(Document document, CancellationToken cancellationToken = default);

    /// <summary>Idempotent: deleting a missing id succeeds.</summary>
    Task DeleteAsync(Guid id, CancellationToken cancellationToken = default);
}
