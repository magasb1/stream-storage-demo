using StorageDemo.Core.Documents;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// One behavioural spec, run against every <see cref="IDocumentRepository"/> implementation, so
/// LiteDB and PostgreSQL are held to the same contract.
/// </summary>
public abstract class DocumentRepositoryContract
{
    protected abstract IDocumentRepository CreateRepository();

    protected static Document NewDocument(Guid? id = null) => new()
    {
        Id = id ?? Guid.NewGuid(),
        FileName = "report.pdf",
        StorageKey = $"documents/{id ?? Guid.NewGuid()}/report.pdf",
        ContentType = "application/pdf",
        Size = 1234,
        CreatedAt = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
    };

    [Fact]
    public async Task Can_insert_and_read_a_document()
    {
        var repository = CreateRepository();
        var document = NewDocument();

        await repository.AddAsync(document);
        var loaded = await repository.GetAsync(document.Id);

        Assert.NotNull(loaded);
        Assert.Equal(document.Id, loaded.Id);
        Assert.Equal(document.FileName, loaded.FileName);
        Assert.Equal(document.StorageKey, loaded.StorageKey);
        Assert.Equal(document.ContentType, loaded.ContentType);
        Assert.Equal(document.Size, loaded.Size);
        Assert.Equal(document.CreatedAt, loaded.CreatedAt);
    }

    [Fact]
    public async Task Reading_an_unknown_id_returns_null()
        => Assert.Null(await CreateRepository().GetAsync(Guid.NewGuid()));

    [Fact]
    public async Task GetAll_returns_every_document()
    {
        var repository = CreateRepository();
        await repository.AddAsync(NewDocument());
        await repository.AddAsync(NewDocument());

        Assert.Equal(2, (await repository.GetAllAsync()).Count);
    }

    [Fact]
    public async Task GetAll_is_empty_for_a_fresh_database()
        => Assert.Empty(await CreateRepository().GetAllAsync());

    [Fact]
    public async Task Delete_removes_the_document()
    {
        var repository = CreateRepository();
        var document = NewDocument();
        await repository.AddAsync(document);

        await repository.DeleteAsync(document.Id);

        Assert.Null(await repository.GetAsync(document.Id));
    }

    [Fact]
    public async Task Delete_of_an_unknown_id_succeeds()
        => await CreateRepository().DeleteAsync(Guid.NewGuid());

    [Fact]
    public async Task Upsert_inserts_when_the_document_is_new()
    {
        var repository = CreateRepository();
        var document = NewDocument();

        await repository.UpsertAsync(document);

        Assert.NotNull(await repository.GetAsync(document.Id));
    }

    [Fact]
    public async Task Upsert_replaces_an_existing_document_without_duplicating_it()
    {
        var repository = CreateRepository();
        var document = NewDocument();
        await repository.AddAsync(document);

        await repository.UpsertAsync(new Document
        {
            Id = document.Id,
            FileName = document.FileName,
            StorageKey = document.StorageKey,
            ContentType = document.ContentType,
            Size = 9999,
            CreatedAt = document.CreatedAt,
        });

        Assert.Single(await repository.GetAllAsync());
        Assert.Equal(9999, (await repository.GetAsync(document.Id))!.Size);
    }
}
