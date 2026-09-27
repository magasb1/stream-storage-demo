using Microsoft.EntityFrameworkCore;
using StorageDemo.Core.Documents;
using StorageDemo.Infrastructure.Database.PostgreSql;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>Runs the shared contract against real PostgreSQL.</summary>
public sealed class PostgresDocumentRepositoryTests : DocumentRepositoryContract, IDisposable
{
    private const string ConnectionVariable = "POSTGRES_TEST_CONNECTION";

    private static readonly string? ConnectionString =
        Environment.GetEnvironmentVariable(ConnectionVariable);

    private AppDbContext? _db;

    protected override IDocumentRepository CreateRepository()
    {
        Assert.SkipWhen(
            string.IsNullOrWhiteSpace(ConnectionString),
            $"Set {ConnectionVariable} to run the PostgreSQL contract tests.");

        _db = new AppDbContext(new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(ConnectionString)
            .Options);

        _db.Database.Migrate();
        _db.Documents.ExecuteDelete(); // Each test starts from an empty table.

        return new PostgresDocumentRepository(_db);
    }

    public void Dispose() => _db?.Dispose();
}
