using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Database.PostgreSql;

/// <summary>Applies migrations at startup.</summary>
public sealed class PostgresInitializer(AppDbContext db, ILogger<PostgresInitializer> logger)
    : IDatabaseInitializer
{
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await db.Database.MigrateAsync(cancellationToken);
        logger.LogInformation("PostgreSQL migrations applied {DatabaseProvider}", "Postgres");
    }
}
