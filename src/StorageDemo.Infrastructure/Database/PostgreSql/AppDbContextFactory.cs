using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace StorageDemo.Infrastructure.Database.PostgreSql;

/// <summary>Used only by `dotnet ef` at design time.</summary>
public sealed class AppDbContextFactory : IDesignTimeDbContextFactory<AppDbContext>
{
    public AppDbContext CreateDbContext(string[] args)
        => new(new DbContextOptionsBuilder<AppDbContext>()
            .UseNpgsql(
                Environment.GetEnvironmentVariable("Database__Postgres__ConnectionString")
                ?? "Host=localhost;Database=storagedemo;Username=postgres;Password=postgres")
            .Options);
}
