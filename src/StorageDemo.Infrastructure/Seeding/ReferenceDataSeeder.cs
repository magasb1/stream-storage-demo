using Microsoft.Extensions.Logging;

namespace StorageDemo.Infrastructure.Seeding;

/// <summary>Runs in every environment, production included.</summary>
public sealed class ReferenceDataSeeder(ILogger<ReferenceDataSeeder> logger) : ISeeder
{
    public Task SeedAsync(CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Reference data seed complete (nothing to insert)");
        return Task.CompletedTask;
    }
}
