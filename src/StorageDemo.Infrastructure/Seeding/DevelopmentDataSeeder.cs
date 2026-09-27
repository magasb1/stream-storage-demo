using Microsoft.Extensions.Logging;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Storage;

namespace StorageDemo.Infrastructure.Seeding;

/// <summary>Development-only sample data.</summary>
public sealed class DevelopmentDataSeeder(
    IFileStorage fileStorage,
    IDocumentRepository repository,
    ILogger<DevelopmentDataSeeder> logger) : ISeeder
{
    // Stable id, so re-seeding replaces rather than duplicates.
    private static readonly Guid WelcomeId = new("11111111-1111-1111-1111-111111111111");

    public async Task SeedAsync(CancellationToken cancellationToken = default)
    {
        var sourcePath = Path.Combine(AppContext.BaseDirectory, "SeedData", "welcome.txt");
        if (!File.Exists(sourcePath))
        {
            logger.LogWarning("Development seed file missing at {Path}", sourcePath);
            return;
        }

        var storageKey = $"documents/{WelcomeId}/welcome.txt";

        // Idempotent in both directions: the object and the row are each written only if absent.
        if (!await fileStorage.ExistsAsync(storageKey, cancellationToken))
        {
            await using var source = File.OpenRead(sourcePath);
            await fileStorage.SaveAsync(storageKey, source, "text/plain", cancellationToken);
        }

        if (await repository.GetAsync(WelcomeId, cancellationToken) is null)
        {
            await repository.AddAsync(
                new Document
                {
                    Id = WelcomeId,
                    FileName = "welcome.txt",
                    StorageKey = storageKey,
                    ContentType = "text/plain",
                    Size = new FileInfo(sourcePath).Length,
                    CreatedAt = DateTimeOffset.UtcNow,
                },
                cancellationToken);
        }

        logger.LogInformation("Development seed complete {DocumentId}", WelcomeId);
    }
}
