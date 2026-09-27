using LiteDB;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;
using StorageDemo.Infrastructure.Database.PostgreSql;
using StorageDemo.Infrastructure.FileStorage.FileSystem;
using StorageDemo.Infrastructure.FileStorage.S3;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Infrastructure.HealthChecks;

/// <summary>Readiness for a service whose purpose is being live: is this replica accepting media?</summary>
public sealed class LiveIngestHealthCheck(LiveListeners listeners) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult(listeners.NotServing() is { } why
            ? HealthCheckResult.Unhealthy($"Not serving media: {why}.")
            : HealthCheckResult.Healthy("Media ports are accepting."));
}

/// <summary>Readiness only.</summary>
public sealed class FileSystemHealthCheck(FileSystemStorage storage) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => Task.FromResult(storage.RootIsAccessible()
            ? HealthCheckResult.Healthy("Storage root is accessible.")
            : HealthCheckResult.Unhealthy("Storage root is not accessible."));
}

public sealed class S3HealthCheck(S3Storage storage) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => await storage.BucketIsReachableAsync(cancellationToken)
            ? HealthCheckResult.Healthy("Bucket is reachable.")
            : HealthCheckResult.Unhealthy("Bucket is not reachable.");
}

public sealed class LiteDbHealthCheck(ILiteDatabase database) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _ = database.CollectionExists("documents");
            return Task.FromResult(HealthCheckResult.Healthy("Database file is open."));
        }
        catch (LiteException ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Database file is not usable.", ex));
        }
    }
}

public sealed class RedisHealthCheck(IConnectionMultiplexer connection) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await connection.GetDatabase().PingAsync();
            return HealthCheckResult.Healthy("Redis is reachable.");
        }
        catch (RedisException ex)
        {
            return HealthCheckResult.Unhealthy("Redis is not reachable.", ex);
        }
    }
}

public sealed class PostgresHealthCheck(AppDbContext db) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
        => await db.Database.CanConnectAsync(cancellationToken)
            ? HealthCheckResult.Healthy("Database is reachable.")
            : HealthCheckResult.Unhealthy("Database is not reachable.");
}
