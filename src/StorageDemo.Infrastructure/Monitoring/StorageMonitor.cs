using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Core.Coordination;
using StorageDemo.Core.Documents;

namespace StorageDemo.Infrastructure.Monitoring;

/// <summary>Rescans the store so changes made outside this application still reach the database.</summary>
public sealed class StorageMonitor(
    IServiceScopeFactory scopeFactory,
    StorageChangeSignal signal,
    IDistributedLock scanLock,
    IOptions<StorageMonitorOptions> options,
    ILogger<StorageMonitor> logger) : BackgroundService
{
    private readonly StorageMonitorOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            logger.LogInformation("Storage monitor is disabled");
            return;
        }

        logger.LogInformation(
            "Storage monitor watching {Prefix}, on notification or every {IntervalSeconds}s",
            _options.Prefix,
            _options.IntervalSeconds);

        var interval = TimeSpan.FromSeconds(_options.IntervalSeconds);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var notified = await signal.WaitAsync(interval, stoppingToken);

                if (notified && _options.DebounceMilliseconds > 0)
                {
                    // Copying a folder fires an event per file.
                    await Task.Delay(_options.DebounceMilliseconds, stoppingToken);
                }

                try
                {
                    await using var held = await scanLock.TryAcquireAsync(
                        "storage-scan",
                        LockTtl(interval),
                        stoppingToken);

                    if (held is null)
                    {
                        logger.LogDebug("Another replica is scanning; skipping this pass");
                        continue;
                    }

                    // A scope per pass: the repository and DbContext are scoped services.
                    await using var scope = scopeFactory.CreateAsyncScope();
                    var reconciler = scope.ServiceProvider.GetRequiredService<StorageReconciler>();

                    var result = await reconciler.ReconcileAsync(_options.Prefix, stoppingToken);

                    if (result.AnyChanges)
                    {
                        logger.LogInformation(
                            "Reconciled storage {Added} added {Updated} updated {Removed} removed",
                            result.Added,
                            result.Updated,
                            result.Removed);
                    }
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    // A failed pass must not kill the monitor; the next one retries.
                    logger.LogError(ex, "Storage reconciliation pass failed");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>
    /// Long enough to outlast a slow scan, since a lock that expires mid-pass lets a second replica
    /// start one.
    /// </summary>
    private static TimeSpan LockTtl(TimeSpan interval)
        => TimeSpan.FromSeconds(Math.Max(120, interval.TotalSeconds * 5));
}
