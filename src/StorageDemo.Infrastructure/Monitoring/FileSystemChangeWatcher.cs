using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.FileStorage.FileSystem;

namespace StorageDemo.Infrastructure.Monitoring;

/// <summary>
/// Turns local file changes into a nudge for the monitor, so a file dropped into the storage
/// directory shows up in about a second instead of at the next scheduled scan.
/// </summary>
public sealed class FileSystemChangeWatcher(
    IOptions<FileSystemStorageOptions> storageOptions,
    IOptions<StorageMonitorOptions> monitorOptions,
    StorageChangeSignal signal,
    ILogger<FileSystemChangeWatcher> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var root = Path.GetFullPath(storageOptions.Value.RootPath);

        // Watch only the documents subtree: thumbnails are written into the same root by the
        // analysis worker, and watching those would have the application trigger itself.
        var watched = Path.Combine(root, monitorOptions.Value.Prefix.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(watched);

        var watcher = new FileSystemWatcher(watched)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size,
        };

        watcher.Created += OnChanged;
        watcher.Changed += OnChanged;
        watcher.Deleted += OnChanged;
        watcher.Renamed += OnChanged;

        // An overflow means events were lost, which is exactly when a full rescan is wanted.
        watcher.Error += (_, args) =>
        {
            logger.LogWarning(args.GetException(), "File watcher error; falling back to a full scan");
            signal.Trigger();
        };

        watcher.EnableRaisingEvents = true;
        logger.LogInformation("Watching {Path} for local changes", watched);

        stoppingToken.Register(() =>
        {
            watcher.EnableRaisingEvents = false;
            watcher.Dispose();
        });

        return Task.CompletedTask;
    }

    private void OnChanged(object sender, FileSystemEventArgs e) => signal.Trigger();
}
