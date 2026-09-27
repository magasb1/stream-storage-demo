using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Worker;

/// <summary>
/// The detection worker: a separate deployment from the ingest pod, subscribing to streams whose
/// detection toggle is set and posting what it finds back to their owners (detection-plan.md D2).
/// </summary>
internal static class WorkerProgram
{
    private static void Main(string[] args)
    {
        // The content root is the binary's own directory, not whatever directory the process was
        // started from.
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            Args = args,
            ContentRootPath = AppContext.BaseDirectory,
        });

        builder.Services.AddOptions<WorkerOptions>()
            .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        // Applied before anything loads libav, since the path cannot change afterwards.
        if (builder.Configuration["Media:LibraryPath"] is { Length: > 0 } libraryPath)
        {
            Ffmpeg.UseDirectory(libraryPath);
        }

        // One client for the life of the process: the subscription to a stream is one request held
        // open for hours, so no request timeout, and a bounded connect because an owner's address
        // can outlive the owner.
        builder.Services.AddSingleton(new HttpClient(new SocketsHttpHandler { ConnectTimeout = TimeSpan.FromSeconds(1) })
        {
            Timeout = Timeout.InfiniteTimeSpan,
        });

        builder.Services.AddHostedService<DetectionWorker>();

        builder.Build().Run();
    }
}
