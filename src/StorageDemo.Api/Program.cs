using Microsoft.AspNetCore.Http.Features;
using Serilog;
using StorageDemo.Api.Components;
using StorageDemo.Api.Administration.Streaming;
using StorageDemo.Api.Grpc;
using StorageDemo.Api.Controllers;
using StorageDemo.Api.Middleware;
using StorageDemo.Api.Observability;
using StorageDemo.Api.Uploads;
using StorageDemo.Core.Documents;
using StorageDemo.Infrastructure;
using StorageDemo.Infrastructure.Seeding;

var builder = WebApplication.CreateBuilder(args);

builder.Host.UseSerilog((context, services, configuration) => configuration
    .ReadFrom.Configuration(context.Configuration)
    .ReadFrom.Services(services)
    .Enrich.FromLogContext());

builder.AddTelemetry();

var maxUploadBytes = builder.Configuration.GetValue("Uploads:MaxBytes", 50L * 1024 * 1024);
builder.Services.Configure<FormOptions>(o => o.MultipartBodyLengthLimit = maxUploadBytes);

builder.Services.AddSingleton<ContentTypeSniffer>();

builder.Services.AddSingleton<LivePeerProxy>();

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = (int)Math.Min(maxUploadBytes, int.MaxValue);

    // The live RPCs carry the same token REST requires, or the gRPC port is a way round it.
    options.Interceptors.Add<LiveTokenInterceptor>();
});

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddRazorComponents().AddInteractiveServerComponents();
builder.Services.AddScoped<IStreamConfigurationService, StreamConfigurationService>();

builder.Services.AddInfrastructure(builder.Configuration, builder.Environment.IsDevelopment());

var app = builder.Build();

app.UseMiddleware<ExceptionHandlingMiddleware>();
app.UseSerilogRequestLogging();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// The framework refuses to map the components without it, though the page posts nothing as a form.
app.UseAntiforgery();

app.MapGrpcService<DocumentsGrpcService>();
app.MapControllers();

app.MapStaticAssets();
app.MapRazorComponents<App>().AddInteractiveServerRenderMode();

// Liveness must not touch external infrastructure, or a database blip restarts healthy pods.
app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new() { Predicate = check => check.Tags.Contains("ready") });

await InitializeAsync(app);

if (args.Contains("--migrate-only"))
{
    Log.Information("Migration complete; exiting because --migrate-only was passed");
    return;
}

app.Run();

static async Task InitializeAsync(WebApplication app)
{
    var providers = app.Services.GetRequiredService<ProviderInfo>();
    Log.Information(
        "Starting with {StorageProvider} file storage and {DatabaseProvider} database",
        providers.Storage,
        providers.Database);

    await using var scope = app.Services.CreateAsyncScope();

    await scope.ServiceProvider.GetRequiredService<IDatabaseInitializer>().InitializeAsync();

    foreach (var seeder in scope.ServiceProvider.GetServices<ISeeder>())
    {
        await seeder.SeedAsync();
    }
}

/// <summary>Exposed so the integration tests can host the real application.</summary>
public partial class Program;
