using Microsoft.AspNetCore.Mvc;
using StorageDemo.Core.Documents;
using StorageDemo.Core.Storage;

namespace StorageDemo.Api.Middleware;

/// <summary>Turns infrastructure failures into ProblemDetails without leaking provider internals.</summary>
public sealed class ExceptionHandlingMiddleware(RequestDelegate next, ILogger<ExceptionHandlingMiddleware> logger)
{
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await next(context);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            logger.LogInformation("Request cancelled by client {Path}", context.Request.Path);
        }
        catch (Exception ex) when (ex is StorageException or PersistenceException)
        {
            logger.LogError(ex, "Infrastructure failure handling {Path}", context.Request.Path);
            await WriteProblem(context, StatusCodes.Status503ServiceUnavailable, "Storage backend unavailable.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled failure handling {Path}", context.Request.Path);
            await WriteProblem(context, StatusCodes.Status500InternalServerError, "Unexpected error.");
        }
    }

    private static async Task WriteProblem(HttpContext context, int status, string detail)
    {
        if (context.Response.HasStarted)
        {
            return;
        }

        context.Response.StatusCode = status;
        await context.Response.WriteAsJsonAsync(new ProblemDetails
        {
            Status = status,
            Title = detail,
            Instance = context.Request.Path,
        });
    }
}
