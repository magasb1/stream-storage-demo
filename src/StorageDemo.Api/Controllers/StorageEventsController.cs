using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Monitoring;

namespace StorageDemo.Api.Controllers;

/// <summary>
/// Receives object notifications from S3-compatible storage and asks the monitor to rescan.
/// </summary>
[ApiController]
[Route("api/storage/events")]
public sealed class StorageEventsController(
    StorageChangeSignal signal,
    IOptions<StorageMonitorOptions> options,
    ILogger<StorageEventsController> logger) : ControllerBase
{
    private readonly StorageMonitorOptions _options = options.Value;

    /// <summary>
    /// Anyone who can reach this can make the service do work, so it stays closed until a token is
    /// configured, and the token is compared in fixed time.
    /// </summary>
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult Notify([FromHeader(Name = "X-Storage-Token")] string? token)
    {
        if (string.IsNullOrWhiteSpace(_options.WebhookToken))
        {
            return NotFound();
        }

        if (!IsAuthorized(token))
        {
            logger.LogWarning("Rejected a storage notification with a bad token");
            return Unauthorized();
        }

        signal.Trigger();
        logger.LogInformation("Storage notification accepted; a rescan is queued");

        return Accepted();
    }

    private bool IsAuthorized(string? token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(token),
            Encoding.UTF8.GetBytes(_options.WebhookToken!));
    }
}
