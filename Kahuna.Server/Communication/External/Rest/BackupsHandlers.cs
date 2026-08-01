using Kahuna.Shared.Communication.Rest;
using Microsoft.AspNetCore.Mvc;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// REST endpoints for PITR backup and catalog operations.
/// </summary>
public static class BackupsHandlers
{
    public static void MapBackupsRoutes(WebApplication app)
    {
        app.MapPost("/v1/backups/full", (IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.TakeFullBackupAsync(ct))));

        app.MapPost("/v1/backups/incremental", (
            [FromBody] KahunaBackupIncrementalRequest req, IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.TakeIncrementalBackupAsync(req.ParentBackupId, ct))));

        app.MapPost("/v1/backups/coordinated", (IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.TakeCoordinatedBackupAsync(ct))));

        app.MapGet("/v1/backups", (IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.ListBackupsAsync(ct))));

        // Reclaim backup disk: sweep orphaned/leftover artifacts and enforce retention. ?dryRun=true
        // returns the inventory of what would be reclaimed without deleting anything (default false).
        app.MapPost("/v1/backups/gc", ([FromQuery] bool dryRun, IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.RunBackupGarbageCollectionAsync(dryRun, ct))));

        app.MapGet("/v1/backups/{id}/chain", (Guid id, IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.GetBackupChainAsync(id, ct))));

        app.MapPost("/v1/backups/validate-chain", (
            [FromBody] KahunaBackupRestoreRequest req, IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () => Results.Ok(await kahuna.GetBackupChainAsync(req.LeafBackupId, ct))));

        app.MapPost("/v1/restore", (
            [FromBody] KahunaBackupRestoreRequest req, IKahuna kahuna, HttpResponse resp, CancellationToken ct) =>
            Guard(kahuna, resp, async () =>
            {
                if (!kahuna.IsRemoteRestoreAllowed)
                {
                    resp.Headers[KahunaBackupWire.OutcomeHttpHeader] = nameof(KahunaBackupOutcome.NotConfigured);
                    return Results.Problem(
                        "Remote restore is disabled on this node. Configure a server-owned restore root " +
                        "(RestoreRoot) or explicitly allow unconfined remote restore.",
                        statusCode: 403,
                        extensions: new Dictionary<string, object?> { ["outcome"] = nameof(KahunaBackupOutcome.NotConfigured) });
                }

                if (string.IsNullOrWhiteSpace(req.TargetDir))
                    return Results.Problem("targetDir is required.", statusCode: 400);

                return Results.Ok(await kahuna.RestoreToAsync(req.LeafBackupId, req.TargetDir, req.TargetTimeMs, ct));
            }));
    }

    /// <summary>
    /// Checks backup configuration, runs the handler, and translates a typed
    /// <see cref="KahunaBackupException"/> into a stable ProblemDetails carrying the
    /// <see cref="KahunaBackupOutcome"/> (as an <c>outcome</c> extension and the
    /// <see cref="KahunaBackupWire.OutcomeHttpHeader"/> response header) so remote clients can
    /// reconstruct the typed exception.
    /// </summary>
    private static async Task<IResult> Guard(IKahuna kahuna, HttpResponse resp, Func<Task<IResult>> body)
    {
        if (!kahuna.IsBackupConfigured)
        {
            resp.Headers[KahunaBackupWire.OutcomeHttpHeader] = nameof(KahunaBackupOutcome.NotConfigured);
            return Results.Problem("Backup is not configured on this node.", statusCode: 503,
                extensions: new Dictionary<string, object?> { ["outcome"] = nameof(KahunaBackupOutcome.NotConfigured) });
        }

        try
        {
            return await body();
        }
        catch (KahunaBackupException ex)
        {
            resp.Headers[KahunaBackupWire.OutcomeHttpHeader] = ex.Outcome.ToString();
            return Results.Problem(ex.Message, statusCode: MapHttpStatus(ex.Outcome),
                extensions: new Dictionary<string, object?> { ["outcome"] = ex.Outcome.ToString() });
        }
    }

    private static int MapHttpStatus(KahunaBackupOutcome outcome) => outcome switch
    {
        KahunaBackupOutcome.NotConfigured => 503,
        KahunaBackupOutcome.RetryableLeadershipLoss => 503,
        KahunaBackupOutcome.NotBackupCoordinator => 503,
        KahunaBackupOutcome.InsecureRoot => 503,
        KahunaBackupOutcome.ParentMissing => 404,
        KahunaBackupOutcome.TargetConflict => 409,
        KahunaBackupOutcome.Cancelled => 499,
        KahunaBackupOutcome.IoError => 500,
        _ => 422 // correctness/precondition failures (corrupt, coverage, needs-full, unsupported, …)
    };
}
