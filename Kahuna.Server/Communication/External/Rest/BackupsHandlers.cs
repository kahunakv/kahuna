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
        app.MapPost("/v1/backups/full", async (IKahuna kahuna, CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            KahunaBackupInfo info = await kahuna.TakeFullBackupAsync(ct);
            return Results.Ok(info);
        });

        app.MapPost("/v1/backups/incremental", async (
            [FromBody] KahunaBackupIncrementalRequest req,
            IKahuna kahuna,
            CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            KahunaBackupInfo info = await kahuna.TakeIncrementalBackupAsync(req.ParentBackupId, ct);
            return Results.Ok(info);
        });

        app.MapPost("/v1/backups/coordinated", async (IKahuna kahuna, CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            KahunaBackupInfo info = await kahuna.TakeCoordinatedBackupAsync(ct);
            return Results.Ok(info);
        });

        app.MapGet("/v1/backups", async (IKahuna kahuna, CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            IReadOnlyList<KahunaBackupInfo> list = await kahuna.ListBackupsAsync(ct);
            return Results.Ok(list);
        });

        app.MapGet("/v1/backups/{id}/chain", async (Guid id, IKahuna kahuna, CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(id, ct);
            return Results.Ok(chain);
        });

        app.MapPost("/v1/backups/validate-chain", async (
            [FromBody] KahunaBackupRestoreRequest req,
            IKahuna kahuna,
            CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(req.LeafBackupId, ct);
            return Results.Ok(chain);
        });

        app.MapPost("/v1/restore", async (
            [FromBody] KahunaBackupRestoreRequest req,
            IKahuna kahuna,
            CancellationToken ct) =>
        {
            if (!kahuna.IsBackupConfigured)
                return Results.Problem("Backup is not configured on this node.", statusCode: 503);

            if (string.IsNullOrWhiteSpace(req.TargetDir))
                return Results.Problem("targetDir is required.", statusCode: 400);

            KahunaRestoreResponse result = await kahuna.RestoreToAsync(
                req.LeafBackupId, req.TargetDir, req.TargetTimeMs, ct);
            return Results.Ok(result);
        });
    }
}
