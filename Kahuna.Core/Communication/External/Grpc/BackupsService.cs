using Grpc.Core;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// gRPC service mirroring the REST backup endpoints.
/// </summary>
public sealed class BackupsService : Backups.BackupsBase
{
    private readonly IKahuna kahuna;

    public BackupsService(IKahuna kahuna)
    {
        this.kahuna = kahuna;
    }

    public override Task<GrpcBackupInfoResponse> TakeFullBackup(
        GrpcTakeFullBackupRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            return ToGrpc(await kahuna.TakeFullBackupAsync(context.CancellationToken));
        });

    public override Task<GrpcBackupInfoResponse> TakeIncrementalBackup(
        GrpcTakeIncrementalBackupRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            if (!Guid.TryParse(request.ParentBackupId, out Guid parentId))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid parentBackupId GUID."));
            return ToGrpc(await kahuna.TakeIncrementalBackupAsync(parentId, context.CancellationToken));
        });

    public override Task<GrpcBackupInfoResponse> TakeCoordinatedBackup(
        GrpcTakeCoordinatedBackupRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            return ToGrpc(await kahuna.TakeCoordinatedBackupAsync(context.CancellationToken));
        });

    public override Task<GrpcListBackupsResponse> ListBackups(
        GrpcListBackupsRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            IReadOnlyList<KahunaBackupInfo> list = await kahuna.ListBackupsAsync(context.CancellationToken);
            GrpcListBackupsResponse response = new();
            foreach (KahunaBackupInfo b in list)
                response.Backups.Add(ToGrpc(b));
            return response;
        });

    public override Task<GrpcListBackupsResponse> GetBackupChain(
        GrpcGetBackupChainRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            if (!Guid.TryParse(request.LeafBackupId, out Guid leafId))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid leafBackupId GUID."));
            IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(leafId, context.CancellationToken);
            GrpcListBackupsResponse response = new();
            foreach (KahunaBackupInfo b in chain)
                response.Backups.Add(ToGrpc(b));
            return response;
        });

    public override Task<GrpcListBackupsResponse> ValidateChain(
        GrpcValidateChainRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            if (!Guid.TryParse(request.LeafBackupId, out Guid leafId))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid leafBackupId GUID."));
            IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(leafId, context.CancellationToken);
            GrpcListBackupsResponse response = new();
            foreach (KahunaBackupInfo b in chain)
                response.Backups.Add(ToGrpc(b));
            return response;
        });

    public override Task<GrpcRestoreResponse> Restore(GrpcRestoreRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            if (!kahuna.IsRemoteRestoreAllowed)
                throw new KahunaBackupException(KahunaBackupOutcome.NotConfigured,
                    "Remote restore is disabled on this node. Configure a server-owned restore root " +
                    "(RestoreRoot) or explicitly allow unconfined remote restore.");
            if (!Guid.TryParse(request.LeafBackupId, out Guid leafId))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid leafBackupId GUID."));
            if (string.IsNullOrWhiteSpace(request.TargetDir))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "targetDir is required."));
            KahunaRestoreResponse r = await kahuna.RestoreToAsync(leafId, request.TargetDir, request.TargetTimeMs, context.CancellationToken);
            GrpcRestoreResponse response = new()
            {
                TargetDir = r.TargetDir,
                PartitionsRestored = r.PartitionsRestored,
                EntriesApplied = r.EntriesApplied,
                LastAppliedPhysicalMs = r.LastAppliedPhysicalMs,
                Outcome = r.Outcome.ToString(),
                MinRecoverablePhysicalMs = r.MinRecoverablePhysicalMs,
                MaxRecoverablePhysicalMs = r.MaxRecoverablePhysicalMs
            };
            foreach (KahunaBackupInfo b in r.Chain)
                response.Chain.Add(ToGrpc(b));
            return response;
        });

    public override Task<GrpcBackupGcResponse> RunBackupGarbageCollection(
        GrpcBackupGcRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            RequireBackup();
            KahunaBackupGcResult r = await kahuna.RunBackupGarbageCollectionAsync(request.DryRun, context.CancellationToken);
            GrpcBackupGcResponse response = new()
            {
                Applied = r.Applied,
                BytesReclaimed = r.BytesReclaimed
            };
            foreach (KahunaBackupGcDeletion d in r.RetentionDeletions)
                response.RetentionDeletions.Add(new GrpcBackupGcDeletion
                {
                    BackupId = d.BackupId.ToString(),
                    Type = d.Type,
                    CreatedAtUtc = d.CreatedAtUtc.ToString("O"),
                    Bytes = d.Bytes,
                    Reason = d.Reason
                });
            foreach (KahunaBackupGcOrphan o in r.OrphanReclamations)
                response.OrphanReclamations.Add(new GrpcBackupGcOrphan
                {
                    Name = o.Name,
                    IsDirectory = o.IsDirectory,
                    Reason = o.Reason
                });
            return response;
        });

    /// <summary>
    /// Runs a handler, translating a typed <see cref="KahunaBackupException"/> into an
    /// <see cref="RpcException"/> whose trailers carry the <see cref="KahunaBackupOutcome"/> so the
    /// client can reconstruct the typed exception. Other exceptions propagate unchanged.
    /// </summary>
    private static async Task<T> Guarded<T>(Func<Task<T>> body)
    {
        try
        {
            return await body();
        }
        catch (KahunaBackupException ex)
        {
            Metadata trailers = new() { { KahunaBackupWire.OutcomeGrpcTrailer, ex.Outcome.ToString() } };
            throw new RpcException(new Status(MapStatus(ex.Outcome), ex.Message), trailers);
        }
    }

    private static StatusCode MapStatus(KahunaBackupOutcome outcome) => outcome switch
    {
        KahunaBackupOutcome.NotConfigured => StatusCode.Unavailable,
        KahunaBackupOutcome.RetryableLeadershipLoss => StatusCode.Unavailable,
        KahunaBackupOutcome.Cancelled => StatusCode.Cancelled,
        KahunaBackupOutcome.IoError => StatusCode.Internal,
        KahunaBackupOutcome.ParentMissing => StatusCode.NotFound,
        KahunaBackupOutcome.TargetConflict => StatusCode.AlreadyExists,
        _ => StatusCode.FailedPrecondition
    };

    private void RequireBackup()
    {
        if (!kahuna.IsBackupConfigured)
            throw new KahunaBackupException(KahunaBackupOutcome.NotConfigured,
                "Backup is not configured on this node.");
    }

    private static GrpcBackupInfoResponse ToGrpc(KahunaBackupInfo b) => new()
    {
        BackupId = b.BackupId.ToString(),
        Type = b.Type,
        CreatedAtUtc = b.CreatedAtUtc.ToString("O"),
        ParentBackupId = b.ParentBackupId?.ToString() ?? "",
        PartitionCount = b.PartitionCount,
        HasSnapshotTime = b.ClusterSnapshotPhysical.HasValue,
        SnapshotNode = b.ClusterSnapshotNode ?? 0,
        SnapshotPhysical = b.ClusterSnapshotPhysical ?? 0,
        SnapshotCounter = b.ClusterSnapshotCounter ?? 0,
        RequestedKind = b.RequestedKind ?? "",
        ActualKind = b.ActualKind ?? "",
        SubstitutionReason = b.SubstitutionReason ?? "",
        FormatVersion = b.FormatVersion,
        IsInvalid = b.IsInvalid,
        InvalidReason = b.InvalidReason ?? "",
        HasCoverage = b.MinRecoverablePhysicalMs.HasValue,
        MinRecoverablePhysicalMs = b.MinRecoverablePhysicalMs ?? 0,
        MaxRecoverablePhysicalMs = b.MaxRecoverablePhysicalMs ?? 0
    };
}
