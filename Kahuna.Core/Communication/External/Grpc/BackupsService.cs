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

    public override async Task<GrpcBackupInfoResponse> TakeFullBackup(
        GrpcTakeFullBackupRequest request, ServerCallContext context)
    {
        RequireBackup();
        KahunaBackupInfo info = await kahuna.TakeFullBackupAsync(context.CancellationToken);
        return ToGrpc(info);
    }

    public override async Task<GrpcBackupInfoResponse> TakeIncrementalBackup(
        GrpcTakeIncrementalBackupRequest request, ServerCallContext context)
    {
        RequireBackup();
        if (!Guid.TryParse(request.ParentBackupId, out Guid parentId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid parentBackupId GUID."));
        KahunaBackupInfo info = await kahuna.TakeIncrementalBackupAsync(parentId, context.CancellationToken);
        return ToGrpc(info);
    }

    public override async Task<GrpcBackupInfoResponse> TakeCoordinatedBackup(
        GrpcTakeCoordinatedBackupRequest request, ServerCallContext context)
    {
        RequireBackup();
        KahunaBackupInfo info = await kahuna.TakeCoordinatedBackupAsync(context.CancellationToken);
        return ToGrpc(info);
    }

    public override async Task<GrpcListBackupsResponse> ListBackups(
        GrpcListBackupsRequest request, ServerCallContext context)
    {
        RequireBackup();
        IReadOnlyList<KahunaBackupInfo> list = await kahuna.ListBackupsAsync(context.CancellationToken);
        GrpcListBackupsResponse response = new();
        foreach (KahunaBackupInfo b in list)
            response.Backups.Add(ToGrpc(b));
        return response;
    }

    public override async Task<GrpcListBackupsResponse> GetBackupChain(
        GrpcGetBackupChainRequest request, ServerCallContext context)
    {
        RequireBackup();
        if (!Guid.TryParse(request.LeafBackupId, out Guid leafId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid leafBackupId GUID."));
        IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(leafId, context.CancellationToken);
        GrpcListBackupsResponse response = new();
        foreach (KahunaBackupInfo b in chain)
            response.Backups.Add(ToGrpc(b));
        return response;
    }

    public override async Task<GrpcListBackupsResponse> ValidateChain(
        GrpcValidateChainRequest request, ServerCallContext context)
    {
        RequireBackup();
        if (!Guid.TryParse(request.LeafBackupId, out Guid leafId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid leafBackupId GUID."));
        IReadOnlyList<KahunaBackupInfo> chain = await kahuna.GetBackupChainAsync(leafId, context.CancellationToken);
        GrpcListBackupsResponse response = new();
        foreach (KahunaBackupInfo b in chain)
            response.Backups.Add(ToGrpc(b));
        return response;
    }

    public override async Task<GrpcRestoreResponse> Restore(GrpcRestoreRequest request, ServerCallContext context)
    {
        RequireBackup();
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
            LastAppliedPhysicalMs = r.LastAppliedPhysicalMs
        };
        foreach (KahunaBackupInfo b in r.Chain)
            response.Chain.Add(ToGrpc(b));
        return response;
    }

    private void RequireBackup()
    {
        if (!kahuna.IsBackupConfigured)
            throw new RpcException(new Status(StatusCode.Unavailable,
                "Backup is not configured on this node."));
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
        SnapshotCounter = b.ClusterSnapshotCounter ?? 0
    };
}
