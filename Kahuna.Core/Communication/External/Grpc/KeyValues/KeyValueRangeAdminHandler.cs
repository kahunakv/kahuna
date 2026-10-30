
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;
using static Kahuna.Communication.External.Grpc.KeyValues.KeyValueGrpcWire;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Inter-node key-range administration: seeding, lock and receipt handoff, range pages and transaction-state probes.
/// </summary>
internal sealed class KeyValueRangeAdminHandler
{
    private readonly IKahuna keyValues;

    public KeyValueRangeAdminHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    internal async Task<GrpcEnsureKeyRangeSeededResponse> EnsureKeyRangeSeededInternal(GrpcEnsureKeyRangeSeededRequest request, ServerCallContext context)
    {
        bool success = await keyValues.RegisterKeyRangeAsync(request.KeySpace, context.CancellationToken);
        return new GrpcEnsureKeyRangeSeededResponse { Success = success };
    }

    internal async Task<GrpcEnsureKeyRangeRemovedResponse> EnsureKeyRangeRemovedInternal(GrpcEnsureKeyRangeRemovedRequest request, ServerCallContext context)
    {
        bool success = await keyValues.RemoveKeyRangeAsync(request.KeySpace, context.CancellationToken);
        return new GrpcEnsureKeyRangeRemovedResponse { Success = success };
    }

    internal async Task<GrpcGetRangeLocksResponse> GetRangeLocksInternal(GrpcGetRangeLocksRequest request, ServerCallContext context)
    {
        List<KeyValueRangeLock> locks = await keyValues.GetRangeLocks(request.KeySpace);
        GrpcGetRangeLocksResponse response = new();

        foreach (KeyValueRangeLock rl in locks)
        {
            GrpcRangeLockEntry entry = new()
            {
                TransactionIdNode     = rl.TransactionId.N,
                TransactionIdPhysical = rl.TransactionId.L,
                TransactionIdCounter  = rl.TransactionId.C,
                StartInclusive        = rl.StartInclusive,
                EndInclusive          = rl.EndInclusive,
                Mode                  = (GrpcRangeLockMode)rl.Mode,
                ExpiresNode           = rl.Expires.N,
                ExpiresPhysical       = rl.Expires.L,
                ExpiresCounter        = rl.Expires.C,
            };

            if (rl.StartKey is not null) entry.StartKey = rl.StartKey;
            if (rl.EndKey is not null) entry.EndKey = rl.EndKey;

            response.Locks.Add(entry);
        }

        return response;
    }

    internal async Task<GrpcImportRangeLocksResponse> ImportRangeLocksInternal(GrpcImportRangeLocksRequest request, ServerCallContext context)
    {
        List<KeyValueRangeLock> locks = new(request.Locks.Count);

        foreach (GrpcRangeLockEntry entry in request.Locks)
        {
            locks.Add(new KeyValueRangeLock
            {
                TransactionId  = new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                StartKey       = entry.HasStartKey ? entry.StartKey : null,
                StartInclusive = entry.StartInclusive,
                EndKey         = entry.HasEndKey ? entry.EndKey : null,
                EndInclusive   = entry.EndInclusive,
                Mode           = (RangeLockMode)entry.Mode,
                Expires        = new HLCTimestamp(entry.ExpiresNode, entry.ExpiresPhysical, entry.ExpiresCounter),
            });
        }

        await keyValues.ImportRangeLocks(request.KeySpace, locks);
        return new GrpcImportRangeLocksResponse { Success = true };
    }

    internal async Task<GrpcImportCompletionReceiptsResponse> ImportCompletionReceiptsInternal(GrpcImportCompletionReceiptsRequest request, ServerCallContext context)
    {
        List<CompletionReceiptRecord> receipts = new(request.Receipts.Count);

        foreach (GrpcCompletionReceiptEntry entry in request.Receipts)
        {
            receipts.Add(new CompletionReceiptRecord(
                new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                entry.Key,
                entry.HasRecordAnchorKey ? entry.RecordAnchorKey : null,
                (KeyValueDurability)entry.Durability));
        }

        // The forwarding node routed here because this node is the destination/participant partition leader:
        // replicate the record (handoff) or forget (durable-acknowledgement release) onto that partition's log
        // and report whether it was durable so the caller can gate cutover or the receipt release.
        bool durable = request.Forget
            ? await keyValues.ForgetCompletionReceiptsReplicated(request.DestinationPartitionId, receipts)
            : await keyValues.ImportCompletionReceiptsReplicated(request.DestinationPartitionId, receipts);
        return new GrpcImportCompletionReceiptsResponse { Success = durable };
    }

    internal async Task<GrpcReplicateKeyValueRangePageResponse> ReplicateKeyValueRangePageInternal(GrpcReplicateKeyValueRangePageRequest request, ServerCallContext context)
    {
        // The forwarding node only guessed a replica of the destination partition from its
        // committed map: apply the page locally when this node leads the group, and otherwise
        // relay it to the leader this node's own Raft state resolves — waiting out an in-flight
        // election on the freshly created partition a split targets.
        bool committed = await keyValues.ReplicateKeyValueRangePageOnLeader(
            request.PartitionId, request.Page.ToByteArray(), context.CancellationToken);
        return new GrpcReplicateKeyValueRangePageResponse { Success = committed };
    }

    internal async Task<GrpcGetRangeTransactionStateResponse> GetRangeTransactionStateInternal(GrpcGetRangeTransactionStateRequest request, ServerCallContext context)
    {
        (bool ok, List<CompletionReceiptRecord> receipts, byte[] records, byte[] intents, byte[] heads, bool hasMore, string? nextCursor) =
            await keyValues.GetRangeTransactionStateLocal(
                request.PartitionId,
                request.HasStartKey ? request.StartKey : null,
                request.HasEndKey ? request.EndKey : null,
                // Kinds 0 (an old peer's request) means all three — the pre-paging behaviour.
                (KeyValueRangeStateKinds)request.Kinds,
                request.HasCursor ? request.Cursor : null,
                request.MaxItems,
                context.CancellationToken);

        GrpcGetRangeTransactionStateResponse response = new()
        {
            Success = ok,
            TransactionRecords = records.Length > 0 ? UnsafeByteOperations.UnsafeWrap(records) : ByteString.Empty,
            PreparedIntents = intents.Length > 0 ? UnsafeByteOperations.UnsafeWrap(intents) : ByteString.Empty,
            CommittedHeads = heads.Length > 0 ? UnsafeByteOperations.UnsafeWrap(heads) : ByteString.Empty,
            HasMore = hasMore
        };

        if (nextCursor is not null)
            response.NextCursor = nextCursor;

        foreach (CompletionReceiptRecord receipt in receipts)
        {
            GrpcCompletionReceiptEntry entry = new()
            {
                TransactionIdNode     = receipt.TransactionId.N,
                TransactionIdPhysical = receipt.TransactionId.L,
                TransactionIdCounter  = receipt.TransactionId.C,
                Key                   = receipt.Key,
                Durability            = (int)receipt.Durability,
            };

            if (receipt.RecordAnchorKey is not null)
                entry.RecordAnchorKey = receipt.RecordAnchorKey;

            response.Receipts.Add(entry);
        }

        return response;
    }

    internal async Task<GrpcGetStagedBaseVerdictsResponse> GetStagedBaseVerdictsInternal(GrpcGetStagedBaseVerdictsRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);

        // The probes count only when aligned with the keys; a misaligned set is an ask this node cannot
        // interpret and falls back to the applied-intent read.
        KeyValueStagedBaseProbe[]? probes = null;
        if (request.Probes.Count > 0 && request.Probes.Count == request.Keys.Count)
        {
            probes = new KeyValueStagedBaseProbe[request.Probes.Count];
            for (int i = 0; i < probes.Length; i++)
            {
                GrpcStagedBaseProbe probe = request.Probes[i];
                probes[i] = new KeyValueStagedBaseProbe(probe.BaseRevision, probe.BaseState, probe.Revision, probe.NoRevision);
            }
        }

        (bool serviced, IReadOnlyList<KeyValueStagedBaseVerdictEntry> verdicts) = await keyValues.GetStagedBaseVerdictsLocal(
            request.PartitionId, transactionId, request.Epoch, request.Keys, probes, request.WaitMs, context.CancellationToken);

        GrpcGetStagedBaseVerdictsResponse response = new() { Serviced = serviced };

        for (int i = 0; i < verdicts.Count; i++)
            response.Verdicts.Add(new GrpcStagedBaseVerdict
            {
                Verdict = (int)verdicts[i].Verdict,
                HeadRevision = verdicts[i].HeadRevision
            });

        return response;
    }
}
