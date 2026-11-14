
using Kommander;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections.Concurrent;
using Google.Protobuf.Collections;

using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;


namespace Kahuna.Server.Communication.Internode;

/// <summary>Key-range forwarding: seed, remove, lock and history transfer, and transaction-state transfer.</summary>
public partial class GrpcInterNodeCommunication
{
    public async Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcEnsureKeyRangeSeededRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcEnsureKeyRangeSeededResponse remoteResponse = batchResponse.EnsureKeyRangeSeeded!;
        return remoteResponse.Success;
    }

    public async Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcEnsureKeyRangeRemovedRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcEnsureKeyRangeRemovedResponse remoteResponse = batchResponse.EnsureKeyRangeRemoved!;
        return remoteResponse.Success;
    }

    public async Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetRangeLocksRequest request = new()
        {
            KeySpace = keySpace,
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetRangeLocksResponse remoteResponse = batchResponse.GetRangeLocks!;

        List<KeyValueRangeLock> locks = new(remoteResponse.Locks.Count);
        foreach (GrpcRangeLockEntry entry in remoteResponse.Locks)
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
        return locks;
    }

    public async Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcImportRangeLocksRequest request = new()
        {
            KeySpace = keySpace,
        };

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

            request.Locks.Add(entry);
        }

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        _ = batchResponse.ImportRangeLocks;
    }

    public async Task<bool> ImportCompletionReceipts(string node, int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts, CancellationToken cancellationToken, bool forget = false)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcImportCompletionReceiptsRequest request = new() { DestinationPartitionId = partitionId, Forget = forget };

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

            if (receipt.RecordAnchorKey is not null) entry.RecordAnchorKey = receipt.RecordAnchorKey;

            request.Receipts.Add(entry);
        }

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return batchResponse.ImportCompletionReceipts?.Success ?? false;
    }

    public async Task<bool> ReplicateKeyValueRangePage(string node, int partitionId, byte[] page, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcReplicateKeyValueRangePageRequest request = new()
        {
            PartitionId = partitionId,
            Page = UnsafeByteOperations.UnsafeWrap(page)
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return batchResponse.ReplicateKeyValueRangePage?.Success ?? false;
    }

    public async Task<Kahuna.Server.KeyValues.Ranges.RangeHeldHistoryPage> GetRangeHeldHistoryPage(
        string node, int partitionId, string keySpace, string? startKey, string? endKey, IReadOnlyList<HLCTimestamp> pins, HLCTimestamp tombstonesSince, string? cursor, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetRangeTransactionStateRequest request = new()
        {
            PartitionId = partitionId,
            Kinds = (int)KeyValueRangeStateKinds.HeldHistory,
            KeySpace = keySpace,
            TombstonesSince = new GrpcHistoryPin { Node = tombstonesSince.N, Physical = tombstonesSince.L, Counter = tombstonesSince.C }
        };

        if (startKey is not null)
            request.StartKey = startKey;
        if (endKey is not null)
            request.EndKey = endKey;
        if (cursor is not null)
            request.Cursor = cursor;

        foreach (HLCTimestamp pin in pins)
            request.Pins.Add(new GrpcHistoryPin { Node = pin.N, Physical = pin.L, Counter = pin.C });

        GrpcServerBatcherResponse batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetRangeTransactionStateResponse? response = batchResponse.GetRangeTransactionState;

        if (response is null || !response.Success)
            return Kahuna.Server.KeyValues.Ranges.RangeHeldHistoryPage.NotLeader;

        return new Kahuna.Server.KeyValues.Ranges.RangeHeldHistoryPage(
            true,
            response.HeldHistoryServed,
            response.HeldHistory.ToByteArray(),
            [.. response.RefusedPins.Select(static pin => new HLCTimestamp(pin.Node, pin.Physical, pin.Counter))],
            response.HasMore,
            response.HasNextCursor ? response.NextCursor : null);
    }

    public async Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents, byte[] CommittedHeads, bool HasMore, string? NextCursor)> GetRangeTransactionState(string node, int partitionId, string? startKey, string? endKey, KeyValueRangeStateKinds kinds, string? cursor, int maxItems, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetRangeTransactionStateRequest request = new()
        {
            PartitionId = partitionId,
            Kinds = (int)kinds,
            MaxItems = maxItems
        };

        if (startKey is not null)
            request.StartKey = startKey;
        if (endKey is not null)
            request.EndKey = endKey;
        if (cursor is not null)
            request.Cursor = cursor;

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetRangeTransactionStateResponse? response = batchResponse.GetRangeTransactionState;

        if (response is null || !response.Success)
            return (false, [], [], [], [], false, null);

        List<CompletionReceiptRecord> receipts = new(response.Receipts.Count);

        foreach (GrpcCompletionReceiptEntry entry in response.Receipts)
        {
            receipts.Add(new CompletionReceiptRecord(
                new HLCTimestamp(entry.TransactionIdNode, entry.TransactionIdPhysical, entry.TransactionIdCounter),
                entry.Key,
                entry.HasRecordAnchorKey ? entry.RecordAnchorKey : null,
                (KeyValueDurability)entry.Durability));
        }

        return (
            true,
            receipts,
            response.TransactionRecords.ToByteArray(),
            response.PreparedIntents.ToByteArray(),
            response.CommittedHeads.ToByteArray(),
            response.HasMore,
            response.HasNextCursor ? response.NextCursor : null);
    }

    public async Task<(bool Serviced, IReadOnlyList<KeyValueStagedBaseVerdictEntry> Verdicts)> GetStagedBaseVerdicts(string node, int partitionId, HLCTimestamp transactionId, long epoch, IReadOnlyList<string> keys, IReadOnlyList<KeyValueStagedBaseProbe>? probes, int waitMs, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetStagedBaseVerdictsRequest request = new()
        {
            PartitionId = partitionId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch = epoch,
            WaitMs = waitMs
        };

        for (int i = 0; i < keys.Count; i++)
            request.Keys.Add(keys[i]);

        if (probes is not null && probes.Count == keys.Count)
        {
            for (int i = 0; i < probes.Count; i++)
            {
                request.Probes.Add(new GrpcStagedBaseProbe
                {
                    BaseRevision = probes[i].BaseRevision,
                    BaseState = probes[i].BaseState,
                    Revision = probes[i].Revision,
                    NoRevision = probes[i].NoRevision
                });
            }
        }

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcGetStagedBaseVerdictsResponse? response = batchResponse.GetStagedBaseVerdicts;

        if (response is null || !response.Serviced || response.Verdicts.Count != keys.Count)
            return (false, []);

        KeyValueStagedBaseVerdictEntry[] verdicts = new KeyValueStagedBaseVerdictEntry[response.Verdicts.Count];

        for (int i = 0; i < response.Verdicts.Count; i++)
            verdicts[i] = new((KeyValueStagedBaseVerdict)response.Verdicts[i].Verdict, response.Verdicts[i].HeadRevision);

        return (true, verdicts);
    }
}
