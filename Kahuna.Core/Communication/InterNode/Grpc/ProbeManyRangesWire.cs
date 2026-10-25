using Google.Protobuf;
using Google.Protobuf.Collections;
using Kommander.Time;

using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// The inter-node encoding of a batched range probe's request and its per-seek answers, shared by the node that
/// forwards the probe and the node that serves it, so both sides agree on every field. A seek's open bound travels
/// as an absent bound, and an entry's value as an absent value when it has none, so each answer decodes to exactly
/// the result the serving node produced.
/// </summary>
internal static class ProbeManyRangesWire
{
    public static GrpcTryProbeManyRangesRequest ToRequest(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string prefix,
        IReadOnlyList<KeyValueProbeRange> seeks,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        GrpcTryProbeManyRangesRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Prefix = prefix,
            Durability = (GrpcKeyValueDurability)durability,
            LockMode = (GrpcProbeLockMode)lockMode,
            ExpiresMs = expiresMs,
            ReadMode = (GrpcKeyValueReadMode)readMode
        };

        Reserve(request.Seeks, seeks.Count);

        foreach (KeyValueProbeRange seek in seeks)
        {
            GrpcProbeRange range = new()
            {
                StartInclusive = seek.StartInclusive,
                EndInclusive = seek.EndInclusive,
                Limit = seek.Limit
            };

            if (seek.StartKey is not null)
                range.StartKey = seek.StartKey;

            if (seek.EndKey is not null)
                range.EndKey = seek.EndKey;

            request.Seeks.Add(range);
        }

        return request;
    }

    public static KeyValueProbeRange[] FromRequest(GrpcTryProbeManyRangesRequest request)
    {
        KeyValueProbeRange[] seeks = new KeyValueProbeRange[request.Seeks.Count];

        for (int i = 0; i < seeks.Length; i++)
        {
            GrpcProbeRange range = request.Seeks[i];

            seeks[i] = new(
                range.HasStartKey ? range.StartKey : null,
                range.StartInclusive,
                range.HasEndKey ? range.EndKey : null,
                range.EndInclusive,
                range.Limit);
        }

        return seeks;
    }

    public static void AddItems(RepeatedField<GrpcTryProbeManyRangesResponseItem> target, KeyValueProbeRangeResult[] results)
    {
        Reserve(target, results.Length);

        foreach (KeyValueProbeRangeResult result in results)
        {
            GrpcTryProbeManyRangesResponseItem item = new()
            {
                Type = (GrpcKeyValueResponseType)result.Type,
                HasMore = result.HasMore,
                HolderTransactionIdNode = result.HolderTransactionId.N,
                HolderTransactionIdPhysical = result.HolderTransactionId.L,
                HolderTransactionIdCounter = result.HolderTransactionId.C,
                Locked = result.Locked
            };

            if (result.ResumeKey is not null)
                item.ResumeKey = result.ResumeKey;

            Reserve(item.Entries, result.Items.Count);

            foreach ((string key, ReadOnlyKeyValueEntry entry) in result.Items)
            {
                GrpcKeyValueByPrefixItemResponse row = new()
                {
                    Key = key,
                    Revision = entry.Revision,
                    ExpiresNode = entry.Expires.N,
                    ExpiresPhysical = entry.Expires.L,
                    ExpiresCounter = entry.Expires.C,
                    LastUsedNode = entry.LastUsed.N,
                    LastUsedPhysical = entry.LastUsed.L,
                    LastUsedCounter = entry.LastUsed.C,
                    LastModifiedNode = entry.LastModified.N,
                    LastModifiedPhysical = entry.LastModified.L,
                    LastModifiedCounter = entry.LastModified.C,
                    State = (GrpcKeyValueState)entry.State
                };

                if (entry.Value is not null)
                    row.Value = UnsafeByteOperations.UnsafeWrap(entry.Value);

                item.Entries.Add(row);
            }

            target.Add(item);
        }
    }

    /// <summary>
    /// Decodes the answers for <paramref name="count"/> seeks. A response with a different number of items is a peer
    /// fault: every seek then answers <c>MustRetry</c> with no lock, never a misaligned answer.
    /// </summary>
    public static KeyValueProbeRangeResult[] FromItems(RepeatedField<GrpcTryProbeManyRangesResponseItem> items, int count)
    {
        KeyValueProbeRangeResult[] results = new KeyValueProbeRangeResult[count];

        if (items.Count != count)
        {
            for (int i = 0; i < count; i++)
                results[i] = KeyValueProbeRangeResult.MustRetry;

            return results;
        }

        for (int i = 0; i < count; i++)
        {
            GrpcTryProbeManyRangesResponseItem item = items[i];

            List<(string, ReadOnlyKeyValueEntry)> entries = new(item.Entries.Count);

            foreach (GrpcKeyValueByPrefixItemResponse row in item.Entries)
            {
                entries.Add((row.Key, new(
                    ByteStringPayload.GetArrayOrNull(row.HasValue, row.Value),
                    row.Revision,
                    new(row.ExpiresNode, row.ExpiresPhysical, row.ExpiresCounter),
                    new(row.LastUsedNode, row.LastUsedPhysical, row.LastUsedCounter),
                    new(row.LastModifiedNode, row.LastModifiedPhysical, row.LastModifiedCounter),
                    (KeyValueState)row.State)));
            }

            results[i] = new(
                (KeyValueResponseType)item.Type,
                entries,
                item.HasMore,
                item.HasResumeKey ? item.ResumeKey : null,
                new(item.HolderTransactionIdNode, item.HolderTransactionIdPhysical, item.HolderTransactionIdCounter),
                item.Locked);
        }

        return results;
    }

    /// <summary>The typed refusal of a range probe that never ran: one <c>MustRetry</c> item per requested seek.</summary>
    public static GrpcTryProbeManyRangesResponse MustRetry(GrpcTryProbeManyRangesRequest request)
    {
        GrpcTryProbeManyRangesResponse response = new();
        Reserve(response.Items, request.Seeks.Count);

        for (int i = 0; i < request.Seeks.Count; i++)
            response.Items.Add(new GrpcTryProbeManyRangesResponseItem { Type = (GrpcKeyValueResponseType)KeyValueResponseType.MustRetry });

        return response;
    }

    private static void Reserve<T>(RepeatedField<T> target, int count)
    {
        int required = checked(target.Count + count);
        if (required > target.Capacity)
            target.Capacity = required;
    }
}
