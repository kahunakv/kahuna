
using Google.Protobuf;
using Google.Protobuf.Collections;
using Kommander.Time;

using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// The inter-node encoding of a batched probe's request and its per-key answers, shared by the node that forwards
/// the probe and the node that serves it, so both sides agree on every field. An answer is decoded to exactly the
/// result the serving node produced: whether it carried an entry, and an empty value apart from no value.
/// </summary>
internal static class ProbeManyWire
{
    public static GrpcTryProbeManyRequest ToRequest(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        IReadOnlyList<string> keys,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability)
    {
        GrpcTryProbeManyRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            ReadTimestampNode = readTimestamp.N,
            ReadTimestampPhysical = readTimestamp.L,
            ReadTimestampCounter = readTimestamp.C,
            Durability = (GrpcKeyValueDurability)durability,
            LockMode = (GrpcProbeLockMode)lockMode,
            ExpiresMs = expiresMs
        };

        Reserve(request.Keys, keys.Count);
        foreach (string key in keys)
            request.Keys.Add(key);

        return request;
    }

    public static void AddItems(RepeatedField<GrpcTryProbeManyResponseItem> target, KeyValueProbeResult[] results)
    {
        Reserve(target, results.Length);

        foreach (KeyValueProbeResult result in results)
        {
            GrpcTryProbeManyResponseItem item = new()
            {
                Type = (GrpcKeyValueResponseType)result.Type,
                HolderTransactionIdNode = result.HolderTransactionId.N,
                HolderTransactionIdPhysical = result.HolderTransactionId.L,
                HolderTransactionIdCounter = result.HolderTransactionId.C,
                Locked = result.Locked
            };

            if (result.Entry is { } entry)
            {
                item.HasEntry = true;
                item.Revision = entry.Revision;
                item.ExpiresNode = entry.Expires.N;
                item.ExpiresPhysical = entry.Expires.L;
                item.ExpiresCounter = entry.Expires.C;
                item.LastUsedNode = entry.LastUsed.N;
                item.LastUsedPhysical = entry.LastUsed.L;
                item.LastUsedCounter = entry.LastUsed.C;
                item.LastModifiedNode = entry.LastModified.N;
                item.LastModifiedPhysical = entry.LastModified.L;
                item.LastModifiedCounter = entry.LastModified.C;
                item.State = (GrpcKeyValueState)entry.State;

                if (entry.Value is not null)
                    item.Value = UnsafeByteOperations.UnsafeWrap(entry.Value);
            }

            target.Add(item);
        }
    }

    /// <summary>
    /// Decodes the answers for <paramref name="count"/> keys. A response with a different number of items is a
    /// peer fault: every key then answers <c>MustRetry</c> with no lock, never a misaligned answer.
    /// </summary>
    public static KeyValueProbeResult[] FromItems(RepeatedField<GrpcTryProbeManyResponseItem> items, int count)
    {
        KeyValueProbeResult[] results = new KeyValueProbeResult[count];

        if (items.Count != count)
        {
            Array.Fill(results, KeyValueProbeResult.MustRetry);
            return results;
        }

        for (int i = 0; i < count; i++)
        {
            GrpcTryProbeManyResponseItem item = items[i];

            ReadOnlyKeyValueEntry? entry = item.HasEntry
                ? new(
                    ByteStringPayload.GetArrayOrNull(item.HasValue, item.Value),
                    item.Revision,
                    new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    (KeyValueState)item.State)
                : null;

            results[i] = new(
                (KeyValueResponseType)item.Type,
                entry,
                new(item.HolderTransactionIdNode, item.HolderTransactionIdPhysical, item.HolderTransactionIdCounter),
                item.Locked);
        }

        return results;
    }

    /// <summary>The typed refusal of a probe that never ran: one <c>MustRetry</c> item per requested key.</summary>
    public static GrpcTryProbeManyResponse MustRetry(GrpcTryProbeManyRequest request)
    {
        GrpcTryProbeManyResponse response = new();
        Reserve(response.Items, request.Keys.Count);

        for (int i = 0; i < request.Keys.Count; i++)
            response.Items.Add(new GrpcTryProbeManyResponseItem { Type = (GrpcKeyValueResponseType)KeyValueResponseType.MustRetry });

        return response;
    }

    private static void Reserve<T>(RepeatedField<T> target, int count)
    {
        int required = checked(target.Count + count);
        if (required > target.Capacity)
            target.Capacity = required;
    }
}
