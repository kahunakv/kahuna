
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// The result that each inter-node operation gives when its target node does not answer: the peer cannot be
/// reached, stopped, or did not reply. Both transports return these values. The gRPC transport returns them for
/// a retryable transport failure, and the in-memory transport returns them for a blocked link or a node that is
/// not registered. One copy of each value keeps the two transports from giving different answers.
/// <para>
/// Each result is a retry signal. It does not say that the call did not run on the target: on gRPC a reply can
/// be lost after the call ran. Every caller resolves the outcome with a retry, which routes again.
/// </para>
/// <para>
/// An operation that is not here has no typed result for this case. On gRPC its transport exception goes to
/// the caller, and on the in-memory transport a <see cref="KahunaServerException"/> with
/// <see cref="KeyValueResponseType.MustRetry"/> goes to the caller. <see cref="RetryableFailureClassifier"/>
/// calls both of them retryable.
/// </para>
/// </summary>
internal static class UnansweredResults
{
    public static (LockResponseType, long) TryLock => (LockResponseType.MustRetry, 0);

    public static (LockResponseType, long) TryExtendLock => (LockResponseType.MustRetry, 0);

    public static LockResponseType TryUnlock => LockResponseType.MustRetry;

    public static (LockResponseType, ReadOnlyLockEntry?) GetLock => (LockResponseType.MustRetry, null);

    public static (SequenceResponseType, long) CreateSequence => (SequenceResponseType.MustRetry, 0);

    public static (SequenceResponseType, long) UpdateSequence => (SequenceResponseType.MustRetry, 0);

    public static (SequenceResponseType, ReadOnlySequenceEntry?) GetSequence => (SequenceResponseType.MustRetry, null);

    public static (SequenceResponseType, SequenceAllocation) NextSequenceValue => (SequenceResponseType.MustRetry, default);

    public static (SequenceResponseType, SequenceAllocation) ReserveSequenceRange => (SequenceResponseType.MustRetry, default);

    public static SequenceResponseType DeleteSequence => SequenceResponseType.MustRetry;

    public static (KeyValueResponseType, long, HLCTimestamp) TrySetKeyValue => (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

    public static (KeyValueResponseType, long, HLCTimestamp) TryDeleteKeyValue => (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

    public static (KeyValueResponseType, long, HLCTimestamp) TryExtendKeyValue => (KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> item for each request item.</summary>
    public static void TrySetManyNodeKeyValue(List<KahunaSetKeyValueRequestItem> items, Lock lockSync, List<KahunaSetKeyValueResponseItem> responses)
    {
        lock (lockSync)
        {
            foreach (KahunaSetKeyValueRequestItem item in items)
                responses.Add(new() { Key = item.Key, Type = KeyValueResponseType.MustRetry, Durability = item.Durability });
        }
    }

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> item for each request item.</summary>
    public static void TryDeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items, Lock lockSync, List<KahunaDeleteKeyValueResponseItem> responses)
    {
        lock (lockSync)
        {
            foreach (KahunaDeleteKeyValueRequestItem item in items)
                responses.Add(new() { Key = item.Key, Type = KeyValueResponseType.MustRetry, Durability = item.Durability });
        }
    }

    public static (KeyValueResponseType, ReadOnlyKeyValueEntry?) TryGetValue => (KeyValueResponseType.MustRetry, null);

    public static (KeyValueResponseType, ReadOnlyKeyValueEntry?) TryExistsValue => (KeyValueResponseType.MustRetry, null);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no entry for each requested key.</summary>
    public static void TryGetManyNodeValues(
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses) =>
        AddReadManyMustRetry(keys, lockSync, responses);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no entry for each requested key.</summary>
    public static void TryExistsManyNodeValues(
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses) =>
        AddReadManyMustRetry(keys, lockSync, responses);

    private static void AddReadManyMustRetry(
        List<(string key, long revision, KeyValueDurability durability)> keys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses)
    {
        lock (lockSync)
        {
            foreach ((string key, _, KeyValueDurability durability) in keys)
                responses.Add((KeyValueResponseType.MustRetry, key, durability, null));
        }
    }

    /// <summary>One <see cref="KeyValueProbeResult.MustRetry"/> for each of the <paramref name="keyCount"/> keys, in order.</summary>
    public static KeyValueProbeResult[] TryProbeMany(int keyCount)
    {
        KeyValueProbeResult[] refused = new KeyValueProbeResult[keyCount];
        Array.Fill(refused, KeyValueProbeResult.MustRetry);
        return refused;
    }

    /// <summary>One <see cref="KeyValueProbeRangeResult.MustRetry"/> for each of the <paramref name="seekCount"/> seeks, in order.</summary>
    public static KeyValueProbeRangeResult[] TryProbeManyRanges(int seekCount)
    {
        KeyValueProbeRangeResult[] refused = new KeyValueProbeRangeResult[seekCount];
        for (int i = 0; i < refused.Length; i++)
            refused[i] = KeyValueProbeRangeResult.MustRetry;
        return refused;
    }

    public static KeyValueResponseType TryCheckWriteIntentValue => KeyValueResponseType.MustRetry;

    /// <summary>One <see cref="KeyValueResponseType.MustRetry"/> answer for each probed key, in order.</summary>
    public static List<(KeyValueResponseType type, string key, KeyValueDurability durability)> TryCheckManyWriteIntents(List<KeyValueConflictProbe> keys)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> refused = new(keys.Count);

        foreach (KeyValueConflictProbe probe in keys)
            refused.Add((KeyValueResponseType.MustRetry, probe.Key, probe.Durability));

        return refused;
    }

    /// <summary>The refusal names the requested key and durability, with no holder and no observed base.</summary>
    public static (KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision) TryAcquireExclusiveLock(string key, KeyValueDurability durability) =>
        (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero, PointLockBase.None);

    public static KeyValueResponseType TryAcquireExclusivePrefixLock => KeyValueResponseType.MustRetry;

    public static (KeyValueResponseType, HLCTimestamp HolderTransactionId) TryAcquireRangeLock => (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no holder and no observed base for each key.</summary>
    public static void TryAcquireNodeExclusiveLocks(
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> responses)
    {
        lock (lockSync)
        {
            foreach ((string key, _, KeyValueDurability durability) in xkeys)
                responses.Add((KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero, PointLockBase.None));
        }
    }

    public static (KeyValueResponseType, string) TryReleaseExclusiveLock(string key) => (KeyValueResponseType.MustRetry, key);

    public static KeyValueResponseType TryReleaseExclusivePrefixLock => KeyValueResponseType.MustRetry;

    public static KeyValueResponseType TryReleaseExclusiveRangeLock => KeyValueResponseType.MustRetry;

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer for each key.</summary>
    public static void TryReleaseNodeExclusiveLocks(
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses)
    {
        lock (lockSync)
        {
            foreach ((string key, KeyValueDurability durability) in xkeys)
                responses.Add((KeyValueResponseType.MustRetry, key, durability));
        }
    }

    /// <summary>The refusal names the requested key and durability, with no proposal ticket.</summary>
    public static (KeyValueResponseType, HLCTimestamp, string, KeyValueDurability) TryPrepareMutations(string key, KeyValueDurability durability) =>
        (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no proposal ticket for each key.</summary>
    public static void TryPrepareNodeMutations(
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses)
    {
        lock (lockSync)
        {
            foreach ((string key, KeyValueDurability durability) in xkeys)
                responses.Add((KeyValueResponseType.MustRetry, HLCTimestamp.Zero, key, durability));
        }
    }

    public static (KeyValueResponseType, long) TryCommitMutations => (KeyValueResponseType.MustRetry, 0);

    public static (KeyValueResponseType, long) TryRollbackMutations => (KeyValueResponseType.MustRetry, 0);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no proposal index for each key.</summary>
    public static void TryCommitNodeMutations(
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses) =>
        AddMutationsMustRetry(xkeys, lockSync, responses);

    /// <summary>Adds a <see cref="KeyValueResponseType.MustRetry"/> answer with no proposal index for each key.</summary>
    public static void TryRollbackNodeMutations(
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses) =>
        AddMutationsMustRetry(xkeys, lockSync, responses);

    private static void AddMutationsMustRetry(
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses)
    {
        lock (lockSync)
        {
            foreach ((string key, _, KeyValueDurability durability) in xkeys)
                responses.Add((KeyValueResponseType.MustRetry, key, 0, durability));
        }
    }

    /// <summary>A refusal with no items. A new instance each time, because the item list is mutable.</summary>
    public static KeyValueGetByBucketResult GetByBucket => new(KeyValueResponseType.MustRetry, []);

    /// <summary>A refusal with no page. A new instance each time, because the item list is mutable.</summary>
    public static KeyValueGetByRangeResult GetByRange => new(KeyValueResponseType.MustRetry, [], null, false);

    /// <summary>A refusal with no items. A new instance each time, because the item list is mutable.</summary>
    public static KeyValueGetByBucketResult ScanByPrefix => new(KeyValueResponseType.MustRetry, []);

    /// <summary>No session was opened, so a retry, on this node or another one, is safe.</summary>
    public static (KeyValueResponseType, TransactionHandle) StartTransaction => (KeyValueResponseType.MustRetry, TransactionHandle.None);

    /// <summary>
    /// The commit can be applied or not. The answer keeps that unknown: the commit path is idempotent, and a retry
    /// with the record anchor finds the session or the durable decision. The answer gives back the caller's own
    /// anchor, so the retry keeps it.
    /// </summary>
    public static TransactionCommitAnswer CommitTransaction(TransactionHandle handle) =>
        new(KeyValueResponseType.MustRetry, handle.RecordAnchorKey, CommitRefusalClass.Unresolved, "The forwarded commit was not answered");

    /// <summary>The rollback is idempotent, and a retry finds the session or the durable decision, so a decided commit is never undone.</summary>
    public static KeyValueResponseType RollbackTransaction => KeyValueResponseType.MustRetry;

    public static (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy) BeginOperation =>
        (OperationRegistrationOutcome.AlreadyPending, KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero, null, TransactionConflictPolicy.Normal);

    public static (KeyValueResponseType outcome, string? anchor) CompleteOperation => (KeyValueResponseType.MustRetry, null);

    /// <summary>No working set: the caller reads this the same as a node that holds no session.</summary>
    public static TransactionWorkingSet? GetTransactionWorkingSet => null;

    public static (KeyValueResponseType, TransactionWorkingSet?) CloseTransaction => (KeyValueResponseType.MustRetry, null);

    public static (KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry) AcquireSnapshotHold => (KeyValueResponseType.MustRetry, string.Empty, HLCTimestamp.Zero);

    public static (KeyValueResponseType Type, HLCTimestamp LeaseExpiry) RenewSnapshotHold => (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

    public static KeyValueResponseType ReleaseSnapshotHold => KeyValueResponseType.MustRetry;

    /// <summary>The floor read changes nothing, so the caller finds the meta leader again and asks again.</summary>
    public static (KeyValueResponseType Type, HLCTimestamp Floor, int LiveHolds) GetSnapshotFloor => (KeyValueResponseType.MustRetry, HLCTimestamp.Zero, 0);

    public static HistoryHoldAnswer HistoryHold => HistoryHoldAnswer.Refused(KeyValueResponseType.MustRetry);

    public static (KeyValueResponseType Type, KeyValueApplyFingerprint Fingerprint) GetPartitionApplyFingerprint => (KeyValueResponseType.MustRetry, default);

    /// <summary>No answer: the asker reads the node as one that does not apply a new log type, and asks again later.</summary>
    public static IReadOnlyList<string>? GetAppliedLogTypes => null;

    public static (KeyValueResponseType Type, bool Held, long AppliedLogId) GetPreparedIntentPresence => (KeyValueResponseType.MustRetry, false, 0);

    public static KeyValueResponseType ConfirmRangeQuiesce => KeyValueResponseType.MustRetry;

    public static (KeyValueResponseType Type, int Settled, int Decided, int Undecided) SettleDecidedBlockers => (KeyValueResponseType.MustRetry, 0, 0, 0);
}
