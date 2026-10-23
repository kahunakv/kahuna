
using Nixie;
using Kommander.WAL.IO;
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Answers many point keys of one key space in one actor turn, optionally taking a Shared <c>[k,k]</c> range lock
/// on each key just before reading it.
///
/// <para>Each key is answered by the same rules as a single <c>TryGet</c>: stage 1 is
/// <see cref="TryGetHandler.ResolvePointRead"/> itself, and a key that needs the backend gets the read a single get
/// would get (see <see cref="ProbeManyContinuation"/>). Every key of a key space hashes to one actor, so the batch
/// is one mailbox message. Keys that need the backend cost one scheduler hand-off and one resume turn for the whole
/// batch, not one of each per key. A transactional latest read, which loads a missing head inline, loads all the
/// missing heads of the batch with one batched backend read first.</para>
///
/// <para>With <see cref="KeyValueProbeLock.Shared"/>, key k is locked by <see cref="TryAcquireExclusiveRangeLockHandler"/>
/// itself, as a Shared acquire of <c>[k,k]</c>, and then read, in this turn: no write to k is applied between the
/// grant and the read. The grant is an ordinary range lock, so release by bounds, range transfer, the lock listing and
/// the collector see nothing new. The first refusal stops the batch: the refused key answers the refusal and its
/// holder, the keys after it answer <c>MustRetry</c> with no lock and no value, and the keys before it keep their
/// locks and their answers.</para>
/// </summary>
internal sealed class TryProbeManyHandler : BaseHandler
{
    private readonly TryGetHandler getHandler;

    private readonly TryAcquireExclusiveRangeLockHandler rangeLockHandler;

    public TryProbeManyHandler(KeyValueContext context, TryGetHandler getHandler, TryAcquireExclusiveRangeLockHandler rangeLockHandler) : base(context)
    {
        this.getHandler = getHandler;
        this.rangeLockHandler = rangeLockHandler;
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        string[]? keys = message.ProbeKeys;

        if (keys is null || !IsWellFormed(message, keys, context.Configuration.MaxProbeBatchSize))
            return KeyValueStaticResponses.InvalidInputResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        KeyValueProbeResult[] results = new KeyValueProbeResult[keys.Length];
        Transactions.Data.ForeignDecisionHint[]? hints = message.ProbeHints;

        KeyValueRequest readView = new(
            KeyValueRequestType.TryGet, message.TransactionId, HLCTimestamp.Zero, keys[0], null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, message.Durability, 0, 0, default);

        KeyValueRequest? lockView = message.ProbeLock == KeyValueProbeLock.Shared
            ? NewLockView(message, KeyValueKeySpace.OfKey(keys[0])!)
            : null;

        Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads = await PreloadMissingHeads(message, keys);

        // A probe served inline from inside another request's turn, or driven without an actor, finishes its
        // backend reads in this turn instead of deferring a reply that is not its own.
        bool deferReply = context.ActorContext is not null && message.Inline is null;

        ProbeManyContinuation? deferred = null;

        for (int i = 0; i < keys.Length; i++)
        {
            string key = keys[i];
            ForeignDecisionHint hint = hints is null ? default : hints[i];
            bool locked = false;

            if (message.ProbeLock == KeyValueProbeLock.SharedReentryOnly)
            {
                if (!TryRenewOwnPointLock(message, key, currentTime))
                {
                    results[i] = KeyValueProbeResult.MustRetry;
                    continue;
                }

                locked = true;
            }
            else if (lockView is not null)
            {
                lockView.StartKey = key;
                lockView.EndKey = key;
                lockView.ForeignDecisionHint = hint;

                KeyValueResponse grant = rangeLockHandler.Execute(lockView);

                if (grant.Type != KeyValueResponseType.Locked)
                {
                    results[i] = new(grant.Type, null, grant.HolderTransactionId, false);

                    for (int j = i + 1; j < keys.Length; j++)
                        results[j] = KeyValueProbeResult.MustRetry;

                    break;
                }

                locked = true;
            }

            readView.Reset(
                KeyValueRequestType.TryGet, message.TransactionId, HLCTimestamp.Zero, key, null, null, -1,
                KeyValueFlags.None, 0, HLCTimestamp.Zero, message.Durability, 0, 0, default);
            readView.ReadTimestamp = message.ReadTimestamp;
            readView.ForeignDecisionHint = hint;

            PointReadStep step;
            try
            {
                step = await getHandler.ResolvePointRead(readView, currentTime, preloadedHeads);
            }
            catch (ReadBackpressureExceededException)
            {
                // The inline load of this key's head was refused before it touched any state: the key answers
                // MustRetry, as a single read does, and the keys before it keep their locks and answers.
                results[i] = KeyValueProbeResult.MustRetry with { Locked = locked };
                continue;
            }

            if (step.Detach == PointReadDetach.None)
            {
                results[i] = ProbeManyContinuation.ToResult(step.Response, locked);
                continue;
            }

            deferred ??= new(results, deferReply ? ReplyOf(context.ActorContext) : default);

            if (!AttachRead(deferred, i, locked, key, message.ReadTimestamp, step, currentTime, mayJoin: deferReply))
                results[i] = KeyValueProbeResult.MustRetry with { Locked = locked };
        }

        if (deferred is null)
            return KeyValueResponse.ForProbe(results);

        return await Dispatch(message, deferred, keys, currentTime);
    }

    /// <summary>
    /// The request shape a batched probe must have: one to <paramref name="maxKeys"/> non-empty keys of one key
    /// space, and a transaction when it locks. A key with no <c>'/'</c> is its own key space and hashes to an actor
    /// of its own, so it can only be probed alone.
    /// </summary>
    internal static bool IsWellFormed(KeyValueRequest message, string[] keys, int maxKeys)
    {
        if (keys.Length == 0 || keys.Length > maxKeys)
            return false;

        if (message.ProbeLock != KeyValueProbeLock.None && (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0))
            return false;

        if (message.ProbeHints is { } hints && hints.Length != keys.Length)
            return false;

        if (!SameKeySpace(keys))
            return false;

        // A key with no separator belongs to no bucket, so no write path ever consults a range lock on it.
        return message.ProbeLock == KeyValueProbeLock.None || keys[0].IndexOf('/') >= 0;
    }

    /// <summary>True when every key is non-empty and has the key space of the first key.</summary>
    internal static bool SameKeySpace(IReadOnlyList<string> keys)
    {
        string first = keys[0];
        if (string.IsNullOrEmpty(first))
            return false;

        int separator = first.LastIndexOf('/');
        if (separator < 0)
            return keys.Count == 1;

        ReadOnlySpan<char> keySpace = first.AsSpan(0, separator);

        for (int i = 1; i < keys.Count; i++)
        {
            string key = keys[i];
            if (string.IsNullOrEmpty(key) || key.LastIndexOf('/') != separator || !key.AsSpan(0, separator).SequenceEqual(keySpace))
                return false;
        }

        return true;
    }

    private static KeyValueRequest NewLockView(KeyValueRequest message, string keySpace)
    {
        KeyValueRequest view = new(
            KeyValueRequestType.TryAcquireExclusiveRangeLock, message.TransactionId, HLCTimestamp.Zero, keySpace, null, null, -1,
            KeyValueFlags.None, message.ExpiresMs, HLCTimestamp.Zero, message.Durability, 0, 0, default);

        view.StartInclusive = true;
        view.EndInclusive = true;
        view.RangeLockMode = RangeLockMode.Shared;
        return view;
    }

    /// <summary>
    /// Renews this transaction's own live <c>[k,k]</c> range lock on <paramref name="key"/> and returns true, or
    /// returns false when it holds none. Takes no lock.
    /// </summary>
    private bool TryRenewOwnPointLock(KeyValueRequest message, string key, HLCTimestamp currentTime) =>
        RangeLockChecks.TryRenewOwnLock(
            context, KeyValueKeySpace.OfKey(key) ?? key, message.TransactionId, key, true, key, true, message.ExpiresMs, currentTime);

    /// <summary>
    /// For a transactional latest read of persistent keys, loads every head that is not resident with one batched
    /// backend read before the keys are processed, so the inline hydration each such key needs takes its row from
    /// here instead of one scheduler round trip per key. The actor holds its turn across the read, as a single
    /// transactional read does across its own, so no state of these keys changes before they use the rows.
    /// </summary>
    private async ValueTask<Dictionary<string, ReadOnlyKeyValueEntry?>?> PreloadMissingHeads(KeyValueRequest message, string[] keys)
    {
        if (message.TransactionId == HLCTimestamp.Zero || !message.ReadTimestamp.IsNull()
            || message.Durability != KeyValueDurability.Persistent)
            return null;

        List<string>? missing = null;
        foreach (string key in keys)
        {
            if (!context.Store.TryGetValue(key, out _))
                (missing ??= []).Add(key);
        }

        if (missing is null)
            return null;

        string[] missingKeys = [.. missing];
        ReadOnlyKeyValueEntry?[] rows = context.BackendReadScheduler is null
            ? context.PointReadExecutor.ExecuteBatch(missingKeys)
            : await context.BackendReadScheduler.EnqueueTask(
                ResolvePartition(missingKeys[0]),
                (context.PointReadExecutor, missingKeys),
                static state => state.PointReadExecutor.ExecuteBatch(state.missingKeys));

        Dictionary<string, ReadOnlyKeyValueEntry?> preloaded = new(rows.Length, StringComparer.Ordinal);
        for (int i = 0; i < rows.Length; i++)
            preloaded[missingKeys[i]] = rows[i];

        return preloaded;
    }

    /// <summary>
    /// Gives key <paramref name="index"/> the backend read stage 1 asked for: joins the read already in flight for
    /// it, or registers one this probe owns under the slot a single read of the key would use. Returns false when a
    /// read is in flight but <paramref name="mayJoin"/> is false: a probe that finishes inside its turn cannot wait
    /// for a read that only this actor's next turn completes, so the key answers <c>MustRetry</c>.
    /// </summary>
    private bool AttachRead(ProbeManyContinuation deferred, int index, bool locked, string key, HLCTimestamp readTimestamp, PointReadStep step, HLCTimestamp currentTime, bool mayJoin)
    {
        if (step.Detach == PointReadDetach.LatestMiss)
        {
            (string, long, bool) slot = (key, -1L, false);

            if (context.PendingReads.TryGetValue(slot, out ReadContinuation? inflight))
            {
                if (!mayJoin)
                    return false;

                Join(deferred, index, locked, inflight);
                return true;
            }

            PointReadContinuation read = new(key, KeyValueResponseType.Get, default);
            ArmReadDeadline(read, currentTime);
            context.PendingReads[slot] = read;
            deferred.AddLatestRead(index, locked, key, read);
            return true;
        }

        (string, HLCTimestamp, bool) snapshotSlot = (key, readTimestamp, false);

        if (context.PendingSnapshotReads.TryGetValue(snapshotSlot, out ReadContinuation? inflightSnapshot))
        {
            if (!mayJoin)
                return false;

            Join(deferred, index, locked, inflightSnapshot);
            return true;
        }

        SnapshotReadContinuation snapshot = new(
            key, readTimestamp, KeyValueResponseType.Get,
            hydrate: step.Detach == PointReadDetach.SnapshotHydrate, asOfCeiling: step.AsOfCeiling, default);
        ArmReadDeadline(snapshot, currentTime);
        context.PendingSnapshotReads[snapshotSlot] = snapshot;
        deferred.AddSnapshotRead(index, locked, snapshot);
        return true;
    }

    private static void Join(ProbeManyContinuation deferred, int index, bool locked, ReadContinuation inflight)
    {
        TaskCompletionSource<KeyValueResponse?> answer = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // A read that already expired, or has its fill of waiters, turns the key away exactly as it turns away
        // a single read.
        if (!inflight.AddWaiter(answer))
            answer.SetResult(KeyValueStaticResponses.MustRetryResponse);

        deferred.AddJoinedRead(index, locked, answer.Task);
    }

    /// <summary>
    /// Schedules the probe's own backend reads as one operation, and sends one <c>ResumeRead</c> once they and the
    /// reads it joined have finished. Without an actor context (a handler driven outside its actor), or for a probe
    /// served inline from inside another request's turn (whose reply is not this probe's to defer), the same two
    /// stages run here and the answer is returned directly.
    /// </summary>
    private async ValueTask<KeyValueResponse> Dispatch(KeyValueRequest message, ProbeManyContinuation deferred, string[] keys, HLCTimestamp currentTime)
    {
        Persistence.Backend.IPersistenceBackend backend = context.PersistenceBackend;
        Persistence.KeyValuePointReadExecutor pointReads = context.PointReadExecutor;
        int recentRevisions = context.Configuration.RevisionRetention;

        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse>? actorContext = context.ActorContext;

        if (actorContext is null || message.Inline is not null)
        {
            // No joined reads here: a probe that finishes inside its turn never joins (see AttachRead).
            if (deferred.HasOwnReads)
            {
                if (context.BackendReadScheduler is null)
                    deferred.Load(backend, pointReads, recentRevisions);
                else
                    await context.BackendReadScheduler.EnqueueTask(ResolvePartition(keys[0]), () =>
                    {
                        deferred.Load(backend, pointReads, recentRevisions);
                        return true;
                    });
            }

            return deferred.Complete(context);
        }

        if (!actorContext.Reply.HasValue)
        {
            deferred.FailBeforeDispatch(context);
            return KeyValueStaticResponses.ErroredResponse;
        }

        ArmReadDeadline(deferred, currentTime);

        Task? stage2 = null;

        if (deferred.HasOwnReads)
        {
            try
            {
                stage2 = context.BackendReadScheduler.EnqueueTask(ResolvePartition(keys[0]), deferred, continuation =>
                {
                    continuation.Load(backend, pointReads, recentRevisions);
                    return true;
                });
            }
            catch (Exception ex)
            {
                // Backpressure or scheduler rejection: every pending key answers MustRetry now, and the reads the
                // probe registered are failed so single reads that joined them retry too.
                context.Logger.LogWarning(
                    "KeyValueActor/TryProbeMany: read scheduler rejected enqueue for {Count} keys of {KeySpace}: {Ex}",
                    keys.Length, KeyValueKeySpace.OfKey(keys[0]), ex.Message);
                return deferred.FailBeforeDispatch(context);
            }
        }

        context.PendingProbeBatches.Add(deferred);

        Task finished = (stage2, deferred.JoinedReads) switch
        {
            ({ } own, null) => own,
            (null, { } joined) => Task.WhenAll(joined),
            ({ } own, { } joined) => Task.WhenAll([own, .. joined]),
            _ => Task.CompletedTask
        };

        _ = finished.ContinueWith(t =>
        {
            // Runs on a thread-pool thread: only the continuations' own stage-2 fields were touched; every
            // actor-owned mutation waits for the ResumeRead message. A joined read never faults (its waiter is a
            // completion source that only receives results), so a fault here is the probe's own backend read.
            if (!t.IsCompletedSuccessfully)
                deferred.SetFaulted();

            actorContext.Self.Send(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = deferred });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    private static KeyValueReplyRef ReplyOf(IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse>? actorContext) =>
        actorContext is not null && actorContext.Reply.HasValue ? KeyValueReplyRef.From(actorContext.Reply.Value) : default;
}
