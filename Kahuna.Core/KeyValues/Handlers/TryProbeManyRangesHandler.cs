
using Nixie;
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Answers many bounded seeks of one key space in one actor turn, optionally taking a Shared range lock over each
/// seek's bounds just before reading it.
///
/// <para>Each seek is answered by the same rules as a single range page over its bounds and limit: an ephemeral seek
/// is <see cref="TryGetByRangeHandler.GetByRangeEphemeral(KeyValueRequest, HLCTimestamp, int)"/> itself, and a
/// persistent seek is the <see cref="RangeScanContinuation"/> a single page builds, merged by the same code. Every
/// seek reads at the probe's one instant, so a latest probe reads all seeks at one snapshot. The request carries the
/// prefix once, and a prefix hashes to the actor that owns its key space, so the batch is one mailbox message.</para>
///
/// <para>On a backend that serves reads from memory a seek's pages are read and merged in this turn, as a single page
/// does, under one inspection slack shared by the whole batch, so a batch of dead-row-dense seeks cannot hold the
/// mailbox longer than one such page. A seek that needs a disk page otherwise, or that ran out of the shared slack,
/// is read by <see cref="ProbeRangesContinuation"/>: one scheduler operation and one resume turn per round for all
/// such seeks, not one of each per seek.</para>
///
/// <para>With <see cref="KeyValueProbeLock.Shared"/>, a seek's bounds are locked by
/// <see cref="TryAcquireExclusiveRangeLockHandler"/> itself, as a Shared acquire over the same bounds, and then read,
/// in this turn: no write into the bounds is applied between the grant and the stage-1 snapshot, and the grant refuses
/// every later writer. The grant is an ordinary range lock, so release by bounds, range transfer, the lock listing
/// and the collector see nothing new. The first refusal stops the batch: the refused seek answers the refusal and
/// its holder, the seeks after it answer <c>MustRetry</c> with no lock and no entries, and the seeks before it keep
/// their locks and their answers.</para>
/// </summary>
internal sealed class TryProbeManyRangesHandler : BaseHandler
{
    private readonly TryGetByRangeHandler rangeHandler;

    private readonly TryAcquireExclusiveRangeLockHandler rangeLockHandler;

    public TryProbeManyRangesHandler(KeyValueContext context, TryGetByRangeHandler rangeHandler, TryAcquireExclusiveRangeLockHandler rangeLockHandler) : base(context)
    {
        this.rangeHandler = rangeHandler;
        this.rangeLockHandler = rangeLockHandler;
    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        KeyValueProbeRange[]? seeks = message.ProbeRanges;

        if (seeks is null || !IsWellFormed(message.Key, seeks, message.TransactionId, message.ProbeLock, message.ExpiresMs,
                context.Configuration.MaxProbeBatchSize, context.Configuration.MaxProbeRangeEntries, message.ReadMode, message.ReadTimestamp))
            return KeyValueStaticResponses.InvalidInputResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        string prefix = message.Key;
        string keySpace = KeyValueKeySpace.OfPrefix(prefix);

        KeyValueProbeRangeResult[] results = new KeyValueProbeRangeResult[seeks.Length];

        KeyValueRequest readView = new(
            KeyValueRequestType.GetByRange, message.TransactionId, HLCTimestamp.Zero, prefix, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, message.Durability, 0, 0, default);
        readView.ReadTimestamp = message.ReadTimestamp;
        readView.ReadMode = message.ReadMode;
        readView.ForeignScanDecisions = message.ForeignScanDecisions;

        KeyValueRequest? lockView = message.ProbeLock == KeyValueProbeLock.Shared ? NewLockView(message) : null;

        bool persistent = message.Durability != KeyValueDurability.Ephemeral;
        bool pagesInline = context.PersistenceBackend is { ServesReadsFromMemory: true };

        // A probe served inline from inside another request's turn, or driven without an actor, finishes its disk
        // pages in this turn instead of deferring a reply that is not its own.
        bool deferReply = context.ActorContext is not null && message.Inline is null;

        // The dead entries the batch may still inspect in this turn beyond the live ones it needs: one page's worth
        // for the whole batch.
        long slack = KeyValueScanLimits.MaxScanInspectionSlack;

        ProbeRangesContinuation? deferred = null;

        for (int i = 0; i < seeks.Length; i++)
        {
            KeyValueProbeRange seek = seeks[i];
            bool locked = false;

            if (message.ProbeLock == KeyValueProbeLock.SharedReentryOnly)
            {
                if (!RangeLockChecks.TryRenewOwnLock(
                        context, keySpace, message.TransactionId, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive,
                        message.ExpiresMs, currentTime))
                {
                    results[i] = KeyValueProbeRangeResult.MustRetry;
                    continue;
                }

                locked = true;
            }
            else if (lockView is not null)
            {
                lockView.StartKey = seek.StartKey;
                lockView.StartInclusive = seek.StartInclusive;
                lockView.EndKey = seek.EndKey;
                lockView.EndInclusive = seek.EndInclusive;

                KeyValueResponse grant = rangeLockHandler.Execute(lockView);

                if (grant.Type != KeyValueResponseType.Locked)
                {
                    results[i] = KeyValueProbeRangeResult.MustRetry with { Type = grant.Type, HolderTransactionId = grant.HolderTransactionId };

                    for (int j = i + 1; j < seeks.Length; j++)
                        results[j] = KeyValueProbeRangeResult.MustRetry;

                    break;
                }

                locked = true;
            }

            readView.StartKey = seek.StartKey;
            readView.StartInclusive = seek.StartInclusive;
            readView.EndKey = seek.EndKey;
            readView.EndInclusive = seek.EndInclusive;
            readView.Limit = seek.Limit;

            long ownBudget = seek.Limit + 1L;

            if (!persistent)
            {
                KeyValueResponse page = await rangeHandler.GetByRangeEphemeral(readView, currentTime, (int)slack);
                results[i] = ProbeRangesContinuation.ToResult(page, locked);

                if (page.RangeResult?.Observation is { } observation)
                    slack = Math.Max(0, slack - Math.Max(0, observation.EntriesInspected - ownBudget));

                continue;
            }

            // The resident copy of a seek holds the rows its page needs plus the sentinel, not a full scan batch: the
            // copies of all the seeks together then stay within the probe's bound on entries.
            RangeScanContinuation scan = rangeHandler.CreatePersistentScan(readView, currentTime, default, memoryBatch: 1);

            if (pagesInline)
            {
                KeyValueResponse? page = scan.RunInlinePages(context, ownBudget + slack);
                slack = Math.Max(0, slack - Math.Max(0, scan.EntriesInspected - ownBudget));

                if (page is not null)
                {
                    results[i] = ProbeRangesContinuation.ToResult(page, locked);
                    continue;
                }
            }

            deferred ??= new(results, prefix, ResolvePartition(prefix), deferReply ? ReplyOf(context.ActorContext) : default);
            deferred.Add(i, locked, scan);
        }

        if (deferred is null)
            return KeyValueResponse.ForProbeRanges(results);

        return await Dispatch(message, deferred, results, currentTime);
    }

    /// <summary>
    /// The request shape a batched range probe must have: a prefix, one to <paramref name="maxSeeks"/> seeks of at
    /// least one entry each and at most <paramref name="maxEntries"/> entries over all of them, a transaction and a
    /// lease when it locks, and a read timestamp when it is a statement read. Shared with the locator, which refuses
    /// a malformed probe before it routes it.
    /// </summary>
    internal static bool IsWellFormed(
        string? prefix, IReadOnlyList<KeyValueProbeRange> seeks, HLCTimestamp transactionId, KeyValueProbeLock lockMode,
        int expiresMs, int maxSeeks, int maxEntries, KeyValueReadMode readMode, HLCTimestamp readTimestamp)
    {
        if (string.IsNullOrEmpty(prefix) || seeks.Count == 0 || seeks.Count > maxSeeks)
            return false;

        if (lockMode != KeyValueProbeLock.None && (transactionId == HLCTimestamp.Zero || expiresMs < 0))
            return false;

        if (!KeyValueReadModeChecks.IsWellFormed(readMode, readTimestamp))
            return false;

        long entries = 0;

        foreach (KeyValueProbeRange seek in seeks)
        {
            if (seek.Limit < 1)
                return false;

            entries += seek.Limit;
        }

        return entries <= maxEntries;
    }

    private static KeyValueRequest NewLockView(KeyValueRequest message)
    {
        KeyValueRequest view = new(
            KeyValueRequestType.TryAcquireExclusiveRangeLock, message.TransactionId, HLCTimestamp.Zero, message.Key, null, null, -1,
            KeyValueFlags.None, message.ExpiresMs, HLCTimestamp.Zero, message.Durability, 0, 0, default);

        view.RangeLockMode = RangeLockMode.Shared;
        return view;
    }

    /// <summary>
    /// Reads the disk pages of the deferred seeks: one scheduler operation per round for all of them, and one
    /// <c>ResumeRead</c> per round that merges them. Without an actor context (a handler driven outside its actor), or
    /// for a probe served inline from inside another request's turn (whose reply is not this probe's to defer), the
    /// same rounds run here and the answer is returned directly.
    /// </summary>
    private async ValueTask<KeyValueResponse> Dispatch(
        KeyValueRequest message, ProbeRangesContinuation deferred, KeyValueProbeRangeResult[] results, HLCTimestamp currentTime)
    {
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse>? actorContext = context.ActorContext;

        if (actorContext is null || message.Inline is not null)
        {
            Persistence.Backend.IPersistenceBackend backend = context.PersistenceBackend;
            int partitionId = ResolvePartition(message.Key);

            do
            {
                if (context.BackendReadScheduler is null)
                    deferred.ReadPages(backend);
                else
                {
                    try
                    {
                        await context.BackendReadScheduler.EnqueueTask(partitionId, () =>
                        {
                            deferred.ReadPages(backend);
                            return true;
                        });
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogWarning(
                            "KeyValueActor/TryProbeManyRanges: read scheduler rejected the pages of {Count} seeks under {Prefix}: {Ex}",
                            deferred.OpenSeeks, message.Key, ex.Message);

                        deferred.FailOpen();
                        break;
                    }
                }
            }
            while (!deferred.MergePages(context));

            return KeyValueResponse.ForProbeRanges(results);
        }

        if (!actorContext.Reply.HasValue)
        {
            deferred.FailOpen();
            return KeyValueStaticResponses.ErroredResponse;
        }

        ArmReadDeadline(deferred, currentTime);
        context.PendingProbeBatches.Add(deferred);

        // A refused round answered every open seek MustRetry and resolved the deferred reply already.
        deferred.DispatchPages(context);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    private static KeyValueReplyRef ReplyOf(IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse>? actorContext) =>
        actorContext is not null && actorContext.Reply.HasValue ? KeyValueReplyRef.From(actorContext.Reply.Value) : default;
}
