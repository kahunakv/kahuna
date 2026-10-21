using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>Raft log apply, restore, and prepare rejection bookkeeping of <see cref="PreparedIntentStore"/>.</summary>
internal sealed partial class PreparedIntentStore
{
    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log, partitionId, replay: true);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log, partitionId, replay: false);

    // ── materializing resolve ─────────────────────────────────────────────────────
    //
    // A committing resolve that carries MaterializeOnResolve makes its own apply the owner of the committed value:
    // every replica installs the value from its copy of the intent, in log order, before the settle removes the
    // intent. The install runs before the resolve applies, so the unflushed overlay already holds the row when the
    // resolve's convergence hook checks for it and when the remove decides whether to retain the settled intent
    // until the row is durable. The intent peek runs without the apply gate: the partition's ordered apply path is
    // the only writer of the key's live intent, and the resolve only ever follows a canonical commit, so installing
    // the value is correct even if a purge removed the intent in between.

    private IResolvedIntentInstaller? resolvedIntentInstaller;

    /// <summary>Wires the installer a materializing resolve uses (manager construction). Without one, a
    /// materializing resolve applies as a plain resolve and installs nothing — the pure/in-memory configuration.</summary>
    public void AttachResolvedIntentInstaller(IResolvedIntentInstaller installer) => resolvedIntentInstaller = installer;

    /// <summary>The attached installer, so a test can wrap it and observe which replica installed what.</summary>
    internal IResolvedIntentInstaller? ResolvedIntentInstaller => resolvedIntentInstaller;

    /// <summary>
    /// Installs the committed value a materializing resolve names, when this node holds it. Returns whether an
    /// install ran. The source is the live intent of the same transaction attempt, unless it is already aborted
    /// (the state machine refuses the commit then). On a restart replay the live set may no longer hold it: the
    /// prepare lay inside the history window and was kept as replay history (see <see cref="TryGetReplayHistoryIntent"/>),
    /// or the snapshot was written after the settle and the source is the settled intent retained until its row
    /// is durable. Absent from all three on a replay, the installer verifies the row is already durable here
    /// (the settle applied before the crash and the row flushed before the snapshot dropped the retained copy)
    /// and otherwise counts the entry as an unresolved materialization of the restart.
    /// </summary>
    private bool TryInstallOnResolve(int partitionId, long logIndex, ResolveIntentCommand resolve, bool replay)
    {
        IResolvedIntentInstaller? installer = resolvedIntentInstaller;
        if (installer is null || !resolve.Commit || !resolve.MaterializeOnResolve)
            return false;

        PreparedIntent? source = null;

        if (intents.TryGetValue(resolve.Key, out PreparedIntent? live)
            && live.TransactionId == resolve.TransactionId
            && live.Epoch == resolve.Epoch
            && live.Resolution != PreparedIntentResolution.Aborted)
            source = live;
        else if (replay && TryGetReplayHistoryIntent(partitionId, resolve.TransactionId, resolve.Epoch, resolve.Key, out PreparedIntent? history))
            source = history;
        else if (replay && TryGetSettledIntentAwaitingFlush(resolve.TransactionId, resolve.Epoch, resolve.Key, out PreparedIntent? settled))
            source = settled;

        if (source is null)
        {
            if (replay)
                installer.NoteUnresolvedOnReplay(partitionId, logIndex, resolve.TransactionId, resolve.Epoch, resolve.Key, resolve.CommitTimestamp);

            return false;
        }

        installer.Install(partitionId, logIndex, source, replay);
        return true;
    }

    // The proposer's decoded commands, keyed by the exact byte array handed to Raft, budgeted for one
    // take per co-hosted node. See ProposedDeltaCache for the reuse and lifetime contract; reusing the
    // producer's instances is safe only because commands and their intents are immutable.
    private static readonly ProposedDeltaCache<PreparedIntentCommand[]> locallyProposedDeltas = new();

    /// <summary>Applies a delta and reports whether every PREPARE command in it took ownership of its key. Returns
    /// <see langword="false"/> when any prepare is rejected by the state machine — another transaction already
    /// holds the key, or the same identity re-prepared a divergent mutation — or when the staged-base fence
    /// flagged an applied prepare's validated base as moved (<see cref="PreparedIntentApplyResult.StaleBase"/>).
    /// Neither is an acknowledged prepare: the producer must abort rather than commit a mutation whose
    /// recoverable intent it never owned, or whose base a competitor already re-committed (a lost update).
    /// Resolve/remove deltas carry no prepares, so they always report true.
    ///
    /// <para>A stale-flagged prepare additionally fires the replica veto hook: the acknowledgement that folds
    /// this method's return value is the LEADER's alone, so on any other node the flag would otherwise be a
    /// verdict with no effect — and a leader whose own memory is frozen admits exactly the prepares the healthy
    /// replicas refuse (the fsync-gate fork producer). The hook runs after the applies, outside the gate. The
    /// restore path (<see cref="Restore"/>) deliberately bypasses this method, so replayed history never
    /// vetoes; ordered live catch-up cannot produce a false flag, because heads advance in the same log order
    /// the prepares apply in — with one exception this method handles itself: the catch-up window right after
    /// a whole-partition snapshot install, whose entries are history the installed ledger already reflects
    /// (<see cref="IsHistoricalApply"/>) and which therefore apply without the fence.</para></summary>
    // Leader-local, advisory: why this node's apply refused a prepare's acknowledgement, keyed by the prepare's
    // identity and key, for the finalizer that proposed it to classify its retry — a stale base is final (heads
    // only advance, so every re-ask answers the same), a held key is settlement lag or a live conflict. Taken
    // once by that finalizer; entries no finalizer takes (an abandoned attempt) are bounded by the cap.
    private readonly ConcurrentDictionary<(HLCTimestamp TransactionId, long Epoch, string Key), Writes.PrepareRejectionKind> prepareRejections = new();

    private const int PrepareRejectionsMax = 4_096;

    /// <summary>Remembers why the prepare of <paramref name="transactionId"/> on <paramref name="key"/> was refused.
    /// Also fed by a remote leader's typed answer, so the origin classifies remote and local refusals alike.</summary>
    internal void RecordPrepareRejection(HLCTimestamp transactionId, long epoch, string key, Writes.PrepareRejectionKind kind)
    {
        if (kind == Writes.PrepareRejectionKind.None)
            return;

        (HLCTimestamp, long, string) identity = (transactionId, epoch, key);
        if (prepareRejections.Count >= PrepareRejectionsMax && !prepareRejections.ContainsKey(identity))
            return;

        prepareRejections[identity] = kind;
    }

    /// <summary>Takes (and forgets) the refusal kind memoed for one prepare, or false when none was recorded —
    /// the refusal happened on another node that answered without a kind, or the batch never committed.</summary>
    internal bool TryTakePrepareRejection(HLCTimestamp transactionId, long epoch, string key, out Writes.PrepareRejectionKind kind) =>
        prepareRejections.TryRemove((transactionId, epoch, key), out kind);

    /// <summary>Memos <paramref name="kind"/> under every prepare identity <paramref name="prepareDelta"/> carries —
    /// the origin-side mirror of a remote leader's typed refusal.</summary>
    internal void RecordPrepareRejectionsForDelta(byte[] prepareDelta, Writes.PrepareRejectionKind kind)
    {
        if (kind == Writes.PrepareRejectionKind.None)
            return;

        foreach (PreparedIntentCommand command in DecodeDelta(prepareDelta))
        {
            if (command is PrepareIntentCommand prepare)
                RecordPrepareRejection(prepare.Intent.TransactionId, prepare.Intent.Epoch, prepare.Intent.Key, kind);
        }
    }

    /// <summary>Takes the dominant refusal kind memoed for the prepares of <paramref name="prepareDelta"/> (a
    /// stale base outranks a held key) — the receiving leader's answer to a forwarded bundle whose prepare it
    /// refused.</summary>
    internal Writes.PrepareRejectionKind TakePrepareRejectionForDelta(byte[] prepareDelta)
    {
        Writes.PrepareRejectionKind dominant = Writes.PrepareRejectionKind.None;

        foreach (PreparedIntentCommand command in DecodeDelta(prepareDelta))
        {
            if (command is PrepareIntentCommand prepare
                && TryTakePrepareRejection(prepare.Intent.TransactionId, prepare.Intent.Epoch, prepare.Intent.Key, out Writes.PrepareRejectionKind kind)
                && kind.Outranks(dominant))
                dominant = kind;
        }

        return dominant;
    }

    public bool ApplyDeltaAckPrepares(RaftLog log) => ApplyDeltaAckPrepares(UnattributedPartition, log);

    /// <inheritdoc cref="ApplyDeltaAckPrepares(RaftLog)"/>
    /// <param name="partitionId">The partition whose log carries the delta; selects the committed-head ledger
    /// slice its settlements feed (see <see cref="Apply(PreparedIntentCommand, int)"/>).</param>
    public bool ApplyDeltaAckPrepares(int partitionId, RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (!locallyProposedDeltas.TryTake(log.LogData, out PreparedIntentCommand[]? commands))
            commands = DecodeDelta(log.LogData);

        bool allPreparesAccepted = true;
        List<PreparedIntent>? staleFlagged = null;

        // History below an installed snapshot's reflected position applies without the advisory fence: the
        // ledger is ahead of these entries by construction, so a verdict here could only be a false refusal and
        // a false veto (see IsHistoricalApply).
        HistoryWindow window = WindowOf(partitionId, log.Id);
        bool installed = false;

        foreach (PreparedIntentCommand command in commands)
        {
            if (command is ResolveIntentCommand resolve && TryInstallOnResolve(partitionId, log.Id, resolve, replay: false))
                installed = true;

            PreparedIntentApplyResult result = Apply(command, partitionId, window, replay: false, log.Time, log.Id);
            if (command is PrepareIntentCommand prepare && (result.Outcome == TransactionApplyOutcome.Rejected || result.StaleBase))
            {
                allPreparesAccepted = false;
                if (result.StaleBase)
                    (staleFlagged ??= []).Add(prepare.Intent);

                // A state-machine rejection means another transaction's live intent holds the key; a stale-base
                // flag means the base moved; a settled replay means the transaction is already decided. The
                // proposing finalizer takes this to decide between helping, backing off, aborting at once, and
                // concluding from the record.
                RecordPrepareRejection(
                    prepare.Intent.TransactionId, prepare.Intent.Epoch, prepare.Intent.Key,
                    result.SettledReplay ? Writes.PrepareRejectionKind.Settled
                    : result.AdmissionRefused ? Writes.PrepareRejectionKind.AdmissionRefused
                    : result.StaleBase ? Writes.PrepareRejectionKind.StaleBase
                    : Writes.PrepareRejectionKind.KeyHeld);
            }
        }

        MarkApplied(partitionId, log.Id);

        if (installed)
            resolvedIntentInstaller!.CompleteEntry(partitionId, log.Id, replay: false);

        FireStaleBaseVetoes(staleFlagged);
        return allPreparesAccepted;
    }

    /// <summary>Fires the veto hook for each stale-flagged prepare of one delta apply, with the head revision
    /// re-read at invocation (a benign race with a concurrent settle can only raise it). No gate is held here;
    /// the wiring schedules detached work.</summary>
    private void FireStaleBaseVetoes(List<PreparedIntent>? staleFlagged)
    {
        if (staleFlagged is null || onStaleBaseVeto is null)
            return;

        foreach (PreparedIntent intent in staleFlagged)
        {
            TryFindHead(intent.Key, out CommittedHead head);
            onStaleBaseVeto(intent, head.Revision);
        }
    }

    private bool ApplyLog(RaftLog log, int partitionId, bool replay)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (!locallyProposedDeltas.TryTake(log.LogData, out PreparedIntentCommand[]? commands))
            commands = DecodeDelta(log.LogData);

        HistoryWindow window = WindowOf(partitionId, log.Id);
        bool installed = false;

        foreach (PreparedIntentCommand command in commands)
        {
            if (command is ResolveIntentCommand resolve && TryInstallOnResolve(partitionId, log.Id, resolve, replay))
                installed = true;

            PreparedIntentApplyResult result = Apply(command, partitionId, window, replay, log.Time, log.Id);

            if (!replay)
                continue;

            // A prepare the fully reflected fence folded (a no-op that left no intent) is history for the intent
            // set but the only copy this node has of the value its materialization names: keep it as replay
            // history until its settle replays. The no-op-with-no-intent shape is the fence's alone — the state
            // machine installs a prepare over an empty key and answers a same-identity re-prepare with the
            // existing intent.
            if (command is PrepareIntentCommand prepare)
            {
                if (window == HistoryWindow.FullyReflected && result is { Outcome: TransactionApplyOutcome.IdempotentNoop, Intent: null })
                    RememberReplayHistoryIntent(partitionId, prepare.Intent);
            }
            else if (command is RemoveIntentCommand remove)
            {
                ForgetReplayHistoryIntent(partitionId, remove.TransactionId, remove.Epoch, remove.Key);
            }
        }

        MarkApplied(partitionId, log.Id);

        if (installed)
            resolvedIntentInstaller!.CompleteEntry(partitionId, log.Id, replay);

        return true;
    }

}
