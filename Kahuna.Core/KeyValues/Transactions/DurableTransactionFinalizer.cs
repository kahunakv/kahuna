using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>The per-partition prepared-intent group of one transaction. The anchor partition additionally carries
/// the canonical record's initialization (see <see cref="DurableFinalizeInput.AnchorPartitionId"/>). The
/// <paramref name="Generation"/> is the range-descriptor generation this partition was resolved against at freeze,
/// used to re-fence the prepare submission at dispatch so a split between freeze and dispatch releases it retryably
/// instead of appending to a retired partition.</summary>
internal sealed record DurablePartitionPrepare(int PartitionId, long Generation, IReadOnlyList<PreparedIntent> Intents);

/// <summary>The frozen, immutable inputs of one finalize attempt: identity, the canonical commit timestamp and
/// decision deadline, the participant manifest, and the per-partition prepared intents. Freezing happens in the
/// coordinator before finalize; this type is what the finalizer drives to a durable outcome.</summary>
internal sealed record DurableFinalizeInput(
    HLCTimestamp TransactionId,
    long Epoch,
    string CoordinatorKey,
    string RecordAnchorKey,
    int AnchorPartitionId,
    long AnchorGeneration,
    HLCTimestamp CommitTimestamp,
    HLCTimestamp DecisionDeadline,
    long ManifestHash,
    IReadOnlyList<TransactionParticipantRef> Manifest,
    IReadOnlyList<DurablePartitionPrepare> Partitions,
    HLCTimestamp CreatedAt);

internal enum DurableFinalizeResult
{
    Committed,
    Aborted,
    MustRetry
}

/// <summary>The result of checking every staged write's validated base against current committed state before
/// anything durable is proposed (the write-side compare-and-set).</summary>
internal enum StagedBaseValidation
{
    /// <summary>Every staged write's base still matches the committed state it was validated against.</summary>
    Valid,

    /// <summary>A base moved — another transaction committed the same base first. Committing would silently
    /// discard that write, so the only truthful outcome is a conflict abort.</summary>
    Conflict,

    /// <summary>A base could not be resolved right now (a foreign undecided intent, a read that must wait for
    /// replication): the check is retryable, and nothing may be decided from it.</summary>
    Unknown
}

/// <summary>The outcome of a finalize attempt, already mapped to the MustRetry/Aborted result contract: only a
/// conflict-class abort is <see cref="DurableFinalizeResult.Aborted"/>; every other abort and every
/// infrastructural failure is <see cref="DurableFinalizeResult.MustRetry"/>.
/// <para><paramref name="LateCommitRejected"/> marks a <see cref="DurableFinalizeResult.MustRetry"/> whose commit
/// request the record's deadline gate withheld (the attempt HLC passed the frozen decision deadline), so the
/// coordinator can count that cause once per transaction rather than once per retried attempt.</para></summary>
internal readonly record struct DurableFinalizeOutcome(
    DurableFinalizeResult Result,
    TransactionAbortClass AbortClass,
    bool LateCommitRejected = false,
    // True when Result was read from the canonical record after the decision applied (locally on the anchor
    // leader, or from the anchor leader's typed answer), so the resolution can take its direction from it
    // without a second canonical read.
    bool CanonicalDecisionRead = false);

/// <summary>
/// Drives one transaction's finalize under the durable-intent 2PC model: initialize the canonical
/// record, prepare every participant's durable intent (prepare barrier), validate the read-set, decide the
/// canonical outcome by compare-and-set, then resolve each intent. It owns the protocol sequencing and outcome
/// mapping, behind a replicate seam so the same logic runs against real Raft in production and synchronously in
/// tests. It never truncates a log: abort is a canonical decision, not a rollback.
/// </summary>
internal sealed class DurableTransactionFinalizer : IDisposable
{
    // Local apply is actor work, not Raft I/O. This gate is shared by every finalize using this finalizer — and,
    // when the node hands one in, by its recovery paths too — so concurrent transactions and helping passes
    // cannot multiply their individual fan-out into an unbounded actor-inbox flood.
    internal const int MaxConcurrentLocalApplies = 32;

    // A prepare rejected only because the key still holds a predecessor's committed-but-unsettled intent (deferred
    // settlement removes it a moment later) is retryable in place — re-prepare the set a few times before conceding,
    // so a healthy commit is not aborted merely because background settlement had not caught up. Small: the blocking
    // intent settles within a few ms and the frozen decision deadline is the real ceiling.
    private const int MaxPrepareRetries = 8;

    private readonly SemaphoreSlim localApplyGate;

    // True when this finalizer created its gate and must dispose it; false for a node-shared gate.
    private readonly bool ownsLocalApplyGate;

    /// <summary>Replicates a partition's serialized delta of the given log type and returns whether it committed
    /// durably. In production this is an auto-commit Raft round trip; the finalizer applies the delta to the
    /// local store on success (idempotent with the replication-callback apply on every replica). The
    /// <paramref name="admissionClass"/> tells the shared write scheduler whether this is ordinary work
    /// (record init, prepare) or terminal work that finishes an already-prepared transaction (decision, settle,
    /// materialize) — terminal work draws on reserve capacity so an ordinary-write burst cannot starve it.</summary>
    public delegate Task<bool> ReplicateDelegate(int partitionId, string logType, byte[] logData, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken);

    /// <summary>Like <see cref="ReplicateDelegate"/> but re-fences the submission at dispatch against the range
    /// descriptor <paramref name="fenceKey"/> was resolved to at freeze (<paramref name="fenceGeneration"/>): a
    /// split/merge between freeze and dispatch releases it retryably instead of appending to a retired partition.
    /// Used for the pre-decision record initialization, prepare, and decision; null falls back to the unfenced
    /// replicate (protocol tests, and the post-decision settle/materialize which recovery backstops).</summary>
    public delegate Task<bool> ReplicateFencedDelegate(int partitionId, string logType, byte[] logData, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken);

    /// <summary>Replicates the anchor partition's record initialization and its own prepared-intent group as one
    /// atomic ordered proposal (removing a pre-decision barrier), fenced against the anchor descriptor. Returns two
    /// independent signals: <c>BatchCommitted</c> — the proposal reached Raft so the record is durably initialized;
    /// and <c>PrepareAcknowledged</c> — the anchor prepare took ownership of its key. The two cannot be folded: a
    /// committed batch whose prepare was rejected must drive a truthful abort (the record exists), while a batch that
    /// never committed is a clean retry with nothing durable. Null keeps the unbundled init-then-prepare sequence
    /// (protocol tests, and the fallback when the anchor key routes outside the participant partitions).</summary>
    public delegate Task<(bool BatchCommitted, bool PrepareAcknowledged)> ReplicateAnchorBundleDelegate(int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken);

    /// <summary>Runs the post-decision resolution (materialize committed values, settle intents) on a background
    /// task, off the commit critical path (deferred settlement). When <see langword="null"/>, resolution is instead
    /// awaited inline so it completes before <see cref="FinalizeAsync"/> returns (synchronous settlement). Either
    /// way the canonical decision is already durable, so recovery finishes resolution if a deferred run is lost.</summary>
    public delegate void ResolutionScheduler(Func<CancellationToken, Task> resolution);

    /// <summary>Applies a committed intent's value on the leader's live KV state (clears the committing
    /// transaction's staged write intent + MVCC snapshot and applies the value to the base entry). The replicated
    /// key/value record makes followers converge, but the leader does not apply it through the replication callback,
    /// so this is how the committed value becomes visible on the leader. Null in bare protocol tests (no actor).</summary>
    public delegate Task<bool> ApplyCommitLocally(int partitionId, PreparedIntent intent);

    /// <summary>Clears an aborted transaction's staged write intent + MVCC snapshot on the owning actor (the durable
    /// analog of ApplyConfirmedRollback), so the key is not blocked until the intent lease expires. Null in bare
    /// protocol tests (no actor).</summary>
    public delegate Task<bool> ApplyRollbackLocally(int partitionId, PreparedIntent intent);

    /// <summary>Settles foreign intents blocking <paramref name="intents"/>' keys on <paramref name="partitionId"/>
    /// whose canonical record is already terminal (decided but not yet settled — settlement lag, not a live
    /// conflict), returning how many were settled. Wired to
    /// <see cref="DurableTransactionRecovery.TryResolveDecidedBlockersAsync"/>; null disables the helping pass and
    /// keeps the pure backoff-retry behaviour (bare protocol tests, or a deployment that opts out).</summary>
    public delegate Task<int> ResolveDecidedBlockersDelegate(
        int partitionId, IReadOnlyList<PreparedIntent> intents, HLCTimestamp transactionId, long epoch, CancellationToken cancellationToken);

    /// <summary>Proposes [record init + anchor prepare + commit decision] as ONE atomic durable batch on the
    /// anchor partition — the one-phase commit fast path. When the anchor is led by a remote node the whole
    /// bundle crosses the wire as one typed operation and the reply carries the canonical outcome read on that
    /// leader after the ordered apply; the origin must never infer a commit from the batch signals alone,
    /// because the commit transition is judged at apply, in log order. Returns <see langword="null"/> only when
    /// the remote leader does not implement the typed operation (an older node), in which case the caller falls
    /// back to standard 2PC. Null delegate disables the fast path entirely.</summary>
    public delegate Task<Writes.DurableOnePhaseReply?> ReplicateOnePhaseBundleDelegate(
        int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta,
        HLCTimestamp transactionId, long epoch, HLCTimestamp opId,
        string fenceKey, long fenceGeneration, CancellationToken cancellationToken);

    /// <summary>Checks every frozen intent's validated base (<see cref="PreparedIntent.BaseRevision"/> /
    /// <see cref="PreparedIntent.BaseState"/>) against the key's current committed state — the write-side
    /// compare-and-set that catches a base that moved after staging. The in-memory write-intent lease that
    /// normally prevents this can lapse while a transaction is still finalizing (a killed node's wiped locks, a
    /// stall outliving the lease), letting another transaction commit the same base first; committing over it
    /// would silently discard that write. Null disables the check (protocol tests that fabricate intents).</summary>
    public delegate Task<StagedBaseValidation> ValidateStagedBasesDelegate(DurableFinalizeInput input, CancellationToken cancellationToken);

    /// <summary>Collects the staged-base fence verdicts of every replica of the participant partitions before
    /// the commit decision is proposed — the pre-decision replica fence confirmation. The prepare
    /// acknowledgement folds only the LEADER's fence verdict, and a leader whose committed-head memory is
    /// frozen or freshly restored admits exactly the prepares the healthy replicas refuse; the detached
    /// stale-base veto carried those verdicts but raced the commit at the anchor and lost the race in the
    /// fsync-gate runs. False means a replica proved a validated base moved: the caller must abort with a
    /// truthful conflict. Missing verdicts never block the commit. Runs only on the 2PC path — the one-phase
    /// bundle is restricted to single-process groups for validated-base transactions, where every replica
    /// shares this process's stores. Null disables the confirmation (single-process groups, protocol tests).</summary>
    public delegate Task<bool> ConfirmReplicaFenceDelegate(DurableFinalizeInput input, CancellationToken cancellationToken);

    /// <summary>Replicates the terminal decision delta onto the anchor partition WITHOUT projecting the sent
    /// delta into this node's record store: unlike the record init, a decision can lose at the anchor to one
    /// that already won (a routed presumed abort racing this commit, or the reverse), and the replicate's
    /// success reports only that the batch committed — not that the transition applied. Null falls back to the
    /// ordinary fenced/unfenced replicate, whose sender-side projection is then trusted (single-node and
    /// protocol-test configurations, where the local apply IS the canonical apply).</summary>
    public delegate Task<bool> ReplicateDecisionDelegate(int partitionId, byte[] decisionDelta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken);

    /// <summary>Replicates the terminal decision delta on the anchor partition and answers the CANONICAL outcome
    /// read after the ordered apply — from this node's own store when it leads the anchor, from the anchor
    /// leader's typed answer otherwise — so the winner needs no separate lookup call. An answer that is
    /// replicated but not <c>Known</c> (an older remote node) falls back to the routed lookup. Preferred over
    /// <see cref="ReplicateDecisionDelegate"/> when wired.</summary>
    public delegate Task<Writes.DurableDecisionReply> DecideDelegate(int partitionId, byte[] decisionDelta, HLCTimestamp transactionId, long epoch, string fenceKey, long fenceGeneration, CancellationToken cancellationToken);

    /// <summary>Reads the transaction's canonical record by its anchor key — locally when this node leads the
    /// anchor partition, routed to the anchor leader otherwise. The decision winner and the resolution
    /// direction must come from this, never from a node-local store a losing decision's projection could have
    /// diverged. Null falls back to the local record store (single-node and protocol tests).</summary>
    public delegate Task<TransactionRecord?> LookupRecordRoutedDelegate(HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken);

    private readonly TransactionRecordStore recordStore;

    // Consulted by the one-phase fast path's pre-flight check (foreign durable intent on any written key ⇒
    // fall back to 2PC, whose prepare/retry/helping machinery owns that conflict).
    private readonly PreparedIntentStore intentStore;

    private readonly ReplicateDelegate replicate;

    // Fenced replicate for the pre-decision path; null falls back to the unfenced replicate.
    private readonly ReplicateFencedDelegate? replicateFenced;

    // Bundles the anchor partition's [record init, prepare] into one proposal; null keeps the unbundled sequence.
    private readonly ReplicateAnchorBundleDelegate? replicateAnchorBundle;

    // Null = synchronous settlement: resolution is awaited inline in FinalizeAsync. Non-null = deferred settlement.
    private readonly ResolutionScheduler? scheduleResolution;

    private readonly ApplyCommitLocally? applyCommitLocally;

    private readonly ApplyRollbackLocally? applyRollbackLocally;

    // Null disables the prepare-conflict helping pass; the retry loop then always backs off blind.
    private readonly ResolveDecidedBlockersDelegate? resolveDecidedBlockers;

    // Null disables the one-phase commit fast path; every finalize then runs the standard 2PC flow.
    private readonly ReplicateOnePhaseBundleDelegate? replicateOnePhaseBundle;

    // Null disables the up-front staged-base check (protocol tests that fabricate intents).
    private readonly ValidateStagedBasesDelegate? validateStagedBases;

    // Null disables the pre-decision replica fence confirmation (single-process groups, protocol tests).
    private readonly ConfirmReplicaFenceDelegate? confirmReplicaFence;

    // Projection-free decision replicate; null falls back to the ordinary fenced/unfenced replicate.
    private readonly ReplicateDecisionDelegate? replicateDecision;

    // Typed decision: replicate and read the canonical winner in one round; null keeps replicate-then-lookup.
    private readonly DecideDelegate? decide;

    // Canonical record read for the decision winner and the resolution direction; null reads the local store.
    private readonly LookupRecordRoutedDelegate? lookupRecordRouted;

    // Resolves a key's CURRENT data partition at resolution time, so a materialize running after a range
    // split/merge lands on the key's new owner instead of the partition frozen at prepare time. Null keeps
    // the frozen target (protocol tests, deployments without key-range routing).
    private readonly Func<string, int>? resolveCurrentPartition;

    // Mints a fresh HLC immediately before the terminal transition, used as the attempt's AttemptHlc so elapsed
    // prepare/validate time can actually trip the frozen decision deadline. Null keeps the deprecated behaviour of
    // reusing the operation id as the attempt HLC — used only by protocol tests that supply an explicit late opId.
    private readonly Func<HLCTimestamp>? attemptClock;

    // Records the finalize latency measured up to (and including) the canonical decision — deliberately excluding
    // post-decision resolution/settlement — so the p99 that sizes future decision deadlines is not inflated by
    // work that happens after the decision. Null in tests that do not observe latency.
    private readonly Action<double>? recordDecisionLatencyMs;

    private readonly int maxMaterializationBatchItems;

    private readonly long maxMaterializationBatchBytes;

    // Emits the value-free by-reference materialization record instead of copying the committed value into the
    // log a second time. Off unless every node in the cluster applies that record (see the configuration flag's
    // upgrade order); an older node skips an unknown message type, which loses the write on that node.
    private readonly bool materializeByReference;

    public DurableTransactionFinalizer(
        TransactionRecordStore recordStore,
        // Applied by the ordered scheduler-completion path (the single apply owner), never mutated by the
        // finalizer; read here only by the one-phase fast path's pre-flight foreign-intent check.
        PreparedIntentStore intentStore,
        ReplicateDelegate replicate,
        ResolutionScheduler? resolutionScheduler = null,
        ApplyCommitLocally? applyCommitLocally = null,
        ApplyRollbackLocally? applyRollbackLocally = null,
        Func<HLCTimestamp>? attemptClock = null,
        Action<double>? recordDecisionLatencyMs = null,
        ReplicateFencedDelegate? replicateFenced = null,
        ReplicateAnchorBundleDelegate? replicateAnchorBundle = null,
        int maxMaterializationBatchItems = 512,
        long maxMaterializationBatchBytes = 4 * 1024 * 1024,
        ResolveDecidedBlockersDelegate? resolveDecidedBlockers = null,
        ReplicateOnePhaseBundleDelegate? replicateOnePhaseBundle = null,
        ValidateStagedBasesDelegate? validateStagedBases = null,
        Func<string, int>? resolveCurrentPartition = null,
        ReplicateDecisionDelegate? replicateDecision = null,
        LookupRecordRoutedDelegate? lookupRecordRouted = null,
        ConfirmReplicaFenceDelegate? confirmReplicaFence = null,
        bool materializeByReference = false,
        SemaphoreSlim? localApplyGate = null,
        DecideDelegate? decide = null)
    {
        this.decide = decide;
        // A node-shared gate bounds local applies across the finalizer and its recovery paths together; a
        // finalizer built without one (protocol tests) bounds only itself.
        this.localApplyGate = localApplyGate ?? new SemaphoreSlim(MaxConcurrentLocalApplies);
        ownsLocalApplyGate = localApplyGate is null;
        this.materializeByReference = materializeByReference;
        this.validateStagedBases = validateStagedBases;
        this.confirmReplicaFence = confirmReplicaFence;
        this.resolveCurrentPartition = resolveCurrentPartition;
        this.replicateDecision = replicateDecision;
        this.lookupRecordRouted = lookupRecordRouted;
        this.recordStore = recordStore;
        this.intentStore = intentStore;
        this.replicateOnePhaseBundle = replicateOnePhaseBundle;
        this.replicate = replicate;
        this.replicateFenced = replicateFenced;
        this.replicateAnchorBundle = replicateAnchorBundle;
        this.applyCommitLocally = applyCommitLocally;
        this.applyRollbackLocally = applyRollbackLocally;
        // A null scheduler means synchronous settlement (FinalizeAsync awaits resolution inline).
        this.scheduleResolution = resolutionScheduler;
        this.attemptClock = attemptClock;
        this.recordDecisionLatencyMs = recordDecisionLatencyMs;
        this.resolveDecidedBlockers = resolveDecidedBlockers;
        this.maxMaterializationBatchItems = Math.Max(1, maxMaterializationBatchItems);
        this.maxMaterializationBatchBytes = Math.Max(1, maxMaterializationBatchBytes);
    }

    /// <summary>
    /// Test-only interleaving hook, awaited after the pre-propose staged-base validation passes and before
    /// anything durable is proposed. Lets a test run a competing commit inside the probe→prepare window — the
    /// interleaving behind the bank-soak run-K lost update — which no external caller can time
    /// deterministically. Null (zero-cost) in production.
    /// </summary>
    internal Func<CancellationToken, Task>? TestAfterPreValidationHook;

    /// <param name="validateReadSet">Runs the optimistic read-set conflict check after every prepare is durable;
    /// true means no conflict. Only invoked when every prepare committed.</param>
    /// <param name="opId">This attempt's unique operation id, also used as the transition's attempt HLC (for the
    /// deadline check and the recorded winner). Must be less than or equal to the frozen decision deadline for a
    /// commit to be authorized.</param>
    /// <param name="readSetExclusion">The read-set shape that keeps the one-phase bundle closed, when there is
    /// one (see <c>TransactionCoordinator.ComputeOnePhaseEligibility</c>): a dependency the bundle cannot re-check
    /// at apply time. Recorded verbatim as the gate verdict and runs the standard 2PC flow; null admits the read
    /// set and leaves the partition-shape checks to decide.</param>
    /// <param name="applyTimeValidation">Whether a one-phase bundled commit must be validated at apply time, in log
    /// order, against the partition's replicated committed-head ledger — every co-bundled validated base and every
    /// entry of <paramref name="bundledReadDependencies"/>. Set only when every node in the group applies that
    /// check (<see cref="Configuration.KahunaConfiguration.OnePhaseApplyTimeValidation"/>).</param>
    /// <param name="bundledReadDependencies">The read-only point dependencies routed to the anchor partition, with
    /// the committed state the transaction observed, carried into the bundled commit for its apply-time check.</param>
    public async Task<DurableFinalizeOutcome> FinalizeAsync(
        DurableFinalizeInput input,
        Func<CancellationToken, Task<bool>> validateReadSet,
        HLCTimestamp opId,
        CancellationToken cancellationToken,
        OnePhaseGateOutcome? readSetExclusion = null,
        bool applyTimeValidation = false,
        IReadOnlyList<BundledReadDependency>? bundledReadDependencies = null)
    {
        long startTicks = Stopwatch.GetTimestamp();

        // Test-only interleaving point; see TestAfterPreValidationHook. Read once so a concurrent
        // clear cannot fault the invocation below.
        Func<CancellationToken, Task>? afterPreValidationHook = TestAfterPreValidationHook;

        // ── Staged-base compare-and-set, before anything durable is proposed ──
        // Each frozen intent carries the committed base it was validated against; if that base moved — the
        // in-memory write-intent lease lapsed mid-finalize and another transaction committed the same base
        // first — committing here would silently discard that write (a lost update). The read-set validation
        // cannot cover this: a read-then-written key is validated as a write, and this is that validation.
        // A base that cannot be resolved right now (a foreign undecided intent) decides nothing and retries.
        //
        // This pre-propose pass is the cheap early half only: it cannot see a competitor that commits inside
        // the window between this probe and the prepare landing (its intent settled and garbage-collected, or
        // settled by the prepare-retry helping pass below) — exactly the window that admitted the bank-soak
        // lost update. The authoritative half runs at the prepare's own apply position: the intent store's
        // staged-base fence compares the validated base against the last transactionally committed head of the
        // key and refuses the prepare acknowledgement on a mismatch, which drives the truthful abort below.
        // The one-phase bundle cannot rely on that refusal (its decision shares the prepare's atomic batch),
        // so TryOnePhaseFinalizeAsync re-runs this validation immediately before its propose instead.
        if (validateStagedBases is not null)
        {
            long preflightStart = Stopwatch.GetTimestamp();
            StagedBaseValidation preflight = await validateStagedBases(input, cancellationToken).ConfigureAwait(false);
            DurableTransactionMetrics.FinalizePreflightMs.Record(Stopwatch.GetElapsedTime(preflightStart).TotalMilliseconds);

            switch (preflight)
            {
                case StagedBaseValidation.Conflict:
                {
                    // The abort is driven through the record CAS (a tombstone from absence when nothing durable
                    // exists yet), never fabricated: a prior attempt of this same frozen input may have left a
                    // decision proposal in flight, and only a record-backed abort fences it out.
                    DurableFinalizeOutcome conflictOutcome = await DecideAsync(
                        input, commit: false, TransactionAbortClass.Conflict, opId, cancellationToken).ConfigureAwait(false);
                    await FinishResolutionAsync(input, conflictOutcome, cancellationToken).ConfigureAwait(false);
                    return conflictOutcome;
                }

                case StagedBaseValidation.Unknown:
                    return Retry();
            }
        }

        // Test-only: runs a competing action inside the window between the pre-propose staged-base
        // validation above and the prepares landing below — the exact interleaving the post-prepare
        // staged-base fence exists to catch, which no external caller can time deterministically.
        if (afterPreValidationHook is not null)
            await afterPreValidationHook(cancellationToken).ConfigureAwait(false);

        // ── Initialize the canonical record (Undecided) on the anchor partition ──
        byte[] initDelta = TransactionRecordStore.SerializeDelta([new InitializeTransactionCommand(
            input.TransactionId, input.Epoch, input.CoordinatorKey, input.RecordAnchorKey,
            input.CommitTimestamp, input.DecisionDeadline, input.ManifestHash, input.Manifest, opId, input.CreatedAt)]);

        // When the anchor key routes to a participant partition (the common case), the record init and that
        // partition's prepare are one atomic proposal — one fewer pre-decision barrier. Every other partition's
        // prepare fans out concurrently, so the bundle and the remaining prepares share a single barrier.
        // Each partition's prepare payload is serialized exactly once here and reused for the initial submission and
        // every retry attempt below: the frozen intent set never changes, so re-encoding it per attempt would copy the
        // whole payload again for no gain.
        byte[][] prepareDeltas = new byte[input.Partitions.Count][];
        for (int i = 0; i < input.Partitions.Count; i++)
            prepareDeltas[i] = SerializePrepare(input.Partitions[i]);

        // ── One-phase commit fast path ──
        // When the participant set collapses to the anchor partition, the whole transaction can decide in ONE
        // durable barrier: validate the read set up front, then propose [record init + prepare + commit
        // decision] as a single atomic batch — locally when this node leads the anchor, forwarded whole to the
        // anchor leader otherwise (one extra hop, still one durable round). Any ineligibility (an older remote
        // leader without the typed operation, a foreign durable intent on a written key, failed validation,
        // scheduler rejection outcome that is retryable-but-ambiguous) falls through to the standard 2PC flow
        // below, unchanged.
        //
        // Ineligible whenever the validated read set reaches beyond the written keys. The bundle's validation
        // runs before anything durable, and its only apply-time re-checks cover written keys (the bundled-prepare
        // presence gate) — a read-only dependency is re-checked by nothing. A stalled bundle (a killed leader's
        // WAL tail committing after restart) would then decide with a read validated long ago, while the
        // in-memory write intents that make this transaction visible to other validators' conflict probes are
        // already gone — closing no one's write-skew window but its own victim's. The 2PC flow keeps a durable,
        // probe-visible prepared intent on every written key from prepare until the decision, so concurrent
        // validators abort instead of committing around it.
        //
        // Without apply-time validation, validated-base (read-modify-write) transactions are also routed away
        // from the bundle in multi-process clusters — the caller reports that condition as a read-set exclusion
        // (see ComputeOnePhaseEligibility in TransactionCoordinator). On the 2PC path a moved base
        // is caught at prepare-apply time by the intent store's staged-base fence (the acknowledgement is
        // refused and the coordinator aborts truthfully), but the bundle's decision shares the prepare's atomic
        // batch, so a refused acknowledgement arrives with the decision already durable — the fence cannot
        // withhold it. The bundle's guard is then the late staged-base re-validation inside
        // TryOnePhaseFinalizeAsync, immediately before the propose. In a single-process Raft group the caller
        // keeps the bundle eligible: a competitor's whole finalize (including settlement and intent removal)
        // interleaving into the sub-millisecond validate→propose gap after an in-process write-intent lease
        // lapse is accepted as a residual in exchange for the embedded fast path; in multi-process a stalled
        // bundle proposal can apply arbitrarily late, so the residual is unbounded there and the routing above
        // closes it.
        //
        // With apply-time validation the bundled commit carries the check itself: at apply, in log order, the
        // record store judges every co-bundled validated base and every carried on-partition read dependency
        // against the partition's replicated committed-head ledger, and a base or read a competitor moved past
        // before the bundle applied rejects the commit on every replica. The caller then keeps the bundle open
        // for read-modify-write and on-partition-read transactions in multi-process groups too, and closes it
        // only for the dependencies no deterministic apply-time check exists for (predicates, off-partition
        // reads). The pre-propose validations below stay: they avoid proposing bundles that will be rejected;
        // the apply-time check is the backstop for the stall window, not their replacement.
        // Every finalize records its gate verdict, so excluded transactions are visible beside the entered ones
        // that later commit or fall back. The caller's read-set exclusion is recorded as it was classified — the
        // shape that closed the bundle (a predicate, an off-partition read, ...) is what an operator needs to act
        // on, and a single coarse tag would make a workload the bundle cannot serve look like a flag that did
        // not take effect.
        OnePhaseGateOutcome gate =
            replicateOnePhaseBundle is null ? OnePhaseGateOutcome.Disabled
            : readSetExclusion is { } excluded ? excluded
            : input.Partitions.Count != 1 ? OnePhaseGateOutcome.MultiPartition
            : input.Partitions[0].PartitionId != input.AnchorPartitionId ? OnePhaseGateOutcome.AnchorOffPartition
            : OnePhaseGateOutcome.Entered;
        DurableTransactionMetrics.OnePhaseGateDecided(gate);

        if (gate == OnePhaseGateOutcome.Entered)
        {
            (DurableFinalizeOutcome? onePhase, OnePhaseFallbackReason fallback) = await TryOnePhaseFinalizeAsync(
                input, initDelta, prepareDeltas[0], validateReadSet, opId, applyTimeValidation, bundledReadDependencies, cancellationToken).ConfigureAwait(false);

            if (onePhase is { } fastOutcome)
            {
                recordDecisionLatencyMs?.Invoke(Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds);
                await FinishResolutionAsync(input, fastOutcome, cancellationToken).ConfigureAwait(false);
                return fastOutcome;
            }

            DurableTransactionMetrics.OnePhaseFellBack(fallback);
        }

        // The first barrier alone is timed separately from the whole prepare stage, so the retry loop's share
        // (helping, backoff, re-proposals) is the difference.
        long firstPrepareStart = Stopwatch.GetTimestamp();

        int anchorIndex = -1;
        if (replicateAnchorBundle is not null)
        {
            for (int i = 0; i < input.Partitions.Count; i++)
            {
                if (input.Partitions[i].PartitionId == input.AnchorPartitionId)
                {
                    anchorIndex = i;
                    break;
                }
            }
        }

        // Per-participant acknowledgement, kept across retry rounds. An acknowledged participant's intent is
        // replicated state that stays until a decision or a presumed-abort resolves it (a range move waits for
        // undecided intents inside their window before it cuts over), so re-proposing it buys nothing but a log
        // append and a replication round; only the participants still unacknowledged are re-proposed.
        bool[] acknowledged = new bool[input.Partitions.Count];
        bool allPrepared;
        if (anchorIndex >= 0)
        {
            Task<(bool BatchCommitted, bool PrepareAcknowledged)> anchorBundleTask = replicateAnchorBundle!(
                input.AnchorPartitionId, initDelta, prepareDeltas[anchorIndex], input.RecordAnchorKey, input.AnchorGeneration, cancellationToken);

            // Every non-anchor partition prepares concurrently. Never abandon a submission on the first failure —
            // its outcome is needed to drive a truthful abort; a prepared-then-aborted intent is cleaned up by
            // resolution/recovery.
            List<Task<bool>> otherPrepareTaskList = new(input.Partitions.Count);
            for (int i = 0; i < input.Partitions.Count; i++)
            {
                if (i == anchorIndex)
                    continue;

                DurablePartitionPrepare partition = input.Partitions[i];
                otherPrepareTaskList.Add(ReplicatePrepareAsync(partition.PartitionId, prepareDeltas[i], partition.Intents[0].Key, partition.Generation, cancellationToken));
            }

            Task<bool>[] otherPrepareTasks = otherPrepareTaskList.ToArray();

            (bool BatchCommitted, bool PrepareAcknowledged) anchorResult = await anchorBundleTask.ConfigureAwait(false);
            bool[] otherResults = await Task.WhenAll(otherPrepareTasks).ConfigureAwait(false);

            // The record init and anchor prepare committed atomically. If the batch never committed, nothing is
            // durable — a clean retry, exactly as a standalone init failure was. A decision must NOT be written here:
            // an abort against an absent record creates a tombstone that would poison a same-identity retry.
            if (!anchorResult.BatchCommitted)
                return Retry();

            // The record is durably initialized. Fold the anchor prepare's own acknowledgement in with the others;
            // a rejected anchor prepare (another transaction owns the anchor key) drops allPrepared and drives the
            // truthful abort below, exactly as a rejected non-anchor prepare does.
            acknowledged[anchorIndex] = anchorResult.PrepareAcknowledged;
            for (int i = 0, other = 0; i < input.Partitions.Count; i++)
            {
                if (i != anchorIndex)
                    acknowledged[i] = otherResults[other++];
            }

            allPrepared = AllAcknowledged(acknowledged);
        }
        else
        {
            // Nothing to bundle (anchor key routes outside the participant partitions, or no bundle seam): keep the
            // original init-then-prepare sequence. Nothing is durable if the init fails, so it is a clean retry.
            if (!await ReplicateRecordAsync(input.AnchorPartitionId, initDelta, input.RecordAnchorKey, input.AnchorGeneration, Writes.WriteAdmissionClass.Ordinary, cancellationToken).ConfigureAwait(false))
                return Retry();

            // ── Prepare barrier: prepare every partition, waiting for all (never abandon a submission on the first
            // failure — its outcome is needed to drive a truthful abort). A prepared-then-aborted intent is cleaned
            // up by resolution/recovery; a failed prepare forces the transaction to abort. ──
            Task<bool>[] prepareTasks = new Task<bool>[input.Partitions.Count];
            for (int i = 0; i < input.Partitions.Count; i++)
            {
                DurablePartitionPrepare partition = input.Partitions[i];
                prepareTasks[i] = ReplicatePrepareAsync(partition.PartitionId, prepareDeltas[i], partition.Intents[0].Key, partition.Generation, cancellationToken);
            }
            bool[] prepareResults = await Task.WhenAll(prepareTasks).ConfigureAwait(false);
            for (int i = 0; i < prepareResults.Length; i++)
                acknowledged[i] = prepareResults[i];

            allPrepared = AllAcknowledged(acknowledged);
        }

        DurableTransactionMetrics.FinalizeFirstPrepareMs.Record(Stopwatch.GetElapsedTime(firstPrepareStart).TotalMilliseconds);

        // ── Prepare retry (window narrowing): a prepare rejected because the key still holds a predecessor's
        // committed-but-unsettled intent is retryable — the predecessor's background settlement removes that intent a
        // moment later. Re-prepare the participants that are still unacknowledged (the record is already durable,
        // so no re-init; an acknowledged participant's intent stays put): a blocked partition retries until the
        // foreign intent settles. Bounded, with a short backoff to yield to settlement. No decision has been written
        // yet, so a retry that succeeds still commits truthfully instead of aborting a healthy commit to MustRetry. A
        // genuine conflict (another live transaction, or an undecided intent that never resolves) simply exhausts the
        // budget and falls through to the truthful abort below; the frozen decision deadline is the final ceiling.
        //
        // Each refusal is classified before it is retried: a stale base is final (heads only advance, so every
        // re-ask answers the same) and aborts as a conflict at once; a range that moved since freeze can never
        // accept this frozen input and yields a clean retry from a fresh freeze; a held key is helped and retried;
        // a refusal with no verdict (a lost reply, an older remote node) is retried as before. ──
        //
        // The loop's cost is attributed separately from the first barrier: rounds, helper calls and time, and
        // backoff time per finalize, plus how the loop ended. All are recorded only for finalizes that entered it.
        bool retryLoopEntered = !allPrepared;
        int retryRounds = 0;
        int helperCalls = 0;
        double helperMs = 0;
        double backoffMs = 0;
        bool retryCancelled = false;
        bool staleBase = false;
        bool rangeMoved = false;
        List<int> unacknowledged = new(input.Partitions.Count);

        for (int attempt = 0; !allPrepared && attempt < MaxPrepareRetries && !cancellationToken.IsCancellationRequested; attempt++)
        {
            unacknowledged.Clear();
            for (int i = 0; i < input.Partitions.Count; i++)
            {
                if (acknowledged[i])
                    continue;

                switch (ClassifyPrepareRefusal(input, input.Partitions[i]))
                {
                    case Writes.PrepareRejectionKind.StaleBase:
                        staleBase = true;
                        break;

                    case Writes.PrepareRejectionKind.RangeMoved:
                        rangeMoved = true;
                        break;
                }

                unacknowledged.Add(i);
            }

            if (staleBase || rangeMoved)
                break;

            // Helping pass over the refused participants only, concurrently: a blocking intent whose record is
            // already decided is pure settlement lag — settle it now and re-prepare immediately, instead of
            // sleeping in the hope that the deferred-settlement task wins the race. Only when nothing could be
            // helped (a live conflict, or helping unavailable) does the backoff apply; helping failures degrade to
            // that same backoff, never to an escaped exception.
            bool helped = false;
            if (resolveDecidedBlockers is not null)
            {
                long helperStart = Stopwatch.GetTimestamp();
                try
                {
                    Task<int>[] helps = new Task<int>[unacknowledged.Count];
                    for (int k = 0; k < unacknowledged.Count; k++)
                    {
                        DurablePartitionPrepare partition = input.Partitions[unacknowledged[k]];
                        helps[k] = resolveDecidedBlockers(partition.PartitionId, partition.Intents, input.TransactionId, input.Epoch, cancellationToken);
                    }

                    helperCalls += helps.Length;
                    foreach (int settled in await Task.WhenAll(helps).ConfigureAwait(false))
                    {
                        if (settled > 0)
                            helped = true;
                    }
                }
                catch (OperationCanceledException) { retryCancelled = true; }
                catch { helped = false; }
                finally
                {
                    helperMs += Stopwatch.GetElapsedTime(helperStart).TotalMilliseconds;
                }

                if (retryCancelled)
                    break;
            }

            if (!helped)
            {
                long backoffStart = Stopwatch.GetTimestamp();
                try { await Task.Delay(Math.Min(2 * (attempt + 1), 20), cancellationToken).ConfigureAwait(false); }
                catch (OperationCanceledException) { retryCancelled = true; }
                finally
                {
                    backoffMs += Stopwatch.GetElapsedTime(backoffStart).TotalMilliseconds;
                }

                if (retryCancelled)
                    break;
            }

            retryRounds++;

            // Re-propose only the refused participants; every submitted task is awaited, never abandoned.
            Task<bool>[] retryTasks = new Task<bool>[unacknowledged.Count];
            for (int k = 0; k < unacknowledged.Count; k++)
            {
                int i = unacknowledged[k];
                DurablePartitionPrepare partition = input.Partitions[i];
                retryTasks[k] = ReplicatePrepareAsync(partition.PartitionId, prepareDeltas[i], partition.Intents[0].Key, partition.Generation, cancellationToken);
            }

            bool[] retryResults = await Task.WhenAll(retryTasks).ConfigureAwait(false);
            for (int k = 0; k < retryResults.Length; k++)
                acknowledged[unacknowledged[k]] = retryResults[k];

            allPrepared = AllAcknowledged(acknowledged);
        }

        DurableTransactionMetrics.FinalizePrepareRetries.Record(retryRounds);

        if (retryLoopEntered)
        {
            DurableTransactionMetrics.FinalizeHelperCalls.Record(helperCalls);
            DurableTransactionMetrics.FinalizeHelperMs.Record(helperMs);
            DurableTransactionMetrics.FinalizeBackoffMs.Record(backoffMs);
            DurableTransactionMetrics.PrepareRetryLoopEnded(
                allPrepared ? PrepareRetryLoopOutcome.Prepared
                : retryCancelled || cancellationToken.IsCancellationRequested ? PrepareRetryLoopOutcome.Cancelled
                : staleBase ? PrepareRetryLoopOutcome.StaleBase
                : rangeMoved ? PrepareRetryLoopOutcome.RangeMoved
                : PrepareRetryLoopOutcome.Exhausted);
        }

        DurableTransactionMetrics.FinalizePrepareMs.Record(Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds);

        // The range a refused participant was frozen against moved: this input can never prepare there. Nothing
        // decided is durable (the record is Undecided and the acknowledged intents stay recoverable), so a clean
        // retry from a fresh freeze is truthful; the abandoned-attempt fence covers a client that gives up instead.
        if (!allPrepared && rangeMoved && !staleBase)
            return Retry();

        // ── Post-prepare validation, only meaningful when everything is durable ──
        // The replica fence confirmation runs alongside the read-set validation: both need the prepares
        // durable and neither depends on the other's answer. A replica proving a validated base moved is the
        // same truthful conflict a failed validation is — ordered ahead of the decision, where the detached
        // stale-base veto carrying the same verdict used to race the commit and lose.
        long validateStart = Stopwatch.GetTimestamp();
        bool validated = false;
        if (allPrepared)
        {
            Task<bool> readSetValidation = validateReadSet(cancellationToken);
            Task<bool>? replicaFenceConfirmation = confirmReplicaFence?.Invoke(input, cancellationToken);

            validated = await readSetValidation.ConfigureAwait(false);
            if (replicaFenceConfirmation is not null)
                validated &= await replicaFenceConfirmation.ConfigureAwait(false);

            DurableTransactionMetrics.FinalizeValidateMs.Record(Stopwatch.GetElapsedTime(validateStart).TotalMilliseconds);
        }

        // ── Decision barrier: a commit only when every prepare is durable and validation passed; otherwise a
        // conflict abort (validation failed) or a retryable abort (a prepare did not commit). ──
        long decisionStart = Stopwatch.GetTimestamp();
        DurableFinalizeOutcome outcome;
        if (allPrepared && validated)
            outcome = await DecideAsync(input, commit: true, TransactionAbortClass.None, opId, cancellationToken).ConfigureAwait(false);
        else
        {
            // A stale base is a genuine conflict (the write was validated against a base that moved), whatever
            // round it surfaced in; any other unacknowledged participant is a retryable failure.
            TransactionAbortClass abortClass = !allPrepared && !staleBase ? TransactionAbortClass.RetryableFailure : TransactionAbortClass.Conflict;
            outcome = await DecideAsync(input, commit: false, abortClass, opId, cancellationToken).ConfigureAwait(false);
        }
        DurableTransactionMetrics.FinalizeDecisionMs.Record(Stopwatch.GetElapsedTime(decisionStart).TotalMilliseconds);

        // Record the latency up to the decision only — resolution below is excluded so it never inflates the
        // deadline window that a future finalize's prepare must fit inside.
        recordDecisionLatencyMs?.Invoke(Stopwatch.GetElapsedTime(startTicks).TotalMilliseconds);

        await FinishResolutionAsync(input, outcome, cancellationToken).ConfigureAwait(false);

        return outcome;
    }

    /// <summary>
    /// Resolution: apply the terminal decision to every prepared intent — on commit, materialize each intent
    /// into visible KV state, then settle (resolve + remove) the intent. With synchronous settlement (no
    /// scheduler) it is awaited here so the committed value is materialized before the caller returns — required
    /// for correct cross-node read-your-writes until the cross-node anchor decision lookup exists. A scheduler
    /// runs it in the background (deferred settlement); the decision is already durable and recovery finishes any
    /// lost run. A MustRetry outcome is skipped: nothing terminal is durable, so there is nothing to resolve.
    ///
    /// <para>Resolution is best-effort and MUST NOT change the returned outcome: the canonical decision is already
    /// durable, so an exception here (a materialization/apply/settle failure after the commit) is swallowed and
    /// left to the recovery sweep — never allowed to escape and be reported to the caller as a conflict abort.</para>
    /// </summary>
    private async Task FinishResolutionAsync(DurableFinalizeInput input, DurableFinalizeOutcome outcome, CancellationToken cancellationToken)
    {
        if (outcome.Result == DurableFinalizeResult.MustRetry)
            return;

        // A decision read from the canonical record (locally on the anchor leader, or from the anchor leader's
        // typed answer) is the resolution direction; an outcome that was not read that way re-reads the record.
        bool? knownCommit = outcome.CanonicalDecisionRead ? outcome.Result == DurableFinalizeResult.Committed : null;

        if (scheduleResolution is null)
        {
            try
            {
                await ResolveAsync(input, knownCommit, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // The decision is durable; recovery completes the resolution. Do not let it turn a commit into an abort.
            }
        }
        else
        {
            scheduleResolution(ct => ResolveAsync(input, knownCommit, ct));
        }
    }

    /// <summary>
    /// The one-phase commit fast path body. Returns the finalize outcome, or a <see langword="null"/> outcome with
    /// the reason when this transaction must fall back to the standard 2PC flow (foreign durable intent on a
    /// written key, failed up-front validation, or an older remote anchor leader without the typed one-phase
    /// operation). Callable only when the participant set is exactly the anchor partition and
    /// <see cref="replicateOnePhaseBundle"/> is wired.
    /// </summary>
    private async Task<(DurableFinalizeOutcome? Outcome, OnePhaseFallbackReason Fallback)> TryOnePhaseFinalizeAsync(
        DurableFinalizeInput input,
        byte[] initDelta,
        byte[] anchorPrepareDelta,
        Func<CancellationToken, Task<bool>> validateReadSet,
        HLCTimestamp opId,
        bool applyTimeValidation,
        IReadOnlyList<BundledReadDependency>? bundledReadDependencies,
        CancellationToken cancellationToken)
    {
        DurablePartitionPrepare partition = input.Partitions[0];

        // Pre-flight: a foreign durable intent on any written key would reject the bundled prepare — and the
        // bundled decision, sharing the atomic batch, could not be withheld once proposed. Fall back to 2PC,
        // whose prepare/retry/helping machinery owns that conflict. The check is race-free when this node
        // leads the anchor: a NEW conflicting durable prepare cannot land behind it, because its producer
        // would first need the in-memory write intents this transaction already holds on that leader
        // (installed at PrepareMutations); the only foreign intents possible are decided-but-unsettled
        // predecessors, which already existed when this transaction acquired its locks and are therefore
        // visible to this check. With a remote anchor leader the local store sees at most a replica's view
        // and the check is advisory only — the record store's bundled-prepare gate re-checks at apply, on
        // the leader, and a rejected prepare rejects the bundled commit with it.
        foreach (PreparedIntent intent in partition.Intents)
        {
            PreparedIntent? holder = intentStore.Get(intent.Key);
            if (holder is not null && (holder.TransactionId != input.TransactionId || holder.Epoch != input.Epoch))
                return (null, OnePhaseFallbackReason.ForeignIntent);
        }

        // Validation runs BEFORE anything durable — unlike 2PC's post-prepare validation. Safe for the same
        // reason as the pre-flight: conflicting writers are excluded by the in-memory write intents, so the
        // validated snapshot cannot be invalidated between here and the batch's ordered apply. A failed
        // validation falls back to the standard flow, which re-validates and drives the durable conflict
        // abort with its usual semantics.
        long validateStart = Stopwatch.GetTimestamp();
        bool validated = await validateReadSet(cancellationToken).ConfigureAwait(false);
        DurableTransactionMetrics.FinalizeValidateMs.Record(Stopwatch.GetElapsedTime(validateStart).TotalMilliseconds);
        if (!validated)
            return (null, OnePhaseFallbackReason.ValidationFailed);

        // Late staged-base re-validation, as close to the propose as the bundle allows. The bundle decides in
        // the same atomic batch as its prepare, so the prepare-apply staged-base fence cannot withhold its
        // decision (the acknowledgement arrives with the decision already durable) — without apply-time
        // validation the bundle's only guard against a competitor that committed the same base after
        // FinalizeAsync's entry validation is this re-check. Re-running it here shrinks the unguarded window to
        // the validate→apply gap of a single local proposal; a competitor's whole finalize interleaving into
        // that sub-millisecond gap after an in-process lease lapse is the accepted residual of the embedded
        // fast path, and with apply-time validation the bundled commit gate closes even that gap in log order.
        // Conflict decides a record-backed abort (the caller finishes its resolution); Unknown yields a clean
        // retry.
        if (validateStagedBases is not null)
        {
            switch (await validateStagedBases(input, cancellationToken).ConfigureAwait(false))
            {
                case StagedBaseValidation.Conflict:
                    return (await DecideAsync(input, commit: false, TransactionAbortClass.Conflict, opId, cancellationToken).ConfigureAwait(false), OnePhaseFallbackReason.None);

                case StagedBaseValidation.Unknown:
                    return (Retry(), OnePhaseFallbackReason.None);
            }
        }

        // Mint the attempt HLC now: the deadline gate in the record state machine still evaluates it when the
        // bundled decision applies, so a stalled proposal can never commit past the frozen deadline. The decision
        // also carries its bundled prepare keys: sharing the atomic batch means it cannot be withheld if the
        // prepare is rejected, so the record store's bundled-prepare gate re-checks — at apply time, in log order —
        // that this transaction's intent actually took every key. A bundle applying after the in-memory write
        // intents were lost (killed node, expired lease, a stall outliving the locks) whose keys were meanwhile
        // taken by another transaction keeps the record Undecided instead of committing a never-prepared mutation.
        //
        // With apply-time validation the decision additionally carries the on-partition read dependencies and
        // asks the gate to judge them, and every co-bundled validated base, against the partition's replicated
        // committed-head ledger at the same apply position — the deterministic backstop for the stall window
        // the late re-validation above cannot cover.
        HLCTimestamp attemptHlc = attemptClock?.Invoke() ?? opId;

        string[] bundledPrepareKeys = new string[partition.Intents.Count];
        for (int i = 0; i < partition.Intents.Count; i++)
            bundledPrepareKeys[i] = partition.Intents[i].Key;

        byte[] decisionDelta = TransactionRecordStore.SerializeDelta([
            new CommitTransactionCommand(
                input.TransactionId, input.Epoch, input.ManifestHash, opId, attemptHlc, bundledPrepareKeys,
                ApplyTimeValidation: applyTimeValidation,
                BundledReadDependencies: applyTimeValidation && bundledReadDependencies is { Count: > 0 } ? bundledReadDependencies : null)]);

        Writes.DurableOnePhaseReply? proposed = await replicateOnePhaseBundle!(
            partition.PartitionId, initDelta, anchorPrepareDelta, decisionDelta,
            input.TransactionId, input.Epoch, opId,
            input.RecordAnchorKey, input.AnchorGeneration, cancellationToken).ConfigureAwait(false);

        // The remote anchor leader does not implement the typed one-phase operation (an older node); the
        // standard 2PC flow handles it through the per-entry wire it does implement.
        if (proposed is null)
            return (null, OnePhaseFallbackReason.RemoteLeader);

        Writes.DurableOnePhaseReply reply = proposed.Value;

        // Nothing durable — a clean retry, exactly as a failed 2PC record init.
        if (!reply.BatchCommitted)
            return (Retry(), OnePhaseFallbackReason.None);

        // A rejected bundled prepare (another transaction took a key while this proposal was in flight) also
        // rejects the bundled commit through the record store's bundled-prepare gate, so the canonical outcome
        // below reports the truthful winner: Undecided, never a Commit for a mutation that was never durably
        // prepared.
        if (!reply.PrepareAcknowledged)
            DurableTransactionMetrics.OnePhasePrepareRejections.Add(1);

        // The winner is whatever the canonical record reflects after the ordered apply, exactly as in
        // DecideAsync: the deadline gate or the bundled-prepare gate may have kept the record Undecided (late
        // commit → presumed-abort recovery owns it; rejected prepare → 2PC drives the truthful outcome), and a
        // concurrent recovery abort may have won the race in the log. The anchor leader read the record after
        // it applied the batch — its own store when this node led, the typed answer otherwise — so the reply is
        // that canonical read; an unknown decision (a record reclaimed under the read) is a clean retry.
        if (!reply.DecisionKnown)
            return (Retry(), OnePhaseFallbackReason.None);

        if (reply.Decision == TransactionDecision.Undecided)
        {
            // The bundled commit gate withheld the commit, or the deadline gate did. The leader that applied
            // the batch recorded the gate's verdict for this attempt and the reply carries it. A stale base or
            // read is deterministic and final — heads only advance, and the transaction only grows older
            // against the retention horizon — so a retry of the bundle would be rejected again (its own intent
            // still holds the keys, and the pre-propose checks read that intent as valid by construction):
            // drive the truthful conflict abort through the record CAS now, exactly as a failed pre-propose
            // validation does, and roll the installed intent back everywhere. No recorded verdict leaves only
            // the deadline gate, which yields to presumed-abort recovery.
            switch (reply.GatedVerdict)
            {
                case BundledCommitVerdict.StaleBase or BundledCommitVerdict.StaleRead:
                    return (await DecideAsync(input, commit: false, TransactionAbortClass.Conflict, opId, cancellationToken).ConfigureAwait(false), OnePhaseFallbackReason.None);

                case BundledCommitVerdict.PrepareMissing:
                {
                    // Another transaction took a bundled key before the batch applied. When this node's own
                    // intent store can see that holder (it applied the batch, or replicates the anchor
                    // partition), a clean retry suffices: the retry's pre-flight sees the holder and falls back
                    // to 2PC. A holder on a remote anchor leader is invisible to that pre-flight — a retry
                    // would re-propose the same doomed bundle — so continue into the standard 2PC flow now,
                    // whose prepare/retry/helping machinery consults the leader that can see it.
                    foreach (PreparedIntent intent in partition.Intents)
                    {
                        PreparedIntent? holder = intentStore.Get(intent.Key);
                        if (holder is not null && (holder.TransactionId != input.TransactionId || holder.Epoch != input.Epoch))
                            return (Retry(), OnePhaseFallbackReason.None);
                    }

                    return (null, OnePhaseFallbackReason.ForeignIntent);
                }
            }

            if (reply.PrepareAcknowledged)
            {
                DurableTransactionMetrics.LateCommitRejections.Add(1);
                return (LateCommitRejectedRetry(), OnePhaseFallbackReason.None);
            }

            return (Retry(), OnePhaseFallbackReason.None);
        }

        if (reply.Decision == TransactionDecision.Commit)
        {
            DurableTransactionMetrics.OnePhaseCommits.Add(1);

            // Apply the committed values to the leader's live state BEFORE returning, so a back-to-back
            // operation on the same keys sees them (and not the finished transaction's leftover in-memory
            // write intents). The 2PC path gets the same effective visibility because its extra decision
            // barrier gives the deferred resolution task time to win this race; with a single barrier the
            // race would be routinely lost. Only the in-memory apply runs inline — the durable materialize
            // + settle still ride the deferred resolution (idempotent re-apply) or the recovery sweep.
            if (applyCommitLocally is not null)
            {
                foreach (PreparedIntent intent in partition.Intents)
                {
                    try
                    {
                        await applyCommitLocally(partition.PartitionId, intent).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Best-effort: the decision is durable; deferred resolution/recovery re-applies.
                    }
                }
            }
        }

        // Both terminal reads are canonical (the anchor leader's store after the ordered apply), so the
        // resolution may take its direction from the outcome without reading the record again.
        return (reply.Decision switch
        {
            TransactionDecision.Commit => new DurableFinalizeOutcome(DurableFinalizeResult.Committed, TransactionAbortClass.None, CanonicalDecisionRead: true),
            TransactionDecision.Abort => new DurableFinalizeOutcome(DurableFinalizeResult.Aborted, reply.AbortClass, CanonicalDecisionRead: true),
            _ => Retry()
        }, OnePhaseFallbackReason.None);
    }

    /// <summary>
    /// Fences an abandoned finalize attempt whose decision proposal may still be in flight: drives a durable
    /// Abort through the record CAS and classifies from the record that results. If the stalled attempt's commit
    /// already applied (or applies first), the CAS rejects the abort and the truthful outcome is
    /// <see cref="DurableFinalizeResult.Committed"/>; if the abort wins — including as a tombstone created from
    /// absence — any late commit is permanently rejected and <see cref="DurableFinalizeResult.Aborted"/> is safe
    /// to report. A <see cref="DurableFinalizeResult.MustRetry"/> means the fence itself could not be installed
    /// (replication unavailable): the transaction stays indeterminate and the caller must not report a definite
    /// outcome. The terminal decision's resolution (materialize or roll back the prepared intents) runs before
    /// returning, exactly as after a normal finalize.
    /// </summary>
    public async Task<DurableFinalizeOutcome> FenceAbandonedAsync(DurableFinalizeInput input, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        DurableFinalizeOutcome outcome = await DecideAsync(input, commit: false, TransactionAbortClass.PresumedAbort, opId, cancellationToken).ConfigureAwait(false);

        await FinishResolutionAsync(input, outcome, cancellationToken).ConfigureAwait(false);

        return outcome;
    }

    private async Task<DurableFinalizeOutcome> DecideAsync(
        DurableFinalizeInput input, bool commit, TransactionAbortClass abortClass, HLCTimestamp opId, CancellationToken cancellationToken)
    {
        // Mint a fresh attempt HLC immediately before the transition so a slow prepare/validate can push the
        // attempt past the frozen decision deadline (the gate that yields a late commit to presumed-abort
        // recovery). The operation id stays stable for idempotency; only the attempt HLC advances.
        HLCTimestamp attemptHlc = attemptClock?.Invoke() ?? opId;

        TransactionRecordCommand decision = commit
            ? new CommitTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, opId, attemptHlc)
            : new AbortTransactionCommand(input.TransactionId, input.Epoch, input.ManifestHash, abortClass, opId, attemptHlc,
                input.RecordAnchorKey, input.CommitTimestamp, input.DecisionDeadline, input.CreatedAt);

        byte[] delta = TransactionRecordStore.SerializeDelta([decision]);

        // The decision is terminal work finishing an already-prepared transaction: admit it as Terminal so an
        // ordinary-write burst saturating the anchor partition can never reject it. The projection-free
        // decision replicate is preferred: a decision can lose at a remote anchor to one that already won, and
        // projecting the losing delta into this node's store would diverge it from the canonical record.
        // The two halves of the decision stage are timed apart: the replication round trip, and the canonical
        // read-back that names the winner (one inter-node call when the anchor is remote).
        long replicateStart = Stopwatch.GetTimestamp();
        bool replicated;
        Writes.DurableDecisionReply? typed = null;
        if (decide is not null)
        {
            Writes.DurableDecisionReply reply = await decide(input.AnchorPartitionId, delta, input.TransactionId, input.Epoch, input.RecordAnchorKey, input.AnchorGeneration, cancellationToken).ConfigureAwait(false);
            typed = reply;
            replicated = reply.Replicated;
        }
        else
        {
            replicated = replicateDecision is not null
                ? await replicateDecision(input.AnchorPartitionId, delta, input.RecordAnchorKey, input.AnchorGeneration, cancellationToken).ConfigureAwait(false)
                : await ReplicateRecordAsync(input.AnchorPartitionId, delta, input.RecordAnchorKey, input.AnchorGeneration, Writes.WriteAdmissionClass.Terminal, cancellationToken).ConfigureAwait(false);
        }
        DurableTransactionMetrics.FinalizeDecisionReplicateMs.Record(Stopwatch.GetElapsedTime(replicateStart).TotalMilliseconds);

        if (!replicated)
            return Retry();

        // The winner is whatever the CANONICAL record actually reflects after apply, not what we requested — a
        // concurrent recovery abort may have won the race in the log. The typed decision carries it back from the
        // anchor leader's store; otherwise read it by the anchor route: the local store answers only when this
        // node leads the anchor partition, so a sender-side projection can never report a decision the anchor
        // rejected.
        TransactionDecision decisionRead;
        TransactionAbortClass abortClassRead;
        if (typed is { Known: true } known)
        {
            decisionRead = known.Decision;
            abortClassRead = known.AbortClass;
        }
        else
        {
            long lookupStart = Stopwatch.GetTimestamp();
            TransactionRecord? record = await ReadCanonicalRecordAsync(input, cancellationToken).ConfigureAwait(false);
            DurableTransactionMetrics.FinalizeDecisionLookupMs.Record(Stopwatch.GetElapsedTime(lookupStart).TotalMilliseconds);
            if (record is null)
                return Retry();

            decisionRead = record.Decision;
            abortClassRead = record.AbortClass;
        }

        // A commit we requested that left the record Undecided was rejected by the state machine's deadline gate
        // (the only transition that keeps an initialized record Undecided): the attempt's HLC passed the frozen
        // decision deadline, so the transaction yields to presumed-abort recovery. Surface it — a rising rate means
        // the deadline is too tight for the current finalize latency and healthy commits are being aborted.
        if (commit && decisionRead == TransactionDecision.Undecided)
        {
            DurableTransactionMetrics.LateCommitRejections.Add(1);
            return LateCommitRejectedRetry();
        }

        // A durable abort is terminal whatever drove it: the record decides once and a later commit against it is
        // rejected, so every abort class reports Aborted. Only a record that is still undecided is retryable. Both
        // reads above are canonical (the anchor leader's store after the apply), so the resolution may take its
        // direction from the outcome without reading the record again.
        return decisionRead switch
        {
            TransactionDecision.Commit => new DurableFinalizeOutcome(DurableFinalizeResult.Committed, TransactionAbortClass.None, CanonicalDecisionRead: true),
            TransactionDecision.Abort => new DurableFinalizeOutcome(DurableFinalizeResult.Aborted, abortClassRead, CanonicalDecisionRead: true),
            _ => Retry()
        };
    }

    private async Task ResolveAsync(DurableFinalizeInput input, bool? knownCommit, CancellationToken cancellationToken)
    {
        // The resolution DIRECTION must come from the canonical record: materializing legs off a node-local
        // answer that disagrees with the anchor turns an aborted transaction's prepared leg into a durable
        // write nobody counted — the conserved-total drift signature. A decision the finalize already read from
        // that record is the direction; otherwise read it here. A null or non-terminal answer leaves resolution
        // to the recovery sweep, which reads the same canonical route.
        bool commit;
        if (knownCommit is { } known)
        {
            commit = known;
        }
        else
        {
            TransactionRecord? record = await ReadCanonicalRecordAsync(input, cancellationToken).ConfigureAwait(false);
            if (record is null || !record.IsTerminal)
                return;

            commit = record.Decision == TransactionDecision.Commit;
        }

        await Task.WhenAll(input.Partitions.Select(partition => ResolvePartitionAsync(partition, commit, localApplyGate, cancellationToken))).ConfigureAwait(false);
    }

    /// <summary>The transaction's canonical record — routed by the anchor key when the routed lookup is wired
    /// (local exactly when this node leads the anchor partition), the local store otherwise (single-node and
    /// protocol-test configurations, where the local apply is the canonical apply). A lookup that cannot reach
    /// the anchor answers null — the callers treat that as indeterminate (retry / leave to recovery), which is
    /// always safe; answering from a possibly-divergent local store is not.</summary>
    private async Task<TransactionRecord?> ReadCanonicalRecordAsync(DurableFinalizeInput input, CancellationToken cancellationToken)
    {
        if (lookupRecordRouted is null)
            return recordStore.Get(input.TransactionId, input.Epoch);

        try
        {
            return await lookupRecordRouted(input.TransactionId, input.Epoch, input.RecordAnchorKey, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            return null;
        }
    }

    private async Task ResolvePartitionAsync(
        DurablePartitionPrepare partition,
        bool commit,
        SemaphoreSlim localApplyGate,
        CancellationToken cancellationToken)
    {
        // The partition id was frozen at prepare time; a range split/merge between the decision and this
        // resolution can have moved some keys to another partition. Materializing into the frozen partition
        // would write the committed value where readers no longer look (its Raft group and replicas no longer
        // serve the key), permanently hiding an acknowledged commit under placement. Re-resolve each intent's
        // current partition and resolve per current owner; the frozen id is kept when no resolver is wired or
        // nothing moved.
        if (commit && resolveCurrentPartition is not null)
        {
            // One resolver call per intent: resolving again for a second pass could observe a map that
            // moved in between and drop or double-resolve an intent.
            List<PreparedIntent>? stayed = null;
            Dictionary<int, List<PreparedIntent>>? regrouped = null;

            foreach (PreparedIntent intent in partition.Intents)
            {
                int current = ResolveCurrentPartitionSafe(intent.Key, partition.PartitionId);
                if (current == partition.PartitionId)
                {
                    (stayed ??= []).Add(intent);
                    continue;
                }

                regrouped ??= [];
                if (!regrouped.TryGetValue(current, out List<PreparedIntent>? moved))
                    regrouped[current] = moved = [];
                moved.Add(intent);
            }

            if (regrouped is not null)
            {
                List<Task> resolves = new(regrouped.Count + 1);

                if (stayed is { Count: > 0 })
                    resolves.Add(ResolvePartitionCoreAsync(partition with { Intents = stayed }, commit, localApplyGate, cancellationToken));

                foreach ((int current, List<PreparedIntent> moved) in regrouped)
                    resolves.Add(ResolvePartitionCoreAsync(partition with { PartitionId = current, Intents = moved }, commit, localApplyGate, cancellationToken));

                await Task.WhenAll(resolves).ConfigureAwait(false);
                return;
            }
        }

        await ResolvePartitionCoreAsync(partition, commit, localApplyGate, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The intent's current data partition per the wired resolver; the frozen fallback when the
    /// resolver is absent or throws (routing momentarily unavailable — the frozen target keeps the recovery
    /// semantics it always had).</summary>
    private static bool AllAcknowledged(bool[] acknowledged)
    {
        for (int i = 0; i < acknowledged.Length; i++)
        {
            if (!acknowledged[i])
                return false;
        }

        return true;
    }

    /// <summary>
    /// Why <paramref name="partition"/>'s prepare was refused, as far as this node can tell: the leader's memo
    /// for this transaction's keys (recorded at the prepare's apply, locally or mirrored from a remote leader's
    /// typed answer; a stale base outranks a held key), else a range that no longer routes the key to the frozen
    /// partition, else <see cref="Writes.PrepareRejectionKind.None"/> — no verdict, retried as before.
    /// </summary>
    private Writes.PrepareRejectionKind ClassifyPrepareRefusal(DurableFinalizeInput input, DurablePartitionPrepare partition)
    {
        Writes.PrepareRejectionKind dominant = Writes.PrepareRejectionKind.None;

        foreach (PreparedIntent intent in partition.Intents)
        {
            if (intentStore.TryTakePrepareRejection(input.TransactionId, input.Epoch, intent.Key, out Writes.PrepareRejectionKind kind) && kind > dominant)
                dominant = kind;
        }

        if (dominant != Writes.PrepareRejectionKind.None)
            return dominant;

        if (resolveCurrentPartition is not null && partition.Intents.Count > 0
            && ResolveCurrentPartitionSafe(partition.Intents[0].Key, partition.PartitionId) != partition.PartitionId)
            return Writes.PrepareRejectionKind.RangeMoved;

        return Writes.PrepareRejectionKind.None;
    }

    private int ResolveCurrentPartitionSafe(string key, int frozenPartitionId)
    {
        try
        {
            return resolveCurrentPartition?.Invoke(key) ?? frozenPartitionId;
        }
        catch
        {
            return frozenPartitionId;
        }
    }

    private async Task ResolvePartitionCoreAsync(
        DurablePartitionPrepare partition,
        bool commit,
        SemaphoreSlim localApplyGate,
        CancellationToken cancellationToken)
    {
        // Only an intent whose terminal effect is durably applied may be settled (resolved + removed). On
        // commit that means its value is materialized: settling an intent whose materialization did not commit
        // would delete the only durable copy of an already-committed value, so a false/thrown materialization
        // leaves the intent for the recovery sweep to retry. On abort the actor must still positively clear
        // staged state before settlement, otherwise the intent remains for recovery.
        List<PreparedIntent> settleable;
        if (commit)
        {
            // Fill scheduler-sized windows before awaiting them. Each window can coalesce into a capped proposal,
            // while a transaction larger than the scheduler's admission capacity advances without allocating or
            // admitting its whole working set at once.
            bool[] materialized = await MaterializePartitionAsync(partition, cancellationToken).ConfigureAwait(false);

            if (applyCommitLocally is not null)
                materialized = await ApplyLocallyAsync(
                    partition.PartitionId,
                    partition.Intents,
                    materialized,
                    (partitionId, intent) => applyCommitLocally(partitionId, intent),
                    localApplyGate,
                    cancellationToken).ConfigureAwait(false);

            settleable = partition.Intents.Where((_, index) => materialized[index]).ToList();
            DurableTransactionMetrics.Materialized(ResolutionSource.Finalize, settleable.Count);

            // Every intent left behind stays committed-but-unsettled until the recovery sweep — a window in
            // which the value is visible only through the intent overlay. Surface the rate: settlement being
            // refused (a quiesced range, backpressure, a forwarding failure) is otherwise completely silent.
            int settleFailures = partition.Intents.Count - settleable.Count;
            if (settleFailures > 0)
                DurableTransactionMetrics.ResolutionSettleFailures.Add(settleFailures);
        }
        else
        {
            bool[] rolledBack = applyRollbackLocally is null
                ? Enumerable.Repeat(true, partition.Intents.Count).ToArray()
                : await ApplyLocallyAsync(
                    partition.PartitionId,
                    partition.Intents,
                    Enumerable.Repeat(true, partition.Intents.Count).ToArray(),
                    (partitionId, intent) => applyRollbackLocally(partitionId, intent),
                    localApplyGate,
                    cancellationToken).ConfigureAwait(false);

            settleable = partition.Intents.Where((_, index) => rolledBack[index]).ToList();
        }

        if (settleable.Count > 0)
            await SettleIntentsAsync(partition.PartitionId, settleable, commit, cancellationToken).ConfigureAwait(false);
    }

    private Task<bool[]> MaterializePartitionAsync(DurablePartitionPrepare partition, CancellationToken cancellationToken)
    {
        // Abort fence, re-checked at the last moment before any value reaches the log: the resolution
        // direction was read from the canonical record, but a locally visible terminal Abort is definitive
        // (an abort never overwrites a commit, and terminal records replicate only through the canonical
        // log), and a materialization proposed past it would durably apply an aborted transaction's leg on
        // every replica. Report nothing materialized; the recovery sweep re-reads the canonical record.
        if (partition.Intents.Count > 0)
        {
            PreparedIntent fenceProbe = partition.Intents[0];
            if (recordStore.Get(fenceProbe.TransactionId, fenceProbe.Epoch) is { Decision: TransactionDecision.Abort })
            {
                DurableTransactionMetrics.AbortFencedCommitApplies.Add(partition.Intents.Count);
                return Task.FromResult(new bool[partition.Intents.Count]);
            }
        }

        return DurableMaterializationWindow.MaterializeAsync(
            partition.PartitionId, partition.Intents, materializeByReference,
            maxMaterializationBatchItems, maxMaterializationBatchBytes, replicate, cancellationToken);
    }

    /// <summary>Runs one leader-local apply per intent whose durable effect landed, under the node's shared
    /// bounded gate, and reports which applies confirmed. Shared with the recovery paths so helping and the
    /// sweep cannot multiply their fan-out past the same bound the finalizer honors.</summary>
    internal static async Task<bool[]> ApplyLocallyAsync(
        int partitionId,
        IReadOnlyList<PreparedIntent> intents,
        IReadOnlyList<bool> durableEffects,
        Func<int, PreparedIntent, Task<bool>> apply,
        SemaphoreSlim gate,
        CancellationToken cancellationToken)
    {
        bool[] applied = new bool[intents.Count];

        await Parallel.ForEachAsync(
            Enumerable.Range(0, intents.Count),
            new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentLocalApplies },
            async (index, _) =>
            {
                if (!durableEffects[index])
                    return;

                try
                {
                    await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        applied[index] = await apply(partitionId, intents[index]).ConfigureAwait(false);
                    }
                    finally
                    {
                        gate.Release();
                    }
                }
                catch
                {
                    applied[index] = false;
                }
            }).ConfigureAwait(false);

        return applied;
    }

    public void Dispose()
    {
        if (ownsLocalApplyGate)
            localApplyGate.Dispose();
    }

    // Resolves and removes each intent in one atomic delta (applied in order Pending -> resolved -> deleted), so
    // no "resolved-but-not-removed" state can linger to block a later write to the key or serve a stale value.
    // Idempotent: a replay of [Resolve, Remove] over an already-removed intent is a pair of no-ops.
    private async Task SettleIntentsAsync(int partitionId, IReadOnlyList<PreparedIntent> intents, bool commit, CancellationToken cancellationToken)
    {
        List<PreparedIntentCommand> settle = new(intents.Count * 2);
        foreach (PreparedIntent intent in intents)
        {
            settle.Add(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, commit));
            settle.Add(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key));
        }

        byte[] delta = PreparedIntentStore.SerializeDelta(settle);
        if (await ReplicateIntentsAsync(partitionId, delta, cancellationToken).ConfigureAwait(false))
            DurableTransactionMetrics.Settled(ResolutionSource.Finalize, intents.Count);
    }

    // Both replicate helpers return whatever the replicate seam reports. The seam is the single apply owner: on a
    // leader it applies the delta through the scheduler's Raft-ordered completion; forwarded to a remote leader it
    // returns that leader's applied outcome. For a prepared-intent delta the returned boolean already folds in
    // prepare acknowledgement, so a rejected prepare (another transaction owns the key) surfaces here as a failed
    // replicate and drives an abort rather than a commit of a mutation recovery could not complete.
    // Pre-decision record (initialize / decision): fenced against the anchor's frozen descriptor so a split
    // between freeze and dispatch releases it retryably instead of landing on a retired partition.
    // The record initialize is ordinary work; the decision is terminal work that finishes an already-prepared
    // transaction. The caller passes the class so the decision draws on reserve capacity and can never be
    // rejected by an ordinary-write burst on the anchor partition.
    private Task<bool> ReplicateRecordAsync(int partitionId, byte[] delta, string fenceKey, long fenceGeneration, Writes.WriteAdmissionClass admissionClass, CancellationToken cancellationToken) =>
        replicateFenced is not null
            ? replicateFenced(partitionId, ReplicationTypes.TransactionRecord, delta, fenceKey, fenceGeneration, admissionClass, cancellationToken)
            : replicate(partitionId, ReplicationTypes.TransactionRecord, delta, admissionClass, cancellationToken);

    // Pre-decision prepare: fenced against the partition group's frozen descriptor. A rejected prepare surfaces as
    // a failed replicate (the seam folds in prepare acknowledgement) and drives an abort. Ordinary work.
    private Task<bool> ReplicatePrepareAsync(int partitionId, byte[] delta, string fenceKey, long fenceGeneration, CancellationToken cancellationToken) =>
        replicateFenced is not null
            ? replicateFenced(partitionId, ReplicationTypes.PreparedIntent, delta, fenceKey, fenceGeneration, Writes.WriteAdmissionClass.Ordinary, cancellationToken)
            : replicate(partitionId, ReplicationTypes.PreparedIntent, delta, Writes.WriteAdmissionClass.Ordinary, cancellationToken);

    // Post-decision settle: unfenced and terminal. The decision is already durable; a split at this point is
    // resolved by the recovery sweep, and re-fencing would only strand the settle.
    private Task<bool> ReplicateIntentsAsync(int partitionId, byte[] delta, CancellationToken cancellationToken) =>
        replicate(partitionId, ReplicationTypes.PreparedIntent, delta, Writes.WriteAdmissionClass.Terminal, cancellationToken);

    /// <summary>Encodes one partition's frozen intent set as a single prepare delta.</summary>
    private static byte[] SerializePrepare(DurablePartitionPrepare partition)
    {
        PreparedIntentCommand[] commands = new PreparedIntentCommand[partition.Intents.Count];
        for (int i = 0; i < partition.Intents.Count; i++)
            commands[i] = new PrepareIntentCommand(partition.Intents[i]);

        return PreparedIntentStore.SerializeDelta(commands);
    }

    private static DurableFinalizeOutcome Retry() => new(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure);

    // A retry whose cause is the record's deadline gate withholding the requested commit; the coordinator counts
    // the cause once per transaction from the flag.
    private static DurableFinalizeOutcome LateCommitRejectedRetry() =>
        new(DurableFinalizeResult.MustRetry, TransactionAbortClass.RetryableFailure, LateCommitRejected: true);
}
