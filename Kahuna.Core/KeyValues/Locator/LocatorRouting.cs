
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The routing and serving-gate core shared by every locator operation group: key and prefix routing through
/// the range map or the hash router, route-hint capture, leader resolution, and the leadership confirmations
/// that decide whether this node may serve a partition from its own state.
/// </summary>
internal sealed class LocatorRouting
{
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;
    
    private readonly IRaft raft;
    
    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;


    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ClientEndpointAdvertiser advertiser;

    private readonly ILogger<IKahuna> logger;

    public LocatorRouting(
        KeyValuesManager manager,
        KahunaConfiguration configuration,
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        KeySpaceRegistry keySpaceRegistry,
        PartitionDivergenceContainment divergenceContainment,
        ILogger<IKahuna> logger
    )
    {
        this.manager = manager;
        this.configuration = configuration;
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.keySpaceRegistry = keySpaceRegistry;
        this.divergenceContainment = divergenceContainment;
        this.dataPartitionRouter = new DataPartitionRouter(raft);
        this.advertiser = ClientEndpointAdvertiserFactory.Create(raft, configuration);
        this.logger = logger;
    }

    internal KeyValuesManager Manager => manager;

    internal KahunaConfiguration Configuration => configuration;

    internal IRaft Raft => raft;

    internal IInterNodeCommunication InterNodeCommunication => interNodeCommunication;

    internal KeySpaceRegistry KeySpaceRegistry => keySpaceRegistry;

    internal DataPartitionRouter DataPartitionRouter => dataPartitionRouter;

    internal ILogger<IKahuna> Logger => logger;

    // ── the serving gate ────────────────────────────────────────────────────────────────────────
    // Every decision to serve a partition from this node's own state goes through one of the three
    // leadership helpers below or through TryWaitForLeader naming this node. A partition whose local
    // projection is proven incomplete (see PartitionDivergenceContainment) must not be served from
    // it whatever Raft says about leadership: the helpers answer "not the leader here" and the
    // resolver answers "no target", so every caller takes its existing MustRetry path and the client
    // retries against the replica that leads next.

    private readonly PartitionDivergenceContainment divergenceContainment;

    internal async ValueTask<bool> AmILeaderIfHosted(int partitionId, CancellationToken cancellationToken) =>
        !divergenceContainment.IsGated(partitionId) && await raft.AmILeaderIfHosted(partitionId, cancellationToken);

    internal async ValueTask<bool> AmILeaderQuickIfHosted(int partitionId) =>
        !divergenceContainment.IsGated(partitionId) && await raft.AmILeaderQuickIfHosted(partitionId);

    internal async ValueTask<bool> ConfirmLeadershipIfHosted(int partitionId, CancellationToken cancellationToken) =>
        !divergenceContainment.IsGated(partitionId) && await raft.ConfirmLeadershipIfHosted(partitionId, cancellationToken);

    /// <summary>
    /// Records that <paramref name="key"/> was admitted on this node, so the response can carry the
    /// route back as an advisory hint. A no-op when no capture is open or the node advertises no
    /// client endpoint.
    /// </summary>
    internal void RecordLocalKeyRoute(string key, int partitionId, long generation = 0) =>
        RouteCaptureScope.Record(KahunaRoutingDomain.KeyValue, key, partitionId, advertiser.LocalAdvertised, KahunaRouteProvenance.Executed, generation);

    /// <summary>
    /// Records that <paramref name="key"/> was forwarded to <paramref name="leader"/>. That node
    /// re-resolves ownership on arrival, so the hint is a suggestion rather than a proven executor.
    /// </summary>
    internal void RecordForwardedKeyRoute(string key, int partitionId, string leader, long generation = 0) =>
        RouteCaptureScope.Record(KahunaRoutingDomain.KeyValue, key, partitionId, advertiser.Advertise(leader), KahunaRouteProvenance.Forwarded, generation);

    /// <summary>
    /// Records the destination a batch planning loop picked for one of its keys. A planning loop has
    /// resolved a target and not yet run anything, so every key it plans — including the ones it
    /// keeps — is recorded as a suggestion rather than as a proven executor.
    /// </summary>
    internal void RecordBatchKeyRoute(string key, int partitionId, string leader, long generation = 0) =>
        RouteCaptureScope.Record(KahunaRoutingDomain.KeyValue, key, partitionId, advertiser.Advertise(leader), KahunaRouteProvenance.Forwarded, generation);

    /// <summary>
    /// The key-order router: resolves <paramref name="key"/> to <c>(partitionId,
    /// generation)</c> through the range-descriptor map for key-range spaces, or falls back to the
    /// hash router (<c>GetPartitionKey</c>) for hash spaces. Both this router and the leader-side direct-write
    /// resolver (<c>BaseHandler.ResolveDirectWritePartition</c>) go through the same
    /// <see cref="RangeRouting.Locate"/> so the two routing sites cannot drift.
    /// </summary>
    public (int PartitionId, long Generation) LocateRange(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);

    /// <summary>
    /// <see cref="LocateRange"/> without the exception: <see langword="false"/> when the key belongs to a
    /// key-range space that no descriptor covers on this node. For callers that resolve many keys and treat an
    /// unroutable one as a routine answer (see <see cref="RangeRouting.TryLocate"/>).
    /// </summary>
    public bool TryLocateRange(string key, out int partitionId) =>
        RangeRouting.TryLocate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key, out partitionId);

    /// <summary>Routes a key and reports its routing mode in the same single classification pass.</summary>
    internal (int PartitionId, long Generation, bool IsKeyRange, RangeDescriptor? Descriptor) LocateRangeWithMode(string key) =>
        RangeRouting.LocateWithMode(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key);

    /// <summary>
    /// True when <paramref name="key"/> lands in the part of <paramref name="descriptor"/> that is
    /// currently refusing writes (see <see cref="RangeRouting.IsQuiescedForWrite"/>).
    /// </summary>
    internal bool IsRangeQuiesced(RangeDescriptor? descriptor, string key) =>
        RangeRouting.IsQuiescedForWrite(descriptor, key, raft);

    /// <summary>
    /// As <see cref="IsRangeQuiesced"/> for a caller that holds only the key: resolves the key's descriptor
    /// on this node's applied map first. A hash-routed key, and a key-range key no descriptor covers here,
    /// are not quiesced.
    /// </summary>
    internal bool IsKeyQuiesced(string key) =>
        RangeRouting.IsQuiescedForWrite(RangeRouting.FindDescriptor(keySpaceRegistry, manager.RangeMapStore.Current, key), key, raft);

    /// <summary>Routes a per-key operation via <see cref="RangeRouting.Locate"/>.</summary>
    internal int RouteKey(string key) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, key).PartitionId;

    /// <summary>
    /// Routes a prefix/bucket operation to the partition of the key space the prefix names. The prefix
    /// is reduced to that key space first (<c>"t:r"</c> and <c>"t:r/"</c> both name <c>"t:r"</c>), then
    /// a <c>/</c> is appended so <see cref="KeySpaceRegistry.ExtractKeySpace"/> strips it back to the
    /// key space exactly as it does for a real key (<c>"t:r/0001"</c> → space <c>"t:r"</c>). Without
    /// the reduction the slashed spelling would route as key space <c>"t:r/"</c>: a different
    /// placement group, and so a partition whose leader does not serve the keys the scan is about.
    /// </summary>
    internal int RoutePrefixKey(string prefix) =>
        RangeRouting.Locate(keySpaceRegistry, manager.RangeMapStore.Current, dataPartitionRouter, KeyValueKeySpace.OfPrefix(prefix) + "/").PartitionId;

    /// <summary>
    /// The term a lock about to be granted on <paramref name="partitionId"/> is reported under
    /// (<see cref="LockGrantTerm"/>), or 0 when no capture is open and nobody will read it.
    ///
    /// <para>Read <b>before</b> the leadership confirmation that admits the grant, and the order is what makes
    /// the value safe to report. The confirmation proves this node led the partition at some later instant, in
    /// a term at or above the one read here. A holder that later finds the confirmed leader still in this very
    /// term therefore knows the grant was made in it too, by that leader. Read after the confirmation, a node
    /// deposed in between would report the new leader's term for a lock that lives only in its own memory, and
    /// the holder's check would pass against a leader that never saw the lock.</para>
    /// </summary>
    internal long LockGrantTermBeforeConfirmation(int partitionId) =>
        LockGrantScope.Current is null ? 0 : raft.GetPartitionTerm(partitionId);

    /// <summary>
    /// A key of the locked range that routes to <paramref name="partitionId"/>, so a commit-time probe of the
    /// partition's leadership (<see cref="KeyValueConflictChecks.LeaderTerm"/>) reaches the leader that granted
    /// the range lock: the range's start when it is bounded there, otherwise the first key of the key space,
    /// otherwise the range's end. Null when none of them routes to the partition.
    /// </summary>
    internal string? RangeLockRoutingKey(int partitionId, string prefix, string? startKey, string? endKey)
    {
        if (startKey is not null && RoutesTo(startKey, partitionId))
            return startKey;

        string firstKeyOfSpace = KeyValueKeySpace.OfPrefix(prefix) + "/";
        if (RoutesTo(firstKeyOfSpace, partitionId))
            return firstKeyOfSpace;

        return endKey is not null && RoutesTo(endKey, partitionId) ? endKey : null;
    }

    /// <summary>
    /// Answers the <see cref="KeyValueConflictChecks.LeaderTerm"/> check for the partition <paramref name="key"/>
    /// routes to: whether this node is that partition's confirmed leader, still in <paramref name="term"/>.
    /// <list type="bullet">
    /// <item><c>DoesNotExist</c> — it is: the leadership the holder's locks were granted under is unbroken.</item>
    /// <item><c>Aborted</c> — the partition is past that term. Terms only advance, so this is proof of a leader
    /// change whether or not this node leads, and needs no confirmation.</item>
    /// <item><c>MustRetry</c> — this node cannot say: it is behind the term, or a quorum did not confirm that
    /// it leads. The caller asks again, which re-resolves the leader.</item>
    /// </list>
    ///
    /// <para>The confirmation runs here, on the node that answers, and not only in the caller that routed the
    /// probe: a probe that arrives over the wire is served locally, on the sender's belief of who leads, and a
    /// follower that has not yet heard of a newer term would otherwise vouch for a leadership that has ended.
    /// The term is read again after the confirmation, so the value compared is at or above the term this node
    /// was confirmed in; equality with the holder's term then pins both to one uninterrupted leadership.</para>
    /// </summary>
    internal async ValueTask<KeyValueResponseType> CheckLeaderTerm(string key, long term, CancellationToken cancellationToken)
    {
        int partitionId = RouteKey(key);
        long current = raft.GetPartitionTerm(partitionId);

        if (current > term)
            return KeyValueResponseType.Aborted;

        if (current < term || !await ConfirmLeadershipIfHosted(partitionId, cancellationToken))
            return KeyValueResponseType.MustRetry;

        return raft.GetPartitionTerm(partitionId) == term ? KeyValueResponseType.DoesNotExist : KeyValueResponseType.Aborted;
    }

    private bool RoutesTo(string key, int partitionId)
    {
        try
        {
            return RouteKey(key) == partitionId;
        }
        catch (KahunaServerException)
        {
            // No range descriptor covers the key: it is outside the partition's range by definition.
            return false;
        }
    }

    /// <summary>
    /// Resolves the partition leader, mapping the retryable Raft resolution failures — a node that
    /// has not finished cluster initialization after a (re)join (<see cref="RaftNodeNotReadyException"/>),
    /// a leader still undecided within the election budget, or a failed partition restore — to a
    /// <see langword="null"/> result so callers answer <see cref="KeyValueResponseType.MustRetry"/>
    /// instead of leaking the exception to the client as an unhandled server error. No operation has
    /// been performed at this point, so telling the caller to retry (ideally on another node) is
    /// always safe. Mirrors <c>LockLocator</c>'s handling of the same condition.
    /// </summary>
    internal async ValueTask<string?> TryWaitForLeader(int partitionId, CancellationToken cancellationToken)
    {
        try
        {
            string? leader = await raft.TryResolveLeader(partitionId, cancellationToken);
            if (leader is null)
                logger.LogKeyValueLeaderNotResolved(partitionId, "No forward target: the partition is not hosted here, or the request already arrived forwarded");
            else if (leader == raft.GetLocalEndpoint() && divergenceContainment.IsGated(partitionId))
            {
                // The election still names this node while containment is relinquishing the partition:
                // there is no target to forward to and the local projection must not serve. Every request
                // routed here takes this branch until the handover lands, so the line is on a cooldown.
                long now = Environment.TickCount64;
                if (!gatedLeaderWarnedAt.TryGetValue(partitionId, out long last) || now - last >= BeliefOnlyLeaderWarnCooldownMs)
                {
                    gatedLeaderWarnedAt[partitionId] = now;
                    logger.LogKeyValueLeaderNotResolved(partitionId, "No forward target: this node leads the partition but its projection is gated as incomplete");
                }

                return null;
            }

            return leader;
        }
        catch (RaftException ex)
        {
            logger.LogKeyValueLeaderNotResolved(partitionId, ex.Message);

            return null;
        }
    }

    /// <summary>
    /// Memoizing variant of <see cref="TryWaitForLeader(int, CancellationToken)"/> for batch
    /// planning loops: resolves each distinct partition's leader once per call instead of once
    /// per key. The memo must stay call-local — leader identity is inherently racy, but it must
    /// never be cached across requests. Failed resolutions are not memoized so a later key on
    /// the same partition can still observe a leader elected mid-loop.
    /// </summary>
    internal async ValueTask<string?> TryWaitForLeader(
        int partitionId,
        Dictionary<int, string> leaderByPartition,
        CancellationToken cancellationToken
    )
    {
        if (leaderByPartition.TryGetValue(partitionId, out string? cached))
            return cached;

        string? leader = await TryWaitForLeader(partitionId, cancellationToken);
        if (leader is not null)
            leaderByPartition[partitionId] = leader;

        return leader;
    }

    /// <summary>
    /// Gates an authoritative local read on a quorum-confirmed leadership check (Raft read-index).
    /// Local belief (<see cref="IRaft.AmILeader"/>) is not enough for reads: a minority-partitioned
    /// leader keeps believing it leads until it receives a higher-term message, and a belief-gated
    /// read serves stale state as a successful response. Writes don't need this — replication itself
    /// fails on a deposed leader — so read paths answer <see cref="KeyValueResponseType.MustRetry"/>
    /// whenever this returns <see langword="false"/>, matching the write path's behavior.
    /// </summary>
    internal ValueTask<bool> ConfirmLeadershipForRead(int partitionId, CancellationToken cancellationToken) =>
        ConfirmLeadershipIfHosted(partitionId, cancellationToken);

    /// <summary>
    /// <see cref="ConfirmLeadershipForRead"/> for a locally-led key group: confirms every distinct
    /// partition the group's keys route to. A group is served locally only when this node believes
    /// it leads all of them, so all must confirm before any key in the group is answered from
    /// local state. The planning loop that grouped the keys already resolved each partition's
    /// leader into <paramref name="leaderByPartition"/>, so the group's partitions are recovered
    /// by filtering that map instead of re-routing every key.
    /// </summary>
    internal async ValueTask<bool> ConfirmLeadershipForGroupRead(
        string leader,
        Dictionary<int, string> leaderByPartition,
        CancellationToken cancellationToken
    )
    {
        foreach ((int partitionId, string partitionLeader) in leaderByPartition)
        {
            if (partitionLeader != leader)
                continue;

            if (!await ConfirmLeadershipForRead(partitionId, cancellationToken))
                return false;
        }

        return true;
    }

    /// <summary>
    /// Gates an operation that mutates in-memory actor state <em>without</em> a Raft proposal on the
    /// same quorum-confirmed leadership check reads use. A transactional set/delete/extend stages an
    /// MVCC entry plus its write intent, a point/prefix/range exclusive lock and its release live in
    /// the actor, and a prepare/commit/rollback mutation ticket is answered from actor state: none of
    /// them replicate, so "replication itself fails on a deposed leader" does not protect them. A
    /// leader cut off from its voters would stage a write, hand out a lock or release one from a
    /// memory no other node will ever see, and the client would take the answer as authoritative.
    /// Answering <see cref="KeyValueResponseType.MustRetry"/> instead costs one retry against a
    /// settled view.
    /// <para>A node that still believes it leads (<see cref="IRaft.AmILeaderQuick"/>, local belief)
    /// while the quorum will not confirm it is the two-leader window itself, so that case is logged
    /// at warning level, rate-limited per partition, and is the node-log evidence of the window.</para>
    /// </summary>
    internal async ValueTask<bool> ConfirmLeadershipForActorMutation(int partitionId, CancellationToken cancellationToken)
    {
        if (await ConfirmLeadershipIfHosted(partitionId, cancellationToken))
            return true;

        if (await AmILeaderQuickIfHosted(partitionId))
            LogBeliefOnlyLeaderRefusal(partitionId);

        return false;
    }

    /// <summary>
    /// <see cref="ConfirmLeadershipForActorMutation"/> for a locally-led key group: every distinct
    /// partition the group's keys route to must confirm before any key in the group mutates local
    /// actor state. Mirrors <see cref="ConfirmLeadershipForGroupRead"/>.
    /// </summary>
    internal async ValueTask<bool> ConfirmLeadershipForGroupMutation(
        string leader,
        Dictionary<int, string> leaderByPartition,
        CancellationToken cancellationToken
    )
    {
        foreach ((int partitionId, string partitionLeader) in leaderByPartition)
        {
            if (partitionLeader != leader)
                continue;

            if (!await ConfirmLeadershipForActorMutation(partitionId, cancellationToken))
                return false;
        }

        return true;
    }

    // Last warning per partition for the belief-only leader refusal, in Environment.TickCount64
    // milliseconds. A deposed leader receives every operation its stale routers still send it until
    // the cluster's view settles, and one warning per partition per cooldown is enough to place the
    // window in the node log without flooding it.
    private readonly ConcurrentDictionary<int, long> beliefOnlyLeaderWarnedAt = new();

    // Last time a gated partition's "no forward target" refusal was logged, per partition (same cooldown).
    private readonly ConcurrentDictionary<int, long> gatedLeaderWarnedAt = new();

    private const long BeliefOnlyLeaderWarnCooldownMs = 5_000;

    internal static bool AnyTransactional(List<KahunaSetKeyValueRequestItem> items)
    {
        foreach (KahunaSetKeyValueRequestItem item in items)
            if (item.TransactionId != HLCTimestamp.Zero)
                return true;

        return false;
    }

    internal static bool AnyTransactional(List<KahunaDeleteKeyValueRequestItem> items)
    {
        foreach (KahunaDeleteKeyValueRequestItem item in items)
            if (item.TransactionId != HLCTimestamp.Zero)
                return true;

        return false;
    }

    private void LogBeliefOnlyLeaderRefusal(int partitionId)
    {
        long now = Environment.TickCount64;

        if (beliefOnlyLeaderWarnedAt.TryGetValue(partitionId, out long last) && now - last < BeliefOnlyLeaderWarnCooldownMs)
            return;

        beliefOnlyLeaderWarnedAt[partitionId] = now;
        logger.LogBeliefOnlyLeaderRefusedActorMutation(partitionId, raft.GetLocalEndpoint());
    }
}
