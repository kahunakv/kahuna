using Kommander;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Why a leader-side direct write was not admitted onto its range. Reported instead of a bare
/// failure so the two independent guards stay attributable: the generation fence catches routing
/// that went stale <i>after</i> a split cut over, the quiesce covers the window <i>before</i> it.
/// Every non-<see cref="Ok"/> value is retryable — the caller answers MustRetry.
/// </summary>
internal enum DirectWriteRouting
{
    /// <summary>Admitted: the key routes here and the range is open for writes.</summary>
    Ok,

    /// <summary>No descriptor covers the key any more — the range moved out from under the write.</summary>
    NoDescriptor,

    /// <summary>The range split or moved since the write was routed (routed generation ≠ live).</summary>
    GenerationFenced,

    /// <summary>The range's data is being moved to another partition and it is refusing writes.</summary>
    Quiesced
}

/// <summary>
/// Resolves a key to its data partition without failing: <see langword="false"/> when the key cannot be routed
/// on this node now. The non-throwing counterpart of a <c>Func&lt;string, int&gt;</c> partition resolver.
/// </summary>
internal delegate bool PartitionProbe(string key, out int partitionId);

/// <summary>
/// The single source of truth for resolving a key to <c>(partitionId, generation)</c>.
/// Both partition-routing call sites must funnel through here so they can never drift:
/// <list type="number">
/// <item><see cref="KeyValueLocator"/> — routes a request to the right <b>leader node</b> (via every
/// <c>GetPartitionKey</c> / <c>GetPrefixPartitionKey</c> call).</item>
/// <item>The leader-side direct-write path (<c>BaseHandler.ResolveDirectWritePartition</c>) — re-derives
/// and fences the partition to <b>replicate into</b> once on the leader, just before proposing.</item>
/// </list>
/// If those two disagree for a ranged key, a split replicates into the stale partition. Both funnel through
/// this method so they cannot drift.
/// </summary>
internal static class RangeRouting
{
    /// <summary>
    /// Resolves <paramref name="key"/> to its owning partition and routing generation.
    /// <list type="bullet">
    /// <item><b>KeyRange</b> space → the descriptor from <paramref name="rangeMap"/> whose half-open
    /// ordinal interval contains the key; returns <c>(descriptor.PartitionId, descriptor.Generation)</c>.
    /// The generation is the Kahuna routing fence: a direct write is re-fenced against the live map by
    /// <see cref="ResolveForDirectWrite"/> on the partition leader at admission, and a durable prepare by
    /// the partition write scheduler's dispatch fence (<c>RangeMapWriteFence</c>), so a request routed at a
    /// stale generation after a split is rejected with MustRetry. It is <b>not</b> Kommander's physical-partition
    /// <c>ReplicateLogs(expectedGeneration:)</c> fence — that is an independent namespace Kahuna does not sync
    /// from descriptors, so the descriptor generation must never be passed there.</item>
    /// <item><b>Hash</b> space (default; system + <c>{db}/meta</c>) → <c>(DataPartitionRouter.Locate(key), 0)</c>.
    /// Kahuna owns hash assignment (Kommander partitions are <c>Unrouted</c>), mapping the key onto the
    /// user partitions <c>[1, InitialPartitions]</c> (P1 included — hash data may share it with the meta
    /// map). Generation <c>0</c> disables the fence — hash routing is static, nothing to fence against.</item>
    /// </list>
    /// </summary>
    /// <exception cref="RangeDescriptorMissingException">
    /// A key-range space has no descriptor covering the key on this node. A validated map has no gaps
    /// (<see cref="RangeMap.Validate"/> rejects them), so this is the transient state of a space whose seed
    /// descriptor has not replicated here yet, or whose descriptors were removed and whose routing mode is not
    /// reconciled yet. Retryable: nothing was routed.
    /// </exception>
    public static (int PartitionId, long Generation) Locate(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key)
    {
        (int partitionId, long generation, _, _) = LocateWithMode(registry, rangeMap, dataPartitionRouter, key);
        return (partitionId, generation);
    }

    /// <summary>
    /// As <see cref="Locate"/>, additionally reporting whether the key belongs to a key-range
    /// space and, for one, the descriptor it resolved through — so a router can consult the range's
    /// quiesce state without paying for a second map lookup. Classifies the key exactly once (a
    /// single scan for the key-space boundary and one span-keyed registry probe), so callers that
    /// branch on the routing mode don't pay a second classification through
    /// <see cref="IsKeyRange"/>. Nothing is allocated on either branch.
    /// </summary>
    public static (int PartitionId, long Generation, bool IsKeyRange, RangeDescriptor? Descriptor) LocateWithMode(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key)
    {
        if (TryLocateWithMode(registry, rangeMap, dataPartitionRouter, key, out (int, long, bool, RangeDescriptor?) located))
            return located;

        int separator = key.LastIndexOf('/');

        throw new RangeDescriptorMissingException(key, separator < 0 ? key : key[..separator]);
    }

    /// <summary>
    /// As <see cref="Locate"/> for a caller that expects some keys to be unroutable and must not pay an
    /// exception for each: answers <see langword="false"/> where <see cref="Locate"/> throws, when a key-range
    /// space has no descriptor covering the key on this node. That state is routine for the durable-intent
    /// stores (a restart replays data-partition entries before the meta partition has rebuilt the range map),
    /// and they resolve every key they hold on each per-partition count or stamp.
    /// </summary>
    public static bool TryLocate(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key,
        out int partitionId)
    {
        bool routable = TryLocateWithMode(registry, rangeMap, dataPartitionRouter, key, out (int PartitionId, long, bool, RangeDescriptor?) located);
        partitionId = routable ? located.PartitionId : -1;
        return routable;
    }

    // The one classification and lookup behind both the throwing and the non-throwing resolution, so the two
    // cannot route a key differently. Nothing is allocated on either branch.
    private static bool TryLocateWithMode(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key,
        out (int PartitionId, long Generation, bool IsKeyRange, RangeDescriptor? Descriptor) located)
    {
        int separator = key.LastIndexOf('/');
        ReadOnlySpan<char> keySpace = separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);

        if (registry.GetMode(keySpace) == RoutingMode.KeyRange)
        {
            RangeDescriptor? descriptor = rangeMap.Find(keySpace, key);

            if (descriptor is null)
            {
                located = default;
                return false;
            }

            located = (descriptor.PartitionId, descriptor.Generation, true, descriptor);
            return true;
        }

        // Hash-routed: Kahuna's own assignment over the data partitions (2..N); no generation fence.
        located = (dataPartitionRouter.Locate(key), 0L, false, null);
        return true;
    }

    /// <summary>
    /// Single-pass classification + resolution + fence for a leader-side direct write; the shape of
    /// <c>BaseHandler.ResolveDirectWritePartition</c> lives here so it funnels through the same
    /// module as <see cref="Locate"/> and cannot drift from it. A key-range key resolves its live
    /// descriptor, refuses it while the range is quiesced for a data move, and fences it against
    /// <paramref name="routedGeneration"/> (zero = the request carried none — route without fencing,
    /// but still report the live generation so a deferred flush can re-fence). A hash key resolves
    /// through <paramref name="dataPartitionRouter"/> and is neither quiesced nor fenced.
    ///
    /// <para>
    /// This is the last gate every direct persistent write passes on the partition leader before it
    /// is proposed, which is what makes it the authoritative place to enforce a quiesce: the write
    /// reaches it no matter which node the client happened to talk to, and it holds even on a leader
    /// promoted mid-move that never saw the in-memory range lock covering the same window.
    /// <paramref name="now"/> is only compared against a deadline that is actually set, so an open
    /// range costs nothing.
    /// </para>
    /// </summary>
    public static DirectWriteRouting ResolveForDirectWrite(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key,
        long routedGeneration,
        HLCTimestamp now,
        out int partitionId,
        out long fenceGeneration)
    {
        int separator = key.LastIndexOf('/');
        ReadOnlySpan<char> keySpace = separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);

        if (registry.GetMode(keySpace) == RoutingMode.KeyRange)
        {
            RangeDescriptor? descriptor = rangeMap.Find(keySpace, key);

            if (descriptor is null)
            {
                partitionId = 0;
                fenceGeneration = 0;
                return DirectWriteRouting.NoDescriptor;
            }

            partitionId = descriptor.PartitionId;
            fenceGeneration = descriptor.Generation;

            if (descriptor.IsQuiescedAt(key, now))
                return DirectWriteRouting.Quiesced;

            return routedGeneration == 0 || descriptor.Generation == routedGeneration
                ? DirectWriteRouting.Ok
                : DirectWriteRouting.GenerationFenced;
        }

        partitionId = dataPartitionRouter.Locate(key);
        fenceGeneration = 0; // hash space: never fenced at flush.
        return DirectWriteRouting.Ok;
    }

    /// <summary>True when <paramref name="key"/> belongs to a key-range-routed space.</summary>
    public static bool IsKeyRange(KeySpaceRegistry registry, string key) =>
        registry.GetModeForKey(key) == RoutingMode.KeyRange;

    /// <summary>
    /// True when the <paramref name="keySpace"/> is safe to serve with a single-partition prefix
    /// operation (GetByRange, GetByBucket, range-lock). A hash space is always safe. A key-range
    /// space is safe only while it has a single full-range descriptor (StartKey=null, EndKey=null),
    /// i.e. it has never been split.
    ///
    /// <para>
    /// Once a space is split the descriptor for <c>keySpace + "/"</c> (the first sub-range) no
    /// longer covers the full space, so a prefix scan routed to that one partition would silently
    /// miss all data beyond the split point. Call this before every prefix op and surface
    /// <c>Errored</c> when it returns false — until Tasks 10/11 implement multi-range stitching.
    /// </para>
    /// </summary>
    public static bool IsPrefixOpSafe(KeySpaceRegistry registry, RangeMap rangeMap, string keySpace)
    {
        // Callers hand in the operation's prefix as given, which may spell the key space with a
        // trailing slash; the registry and the map know the key space by its bare name.
        keySpace = KeyValueKeySpace.OfPrefix(keySpace);

        if (registry.GetMode(keySpace) != RoutingMode.KeyRange)
            return true;

        RangeDescriptor? d = rangeMap.Find(keySpace, keySpace + "/");
        return d is { StartKey: null, EndKey: null };
    }

    /// <summary>
    /// The descriptor a <b>key-range</b> key resolves through on <paramref name="rangeMap"/>, or null for a
    /// hash-routed key and for a key-range key no descriptor covers. For a caller that needs only the range's
    /// state — whether it is quiesced for a data move — and not a routing decision. The same single
    /// classification as <see cref="Locate"/>, so the two cannot disagree about which range a key is in.
    /// </summary>
    public static RangeDescriptor? FindDescriptor(KeySpaceRegistry registry, RangeMap rangeMap, string key)
    {
        int separator = key.LastIndexOf('/');
        ReadOnlySpan<char> keySpace = separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);

        return registry.GetMode(keySpace) == RoutingMode.KeyRange ? rangeMap.Find(keySpace, key) : null;
    }

    /// <summary>
    /// True when <paramref name="key"/> lands in the part of <paramref name="descriptor"/> that is refusing
    /// writes now because its data is being moved to another partition. The one definition behind every
    /// write-side quiesce check (the locator's pre-route bounce, the leader's direct-write admission, the
    /// coordinator's commit admission and the write scheduler's dispatch fence), so they cannot drift.
    /// Reading the clock costs a mint, so the deadline is tested for being set at all first: a range that is
    /// not being moved, which is every range almost all the time, never pays for it.
    /// </summary>
    public static bool IsQuiescedForWrite(RangeDescriptor? descriptor, string key, IRaft raft) =>
        descriptor is not null
        && descriptor.QuiescedUntil != HLCTimestamp.Zero
        && descriptor.IsQuiescedAt(key, raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()));

    /// <summary>
    /// Flush-time staleness check for a key-range write that was resolved at admission and then deferred.
    /// Returns true when the descriptor covering <paramref name="key"/> has moved since admission — no
    /// covering descriptor now, a different generation, or a different partition than the write was admitted
    /// against. This makes no generation-zero exception: <paramref name="admittedGeneration"/> is the live descriptor generation
    /// captured at admission, so zero is a concrete value to match, not "fence disabled". The partition is
    /// compared as well so a move that somehow did not bump the generation is still caught.
    /// <para>Call <see cref="IsKeyRange"/> first; hash keys never move.</para>
    /// </summary>
    public static bool HasKeyRangeMovedSinceAdmission(RangeMap rangeMap, string key, long admittedGeneration, int admittedPartitionId)
    {
        RangeDescriptor? descriptor = rangeMap.FindCovering(key);

        if (descriptor is null)
            return true;

        return descriptor.Generation != admittedGeneration || descriptor.PartitionId != admittedPartitionId;
    }
}
