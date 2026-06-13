namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The single source of truth for resolving a key to <c>(partitionId, generation)</c>.
/// Both partition-routing call sites must funnel through here so they can never drift:
/// <list type="number">
/// <item><see cref="KeyValueLocator"/> — routes a request to the right <b>leader node</b> (via every
/// <c>GetPartitionKey</c> / <c>GetPrefixPartitionKey</c> call).</item>
/// <item><c>KeyValueProposalActor</c> (<c>:45</c>) — re-derives the partition to <b>replicate
/// into</b> once on the leader.</item>
/// </list>
/// If those two disagree for a ranged key, a split replicates into the stale partition. They are
/// inventoried here; the caller switch is wired separately (both then call this method).
/// </summary>
internal static class RangeRouting
{
    /// <summary>
    /// Resolves <paramref name="key"/> to its owning partition and routing generation.
    /// <list type="bullet">
    /// <item><b>KeyRange</b> space → the descriptor from <paramref name="rangeMap"/> whose half-open
    /// ordinal interval contains the key; returns <c>(descriptor.PartitionId, descriptor.Generation)</c>.
    /// The generation is the routing fence threaded into <c>ReplicateLogs(expectedGeneration:)</c>
    /// by the generation fence.</item>
    /// <item><b>Hash</b> space (default; system + <c>{db}/meta</c>) → <c>(DataPartitionRouter.Locate(key), 0)</c>.
    /// Kahuna owns hash assignment (Kommander partitions are <c>Unrouted</c>), mapping the key onto the
    /// user partitions <c>[1, InitialPartitions]</c> (P1 included — hash data may share it with the meta
    /// map). Generation <c>0</c> disables the fence — hash routing is static, nothing to fence against.</item>
    /// </list>
    /// </summary>
    /// <exception cref="KahunaServerException">
    /// A key-range space has no descriptor covering the key (a gap — should be impossible once the
    /// map is initialized, since <see cref="RangeMap.Validate"/> rejects gaps).
    /// </exception>
    public static (int PartitionId, long Generation) Locate(
        KeySpaceRegistry registry,
        RangeMap rangeMap,
        DataPartitionRouter dataPartitionRouter,
        string key)
    {
        string keySpace = KeySpaceRegistry.ExtractKeySpace(key);

        if (registry.GetMode(keySpace) == RoutingMode.KeyRange)
        {
            RangeDescriptor? descriptor = rangeMap.Find(keySpace, key);

            if (descriptor is null)
                throw new KahunaServerException(
                    $"No range descriptor covers key '{key}' in key-range space '{keySpace}'.");

            return (descriptor.PartitionId, descriptor.Generation);
        }

        // Hash-routed: Kahuna's own assignment over the data partitions (2..N); no generation fence.
        return (dataPartitionRouter.Locate(key), 0L);
    }

    /// <summary>True when <paramref name="key"/> belongs to a key-range-routed space.</summary>
    public static bool IsKeyRange(KeySpaceRegistry registry, string key) =>
        registry.GetMode(KeySpaceRegistry.ExtractKeySpace(key)) == RoutingMode.KeyRange;

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
        if (registry.GetMode(keySpace) != RoutingMode.KeyRange)
            return true;

        RangeDescriptor? d = rangeMap.Find(keySpace, keySpace + "/");
        return d is { StartKey: null, EndKey: null };
    }

    /// <summary>
    /// The generation fence for a <b>key-range</b> key, evaluated on the leader
    /// just before replicating a write. Resolves the current descriptor and compares it to the
    /// generation the request routed on. Returns <c>false</c> (⇒ caller surfaces <c>MustRetry</c>)
    /// when the range moved or split since routing — no covering descriptor, or a generation bump.
    /// On success, <paramref name="partitionId"/> is the descriptor's current partition.
    /// <para>Call <see cref="IsKeyRange"/> first; hash keys are not fenced.</para>
    /// </summary>
    public static bool TryFenceKeyRange(RangeMap rangeMap, string key, long routedGeneration, out int partitionId)
    {
        RangeDescriptor? descriptor = rangeMap.Find(KeySpaceRegistry.ExtractKeySpace(key), key);

        if (descriptor is null)
        {
            partitionId = 0;
            return false;
        }

        partitionId = descriptor.PartitionId;

        // routedGeneration 0 = the request didn't carry a routed generation (pre-switch path); route
        // to the current descriptor without fencing. A non-zero routed generation must match.
        return routedGeneration == 0 || descriptor.Generation == routedGeneration;
    }
}
