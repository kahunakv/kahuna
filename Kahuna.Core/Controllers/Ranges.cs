
using Kommander;
using Kommander.Time;
using Kahuna.Server;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna;

/// <summary>
/// Key-range surface: the key-space registry, range routing, split/merge triggers, range
/// locks and the MVCC snapshot-floor holds.
/// </summary>
public sealed partial class KahunaManager
{
    public Task<bool> ReplicateKeyValueRangePageLocal(int partitionId, byte[] page, CancellationToken cancellationToken) =>
        keyValues.ReplicateKeyValueRangePageLocal(partitionId, page, cancellationToken);

    /// <summary>
    /// Removes all snapshot holds whose lease has expired. Exposed for tests so they can trigger
    /// a purge cycle without waiting for the periodic <see cref="SnapshotFloorReaperActor"/> timer.
    /// </summary>
    internal Task<int> PurgeExpiredSnapshotHoldsAsync(CancellationToken ct = default) =>
        keyValues.PurgeExpiredSnapshotHoldsAsync(ct);

    /// <summary>The replicated range-descriptor map.</summary>
    internal RangeMapStore RangeMapStore => keyValues.RangeMapStore;

    /// <summary>The replicated, refcounted, leased MVCC snapshot-floor registry.</summary>
    internal SnapshotFloorStore SnapshotFloorStore => keyValues.SnapshotFloorStore;

    /// <summary>The per-node key-space routing registry.</summary>
    internal KeySpaceRegistry KeySpaceRegistry => keyValues.KeySpaceRegistry;

    /// <summary>The quiesce store — for test inspection only.</summary>
    internal RangeQuiesceStore RangeQuiesceStore => keyValues.RangeQuiesceStore;

    /// <inheritdoc/>
    public void RegisterKeyRange(string keySpace) => keyValues.KeySpaceRegistry.RegisterKeyRange(keySpace);

    /// <inheritdoc/>
    public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RegisterKeyRangeAsync(keySpace, cancellationToken);

    /// <inheritdoc/>
    public Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default) =>
        keyValues.RemoveKeyRangeAsync(keySpace, cancellationToken);

    /// <summary>
    /// Refusals that are decided before the key space is touched at all. Both admin calls share
    /// them, and both must answer with a status rather than an exception:
    /// <see cref="KeySpaceRegistry.RegisterKeyRange"/> <i>throws</i> on an empty or <c>/meta</c>
    /// space, which unguarded reaches a caller as an unclassifiable 500.
    /// </summary>
    private (string Status, string Reason)? ValidateKeySpaceForAdmin(string keySpace)
    {
        if (string.IsNullOrEmpty(keySpace))
            return ("InvalidInput", "Key space must be non-empty.");

        if (keySpace.EndsWith("/meta", StringComparison.Ordinal))
            return ("InvalidInput",
                $"Key space '{keySpace}' is a schema-log space and is never key-range routed.");

        // Key-range data cannot live on the reserved meta/system partition, so a cluster configured
        // with no data partition can never hold a descriptor. Permanent, not a retryable failure.
        if (keyValues.Raft.Configuration.InitialPartitions < RangeMapStore.FirstDataPartitionId)
            return ("KeyRangeDisabled",
                $"Key-range routing needs at least {RangeMapStore.FirstDataPartitionId} data partition; " +
                $"this cluster is configured with {keyValues.Raft.Configuration.InitialPartitions}.");

        return null;
    }

    /// <summary>What the answering node believes about a key space right now.</summary>
    private (string RoutingMode, int DescriptorCount) ObserveKeySpace(string keySpace) =>
        (keyValues.KeySpaceRegistry.GetMode(keySpace).ToString(),
         keyValues.RangeMapStore.Current.FindAll(keySpace).Count);

    /// <inheritdoc/>
    public async Task<KahunaRegisterKeyRangeResponse> RegisterKeyRangeWithOutcomeAsync(
        string keySpace, CancellationToken cancellationToken = default)
    {
        if (ValidateKeySpaceForAdmin(keySpace) is { } refusal)
        {
            (string refusedMode, int refusedCount) = ObserveKeySpace(keySpace);
            return new()
            {
                Success = false,
                Status = refusal.Status,
                RoutingMode = refusedMode,
                DescriptorCount = refusedCount,
                Reason = refusal.Reason
            };
        }

        bool seeded = false;
        string? transportFailure = null;

        try
        {
            seeded = await keyValues.RegisterKeyRangeAsync(keySpace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            // The seed is forwarded to the meta-partition leader when this node is not it. A forward
            // that dies in transit says nothing about whether the leader committed — reporting a
            // failure here would invite a caller to conclude the space is unregistered when it may
            // already be seeded cluster-wide.
            transportFailure = $"The seed could not be confirmed with the meta-partition leader: {ex.Message}";
        }

        (string routingMode, int descriptorCount) = ObserveKeySpace(keySpace);

        if (seeded)
            return new()
            {
                Success = true,
                Status = "Seeded",
                Seeded = true,
                RoutingMode = routingMode,
                DescriptorCount = descriptorCount
            };

        // A descriptor exists, so the space is usable here — whoever seeded it, this call still did
        // the node-local half that nothing else can do for this node.
        if (descriptorCount > 0)
            return new()
            {
                Success = true,
                Status = "AlreadySeeded",
                Seeded = false,
                RoutingMode = routingMode,
                DescriptorCount = descriptorCount,
                Reason = transportFailure
            };

        return new()
        {
            Success = false,
            Status = "Indeterminate",
            Seeded = false,
            RoutingMode = routingMode,
            DescriptorCount = 0,
            Reason = transportFailure
                ?? "The seed was directed at the meta-partition leader but no descriptor is visible on "
                 + "this node yet; it may still arrive. Re-read GET /v1/ranges before concluding."
        };
    }

    /// <inheritdoc/>
    public async Task<KahunaRemoveKeyRangeResponse> RemoveKeyRangeWithOutcomeAsync(
        string keySpace, CancellationToken cancellationToken = default)
    {
        if (ValidateKeySpaceForAdmin(keySpace) is { } refusal)
        {
            (string refusedMode, int refusedCount) = ObserveKeySpace(keySpace);
            return new()
            {
                Success = false,
                Status = refusal.Status,
                RoutingMode = refusedMode,
                DescriptorCount = refusedCount,
                Reason = refusal.Reason
            };
        }

        bool removed;
        string? transportFailure = null;

        try
        {
            removed = await keyValues.RemoveKeyRangeAsync(keySpace, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            removed = true;
            transportFailure = $"The removal could not be confirmed with the meta-partition leader: {ex.Message}";
        }

        (string routingMode, int descriptorCount) = ObserveKeySpace(keySpace);

        // The only remaining cause of a plain refusal, the permanent ones having been rejected above:
        // a split is mid-cutover and holding the quiesce window open. Reported strictly — the call
        // did not look at the descriptors, so "nothing to remove" is not something it can claim.
        if (!removed)
            return new()
            {
                Success = false,
                Status = "QuiesceWindowOpen",
                RoutingMode = routingMode,
                DescriptorCount = descriptorCount,
                Reason = "A range split is mid-cutover; the window is short. Retry shortly."
            };

        if (descriptorCount == 0)
            return new()
            {
                Success = true,
                Status = "Removed",
                RoutingMode = routingMode,
                DescriptorCount = 0,
                Reason = transportFailure
            };

        return new()
        {
            Success = false,
            Status = "Indeterminate",
            RoutingMode = routingMode,
            DescriptorCount = descriptorCount,
            Reason = transportFailure
                ?? "The removal was accepted but descriptors are still visible on this node; a commit "
                 + "that has not been applied here looks the same. Re-read GET /v1/ranges."
        };
    }

    /// <inheritdoc/>
    public async Task<KahunaSplitRangeResponse> SplitRangeAtKeyWithOutcomeAsync(
        string keySpace, string splitKey, CancellationToken cancellationToken = default)
    {
        if (ValidateKeySpaceForAdmin(keySpace) is { } refusal)
            return new() { Success = false, Status = refusal.Status, Determinate = true, Reason = refusal.Reason };

        if (string.IsNullOrEmpty(splitKey))
            return new()
            {
                Success = false,
                Status = "InvalidInput",
                Determinate = true,
                Reason = "Split key must be non-empty."
            };

        IRaft raft = keyValues.Raft;

        // Gate on leadership before attempting anything. Without this the refusal surfaces from
        // inside partition creation as PartitionCreationFailed, which also means "creating the
        // destination genuinely failed" — one status for two conditions a caller must handle
        // differently (retry elsewhere vs. investigate). The range map and the partition lifecycle
        // are both owned by this partition, so one gate covers both steps of the split.
        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
            return new()
            {
                Success = false,
                Status = "NotLeader",
                Determinate = true,
                LeaderHint = raft.GetPartitionLeaderHint(RangeMapStore.MetaPartitionId),
                Reason = "This node does not lead the partition that owns the range map, so no split was "
                       + "attempted. Retry against the leader."
            };

        SplitOutcome outcome;

        try
        {
            // duringQuiesce is deliberately not plumbed through: it is a race-test hook with no
            // operator meaning, and the public entry point is where it stays unreachable.
            outcome = await keyValues.ForceSplitAtKeyAsync(keySpace, splitKey, null, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            // Leadership can be lost between the gate above and the cutover commit. Whether the map
            // changed is unknowable from here — the one thing this must not do is report it as a
            // clean refusal.
            return new()
            {
                Success = false,
                Status = "Indeterminate",
                Determinate = false,
                Reason = $"The split did not complete and its effect is unknown: {ex.Message}. "
                       + "Re-read GET /v1/ranges."
            };
        }

        return ToSplitResponse(outcome);
    }

    /// <inheritdoc/>
    public async Task<KahunaMergeRangesResponse> MergeRangesWithOutcomeAsync(CancellationToken cancellationToken = default)
    {
        IRaft raft = keyValues.Raft;

        // The same gate the trigger applies internally, lifted to where its answer can be reported.
        // Left inside, a non-leader's refusal comes back as "0 merges" — the same answer a leader
        // gives when nothing is eligible, which is precisely the ambiguity this surface exists to
        // remove.
        if (!await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, cancellationToken).ConfigureAwait(false))
            return new()
            {
                Success = false,
                Status = "NotLeader",
                Determinate = true,
                LeaderHint = raft.GetPartitionLeaderHint(RangeMapStore.MetaPartitionId),
                Reason = "This node does not lead the partition that owns the range map, so no merge pass "
                       + "ran. Retry against the leader."
            };

        try
        {
            int merges = await keyValues.TriggerAutoMergeAsync(cancellationToken).ConfigureAwait(false);

            return new()
            {
                Success = true,
                Status = "Completed",
                Determinate = true,
                Merges = merges
            };
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            // The pass merges pairs one at a time and commits each; a failure partway leaves the
            // earlier ones committed with no count to report them by.
            return new()
            {
                Success = false,
                Status = "Indeterminate",
                Determinate = false,
                Reason = $"The merge pass did not complete and how many merges landed is unknown: {ex.Message}. "
                       + "Re-read GET /v1/ranges."
            };
        }
    }

    /// <summary>
    /// Maps a split outcome to the wire shape, keeping decisions ("this definitely did not happen")
    /// apart from outcomes the caller cannot resolve from its side.
    /// </summary>
    private static KahunaSplitRangeResponse ToSplitResponse(SplitOutcome outcome) => outcome.Status switch
    {
        SplitStatus.Succeeded => new()
        {
            Success = true,
            Status = nameof(SplitStatus.Succeeded),
            Determinate = true,
            NewPartitionId = outcome.NewPartitionId,
            NewGeneration = outcome.NewGeneration
        },

        SplitStatus.NoRange => new()
        {
            Status = nameof(SplitStatus.NoRange),
            Determinate = true,
            Reason = "No range descriptor covers that key: the key space is not registered, or has no "
                   + "seed descriptor yet."
        },

        SplitStatus.InvalidSplitKey => new()
        {
            Status = nameof(SplitStatus.InvalidSplitKey),
            Determinate = true,
            Reason = "The split key must fall strictly inside the covering range; splitting at its "
                   + "start or outside its bounds would produce an empty half."
        },

        SplitStatus.BelowMinRangeSize => new()
        {
            Status = nameof(SplitStatus.BelowMinRangeSize),
            Determinate = true,
            Reason = "Refused by policy: one of the two halves holds no keys. The map is untouched."
        },

        // Nothing was attempted, so the answer is final for routing — but it says nothing about the
        // range itself, only that this attempt could not read it. Retrying is the whole remedy.
        SplitStatus.ProbeIndeterminate => new()
        {
            Status = nameof(SplitStatus.ProbeIndeterminate),
            Determinate = true,
            Reason = "Could not determine whether both halves hold keys — a scan of the range could not be "
                   + "served (an in-flight transactional write, a leadership change, a restoring partition). "
                   + "The map is untouched; retry."
        },

        // The map is untouched, so the answer is final as far as routing is concerned — but the
        // destination partition may exist and now be unreferenced, which is not a clean rollback and
        // should not be described as one.
        SplitStatus.PartitionCreationFailed => new()
        {
            Status = nameof(SplitStatus.PartitionCreationFailed),
            Determinate = true,
            Reason = "The destination partition could not be created; no descriptor changed. A partition "
                   + "may have been created and left unused."
        },

        // Everything below failed after the split transaction was under way. The map may still change.
        SplitStatus.TransferFailed => new()
        {
            Status = nameof(SplitStatus.TransferFailed),
            Determinate = false,
            Reason = "Copying the upper half to the destination partition failed. Re-read GET /v1/ranges."
        },

        SplitStatus.QuiesceFailed => new()
        {
            Status = nameof(SplitStatus.QuiesceFailed),
            Determinate = false,
            Reason = "The source range could not be quiesced for cutover. Re-read GET /v1/ranges."
        },

        SplitStatus.CutoverFailed => new()
        {
            Status = nameof(SplitStatus.CutoverFailed),
            Determinate = false,
            Reason = "The cutover commit did not confirm; it may still land. Re-read GET /v1/ranges."
        },

        SplitStatus.ConcurrentSplit => new()
        {
            Status = nameof(SplitStatus.ConcurrentSplit),
            Determinate = false,
            Reason = "Another split of the same range was in flight. Re-read GET /v1/ranges to see which "
                   + "one landed."
        },

        // An unrecognised status is treated as unresolved rather than as a failure: a new outcome
        // added upstream must not silently read as "the split definitely did not happen".
        _ => new()
        {
            Status = outcome.Status.ToString(),
            Determinate = false,
            Reason = "Unrecognised split outcome; re-read GET /v1/ranges."
        }
    };

    /// <inheritdoc/>
    public KahunaRangeMapResponse GetRangeMap(string? keySpace = null)
    {
        IRaft raft = keyValues.Raft;

        KahunaRangeMapResponse response = new()
        {
            Initialized = raft.IsInitialized,
            LocalEndpoint = raft.GetLocalEndpoint()
        };

        // One snapshot of each side, taken once: the map is replaced wholesale on mutation and the
        // registry is a concurrent dictionary, so re-reading either per key space could interleave a
        // split and report a space twice or not at all.
        RangeMap map = keyValues.RangeMapStore.Current;
        KeySpaceRegistry registry = keyValues.KeySpaceRegistry;

        // The union, not just the map: a space carrying descriptors may be unregistered on this node
        // (routing mode is not replicated), and a space registered here may have no descriptor yet.
        // Both are states an operator needs to see, and either half alone hides one of them.
        SortedSet<string> spaces = new(StringComparer.Ordinal);

        if (keySpace is null)
        {
            foreach (string space in map.KeySpaces)
                spaces.Add(space);

            foreach (string space in registry.RegisteredKeySpaces)
                spaces.Add(space);
        }
        else
        {
            // A filtered read reports the space even when nothing knows about it, so a caller polling
            // for a registration to land reads "Hash, no descriptors" instead of an empty document it
            // has to interpret.
            spaces.Add(keySpace);
        }

        foreach (string space in spaces)
        {
            KahunaKeySpaceRangesResponse entry = new()
            {
                KeySpace = space,
                RoutingMode = registry.GetMode(space).ToString()
            };

            // FindAll returns this space's descriptors already sorted by StartKey (ordinal, null
            // first) — the same order the router binary-searches, so what a reader sees here is the
            // order routing actually uses.
            foreach (RangeDescriptor descriptor in map.FindAll(space))
                entry.Descriptors.Add(new KahunaRangeDescriptorResponse
                {
                    StartKey = descriptor.StartKey,
                    EndKey = descriptor.EndKey,
                    PartitionId = descriptor.PartitionId,
                    Generation = descriptor.Generation
                });

            response.KeySpaces.Add(entry);
        }

        return response;
    }

    /// <summary>The key-range data-movement primitive; register with <c>IRaft.RegisterStateMachineTransfer</c>.</summary>
    internal KvStateMachineTransfer KvStateMachineTransfer => keyValues.KvStateMachineTransfer;

    /// <summary>Returns live range locks held on <paramref name="keySpace"/> in the local actor (export helper).</summary>
    internal Task<List<KeyValueRangeLock>> GetRangeLocksAsync(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    /// <summary>Injects clamped lock entries into the local actor for <paramref name="keySpace"/> (import helper).</summary>
    internal Task ImportRangeLocksAsync(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    // IKahuna surface for inter-node routing.
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace) =>
        keyValues.GetRangeLocksAsync(keySpace);

    public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks) =>
        keyValues.ImportRangeLocksAsync(keySpace, locks);

    /// <summary>Resolves a key to its owning <c>(partitionId, generation)</c> (key-order router).</summary>
    internal (int PartitionId, long Generation) LocateRange(string key) => keyValues.LocateRange(key);

    /// <summary>The split-transaction executor.</summary>
    internal RangeSplitter RangeSplitter => keyValues.RangeSplitter;

    /// <summary>The auto-split trigger (exposed for regression tests of <c>ExecuteSplitAsync</c>).</summary>
    internal RangeSplitTrigger RangeSplitTrigger => keyValues.RangeSplitTrigger;

    /// <summary>The merge-transaction executor.</summary>
    internal RangeMerger RangeMerger => keyValues.RangeMerger;

    /// <summary>
    /// Returns the data partition id that <paramref name="key"/> routes to under Kahuna's own
    /// consistent-hash assignment. Matches the routing used by <c>LocateAndTrySetKeyValue</c> and
    /// all other locating operations, so callers can find the right leader without guessing.
    /// </summary>
    public int GetDataPartitionForKey(string key) => keyValues.LocateRange(key).PartitionId;

    /// <summary>
    /// Checks every KeyRange descriptor and splits any that exceed the configured size threshold.
    /// Returns the number of splits performed. Only executes on the node that holds leadership
    /// of both the system partition (0) and meta partition (1).
    /// </summary>
    public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-split trigger with an explicit <paramref name="threshold"/>
    /// and <paramref name="minRangeSize"/> instead of the production config values.
    /// </summary>
    internal Task<int> TriggerAutoSplitAsync(int threshold, int minRangeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoSplitAsync(threshold, minRangeSize, ct);

    /// <summary>
    /// Scans all KeyRange spaces for adjacent under-min descriptor pairs and merges them.
    /// Returns the number of merges performed. Only executes on the dual-leader node.
    /// </summary>
    public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(ct);

    /// <summary>
    /// Test-seam overload: runs the auto-merge trigger with an explicit <paramref name="minMergeSize"/>
    /// instead of the production config value.
    /// </summary>
    internal Task<int> TriggerAutoMergeAsync(int minMergeSize, CancellationToken ct = default) =>
        keyValues.TriggerAutoMergeAsync(minMergeSize, ct);

    // ── MVCC snapshot floor ─────────────────────────────────────────────────────────────────

    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct) =>
        keyValues.AcquireSnapshotHold(holderId, timestamp, leaseMs, ct);

    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct) =>
        keyValues.RenewSnapshotHold(holdId, leaseMs, ct);

    public Task<KeyValueResponseType>
        LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct) =>
        keyValues.ReleaseSnapshotHold(holdId, ct);

    public Task<(KeyValueResponseType Type, HLCTimestamp EffectiveFloor, int LiveHolds)>
        GetSnapshotFloor(CancellationToken ct) =>
        keyValues.GetSnapshotFloor(ct);
}
