using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Sequencer.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander;
using Kommander.Time;
using Nixie;
using Polly.Contrib.WaitAndRetry;

namespace Kahuna.Server.Sequencer;

/// <summary>
/// Owns the in-memory allocation state of every sequence routed to it.
///
/// <para>The durable record is a high-water mark. Instead of compare-and-swapping it once per value —
/// a Raft round trip, an fsync, and a full record re-serialization for a single increment — the actor
/// reserves a whole block by bumping that mark once, then answers subsequent requests straight from
/// memory. One commit is amortized over <c>SequencerBlockSize</c> values.</para>
///
/// <para>A sequence is reached only through the leader of its storage key's partition, so one actor in
/// the cluster owns it at a time and the bump is a local write. Exclusivity does not rest on that
/// routing though: the bump only lands if the record still carries the revision the actor read, so even
/// a former leader that has not yet learned it lost the partition cannot reserve a window overlapping
/// the new leader's. Whatever is left in a block that gets abandoned — a restart, an eviction, an
/// ownership change — is a gap, never a duplicate, the same trade sequence caches make in conventional
/// databases.</para>
///
/// <para>Requests are processed one at a time, which is what makes the block safe to hold without any
/// lock, and replaces the per-name semaphore the previous read-modify-write loop needed.</para>
/// </summary>
internal sealed class SequenceActor : IActor<SequenceRequest, SequenceResponse>
{
    private const int MaxRetries = 16;

    internal const string ReservedPrefix = "__kahuna:sequences:";

    private const string IdempotencyKeyPrefix = "reserve:";

    /// <summary>
    /// Compare-and-swap retry schedule, materialised once. The previous implementation built a fresh
    /// backoff enumerable on every allocation, including the overwhelming majority that never retry.
    /// </summary>
    private static readonly TimeSpan[] RetryDelays =
        Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromMilliseconds(1), retryCount: MaxRetries).ToArray();

    private readonly IActorContext<SequenceActor, SequenceRequest, SequenceResponse> actorContext;

    private readonly KeyValuesManager keyValues;

    private readonly IRaft raft;

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;

    private readonly Dictionary<string, SequenceBlock> blocks = new();

    /// <summary>Monotonic use counter backing least-recently-used eviction of resident blocks.</summary>
    private long useStamp;

    public SequenceActor(
        IActorContext<SequenceActor, SequenceRequest, SequenceResponse> actorContext,
        KeyValuesManager keyValues,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.keyValues = keyValues;
        this.raft = raft;
        this.configuration = configuration;
        this.logger = logger;
    }

    public async Task<SequenceResponse?> Receive(SequenceRequest message)
    {
        try
        {
            return message.Type switch
            {
                SequenceRequestType.Reserve => await Reserve(message).ConfigureAwait(false),
                SequenceRequestType.Create => await Create(message).ConfigureAwait(false),
                SequenceRequestType.Delete => await Delete(message).ConfigureAwait(false),
                SequenceRequestType.Invalidate => Invalidate(),
                _ => SequenceStaticResponses.Error
            };
        }
        catch (OperationCanceledException)
        {
            // The caller went away mid-round-trip. Anything already taken from the block is a gap.
            return SequenceStaticResponses.MustRetry;
        }
        catch (Exception ex)
        {
            logger.LogError("SequenceActor {Actor}: error processing {Type} for '{Name}': {Error} {Message}\n{Stacktrace}",
                actorContext.Self.Runner.Name, message.Type, message.Name, ex.GetType().Name, ex.Message, ex.StackTrace);

            blocks.Remove(message.Name);
            return SequenceStaticResponses.Error;
        }
    }

    // ── allocation ──────────────────────────────────────────────────────────────────────────────

    private async Task<SequenceResponse> Reserve(SequenceRequest message)
    {
        string? idempotencyKey = message.IdempotencyKey is null ? null : IdempotencyKeyPrefix + message.IdempotencyKey;

        CancellationToken cancellationToken = message.CancellationToken;

        // True once this call has read the record from the store, so a keyed request does not pay a
        // second read after loading the block, and a compare-and-swap failure forces exactly one refresh.
        bool snapshotIsFresh = false;

        for (int attempt = 0; attempt < RetryDelays.Length; attempt++)
        {
            if (!blocks.TryGetValue(message.Name, out SequenceBlock? block))
            {
                (SequenceResponseType loadError, SequenceState? state, long revision) =
                    await Read(message.Name, cancellationToken).ConfigureAwait(false);

                if (state is null)
                    return Static(loadError);

                block = new(GetStorageKey(message.Name), state, revision);
                Admit(message.Name, block);
                snapshotIsFresh = true;
            }

            block.LastUsed = ++useStamp;

            if (block.State.Increment <= 0)
            {
                logger.LogError("SequenceActor: sequence '{Name}' has a non-positive increment {Increment}; record is unusable",
                    message.Name, block.State.Increment);

                blocks.Remove(message.Name);
                return SequenceStaticResponses.Error;
            }

            if (idempotencyKey is not null)
            {
                // A replay this actor already served or read is authoritative: the entry is durable.
                if (block.State.Idempotency.TryGetValue(idempotencyKey, out SequenceIdempotencyEntry recorded))
                    return new(SequenceResponseType.Success, recorded.Allocation);

                // Otherwise the allocation may have been recorded elsewhere, so consult the record
                // before issuing a new one. This is the cost idempotent requests pay for replayability.
                if (!snapshotIsFresh)
                {
                    (SequenceResponseType refreshError, SequenceState? refreshed, long refreshedRevision) =
                        await Read(message.Name, cancellationToken).ConfigureAwait(false);

                    if (refreshed is null)
                    {
                        blocks.Remove(message.Name);
                        return Static(refreshError);
                    }

                    Adopt(block, refreshed, refreshedRevision);
                    snapshotIsFresh = true;

                    if (block.State.Idempotency.TryGetValue(idempotencyKey, out recorded))
                        return new(SequenceResponseType.Success, recorded.Allocation);
                }
            }

            // Plan the allocation without committing it, so a failed write can be retried against a
            // refreshed record without values being consumed twice.
            bool fromBlock = TryPlanFromBlock(block, message.Count, out long start, out long end);

            SequenceBump bump = default;

            if (!fromBlock)
            {
                if (!TryPlanBump(block.State, message.Count, configuration.SequencerBlockSize, out bump))
                    return SequenceStaticResponses.MaxValueExceeded;

                start = bump.Start;
                end = bump.End;
            }

            // A plain request served entirely from the reserved block touches no storage at all.
            if (fromBlock && idempotencyKey is null)
            {
                block.Current = end;
                return new(SequenceResponseType.Success, new SequenceAllocation(block.State.Name, start, end, message.Count, block.Revision));
            }

            HLCTimestamp now = Now();

            if (!fromBlock)
                block.State.CurrentValue = bump.Ceiling;

            SequenceAllocation allocation = new(block.State.Name, start, end, message.Count, block.Revision);

            if (idempotencyKey is not null)
                block.State.Idempotency[idempotencyKey] = new(allocation, now);

            (KeyValueResponseType writeType, long writtenRevision) =
                await Persist(block, now, idempotencyKey, cancellationToken).ConfigureAwait(false);

            if (writeType == KeyValueResponseType.Set)
            {
                block.Revision = writtenRevision;

                if (!fromBlock)
                {
                    block.Current = bump.End;
                    block.Ceiling = bump.Ceiling;
                }
                else
                {
                    block.Current = end;
                }

                return new(SequenceResponseType.Success, allocation);
            }

            if (writeType is not (KeyValueResponseType.NotSet or KeyValueResponseType.MustRetry))
            {
                // The record's state is now unknown to this actor; drop the block rather than keep
                // handing out values against a revision that may no longer exist.
                blocks.Remove(message.Name);
                return Static(Map(writeType));
            }

            // Lost the compare-and-swap (or the partition asked us to retry): re-read and replan. The
            // in-memory block is untouched — it was reserved by an earlier, successful bump and is
            // still exclusively ours.
            snapshotIsFresh = false;

            await Task.Delay(RetryDelays[attempt], cancellationToken).ConfigureAwait(false);

            if (blocks.TryGetValue(message.Name, out SequenceBlock? retained))
            {
                (SequenceResponseType refreshError, SequenceState? refreshed, long refreshedRevision) =
                    await Read(message.Name, cancellationToken).ConfigureAwait(false);

                if (refreshed is null)
                {
                    blocks.Remove(message.Name);
                    return Static(refreshError);
                }

                Adopt(retained, refreshed, refreshedRevision);
                snapshotIsFresh = true;
            }
        }

        logger.LogWarning("Sequence allocation exhausted retries for {Name}", message.Name);

        blocks.Remove(message.Name);
        return SequenceStaticResponses.MustRetry;
    }

    /// <summary>
    /// Plans <paramref name="count"/> values out of the block already reserved, without consuming them.
    /// </summary>
    private static bool TryPlanFromBlock(SequenceBlock block, int count, out long start, out long end)
    {
        Int128 increment = block.State.Increment;
        Int128 last = (Int128)block.Current + increment * count;

        if (last > block.Ceiling)
        {
            start = 0;
            end = 0;
            return false;
        }

        start = (long)((Int128)block.Current + increment);
        end = (long)last;
        return true;
    }

    /// <summary>
    /// Plans the next block: how far the durable high-water mark must move, and which values the
    /// requesting caller takes from the front of it. Arithmetic is widened so a sequence approaching
    /// <see cref="long.MaxValue"/> clamps instead of overflowing.
    /// </summary>
    private static bool TryPlanBump(SequenceState state, int count, int blockSize, out SequenceBump bump)
    {
        Int128 origin = state.CurrentValue;
        Int128 increment = state.Increment;
        Int128 limit = state.MaxValue ?? long.MaxValue;

        Int128 last = origin + increment * count;

        if (last > limit)
        {
            bump = default;
            return false;
        }

        // Reserve a whole block, or exactly the requested run when it is larger — an allocation is
        // never split across two bumps.
        Int128 window = Math.Max(blockSize, count);
        Int128 ceiling = origin + increment * window;

        if (ceiling > limit)
            ceiling = origin + increment * ((limit - origin) / increment);

        bump = new((long)(origin + increment), (long)last, (long)ceiling);
        return true;
    }

    // ── lifecycle ───────────────────────────────────────────────────────────────────────────────

    private async Task<SequenceResponse> Create(SequenceRequest message)
    {
        // Any block left over from a previous incarnation of this name must not survive the new one.
        blocks.Remove(message.Name);

        HLCTimestamp now = Now();

        SequenceState state = new()
        {
            Name = message.Name,
            CurrentValue = message.InitialValue,
            InitialValue = message.InitialValue,
            Increment = message.Increment,
            MaxValue = message.MaxValue,
            CreatedAt = now,
            UpdatedAt = now
        };

        (KeyValueResponseType response, long revision, _) = await keyValues.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            GetStorageKey(message.Name),
            SequenceStateCodec.Serialize(state),
            null,
            -1,
            KeyValueFlags.SetIfNotExists,
            0,
            KeyValueDurability.Persistent,
            message.CancellationToken
        ).ConfigureAwait(false);

        blocks.Remove(message.Name);

        return response switch
        {
            KeyValueResponseType.Set => new(SequenceResponseType.Success, default, revision),
            KeyValueResponseType.NotSet => SequenceStaticResponses.AlreadyExists,
            _ => Static(Map(response))
        };
    }

    private async Task<SequenceResponse> Delete(SequenceRequest message)
    {
        blocks.Remove(message.Name);

        (KeyValueResponseType response, _, _) = await keyValues.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero,
            GetStorageKey(message.Name),
            KeyValueDurability.Persistent,
            message.CancellationToken
        ).ConfigureAwait(false);

        // Dropped again after the round trip so the deleted incarnation's block can never be served,
        // whatever the outcome of the delete.
        blocks.Remove(message.Name);

        return response switch
        {
            KeyValueResponseType.Deleted => SequenceStaticResponses.Success,
            KeyValueResponseType.DoesNotExist => SequenceStaticResponses.NotFound,
            _ => Static(Map(response))
        };
    }

    private SequenceResponse Invalidate()
    {
        blocks.Clear();
        return SequenceStaticResponses.Success;
    }

    // ── storage ─────────────────────────────────────────────────────────────────────────────────

    private async Task<(SequenceResponseType, SequenceState?, long)> Read(string name, CancellationToken cancellationToken)
    {
        (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await keyValues.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            GetStorageKey(name),
            -1,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        if (response == KeyValueResponseType.DoesNotExist)
            return (SequenceResponseType.NotFound, null, -1);

        if (response != KeyValueResponseType.Get || entry?.Value is null)
            return (Map(response), null, -1);

        SequenceState? state = SequenceStateCodec.Deserialize(entry.Value);

        return state is null ? (SequenceResponseType.Error, null, -1) : (SequenceResponseType.Success, state, entry.Revision);
    }

    private async Task<(KeyValueResponseType, long)> Persist(SequenceBlock block, HLCTimestamp now, string? protectedKey, CancellationToken cancellationToken)
    {
        block.State.UpdatedAt = now;

        SequenceStateCodec.Prune(
            block.State,
            now,
            configuration.SequencerIdempotencyRetentionMax,
            configuration.SequencerIdempotencyRetentionTtl,
            protectedKey
        );

        (KeyValueResponseType response, long revision, _) = await keyValues.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            block.StorageKey,
            SequenceStateCodec.Serialize(block.State),
            null,
            block.Revision,
            KeyValueFlags.SetIfEqualToRevision,
            0,
            KeyValueDurability.Persistent,
            cancellationToken
        ).ConfigureAwait(false);

        return (response, revision);
    }

    /// <summary>
    /// Replaces the block's view of the durable record. The reserved window survives — it was won by an
    /// earlier compare-and-swap and no other node can reissue it — unless the record turns out to be a
    /// different incarnation of the name, in which case the old window is void.
    /// </summary>
    private static void Adopt(SequenceBlock block, SequenceState state, long revision)
    {
        bool sameIncarnation = state.CreatedAt == block.State.CreatedAt
            && state.InitialValue == block.State.InitialValue
            && state.Increment == block.State.Increment;

        block.State = state;
        block.Revision = revision;

        if (sameIncarnation)
            return;

        block.Current = state.CurrentValue;
        block.Ceiling = state.CurrentValue;
    }

    // ── residency ───────────────────────────────────────────────────────────────────────────────

    private void Admit(string name, SequenceBlock block)
    {
        int max = configuration.SequencerMaxSequencesPerActor;

        if (max > 0 && blocks.Count >= max)
            EvictLeastRecentlyUsed(max);

        blocks[name] = block;
    }

    /// <summary>
    /// Trims resident blocks back below the cap, oldest use first. Evicting a block abandons its
    /// unissued tail, which is a gap — the same outcome as a restart.
    /// </summary>
    private void EvictLeastRecentlyUsed(int max)
    {
        int target = Math.Max(1, max / 10);

        List<KeyValuePair<string, SequenceBlock>> candidates = new(blocks);
        candidates.Sort(static (a, b) => a.Value.LastUsed.CompareTo(b.Value.LastUsed));

        for (int i = 0; i < target && i < candidates.Count; i++)
            blocks.Remove(candidates[i].Key);
    }

    // ── helpers ─────────────────────────────────────────────────────────────────────────────────

    private HLCTimestamp Now()
    {
        return raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
    }

    internal static string GetStorageKey(string name)
    {
        return ReservedPrefix + name;
    }

    private static SequenceResponse Static(SequenceResponseType type)
    {
        return type switch
        {
            SequenceResponseType.Success => SequenceStaticResponses.Success,
            SequenceResponseType.NotFound => SequenceStaticResponses.NotFound,
            SequenceResponseType.AlreadyExists => SequenceStaticResponses.AlreadyExists,
            SequenceResponseType.InvalidInput => SequenceStaticResponses.InvalidInput,
            SequenceResponseType.MaxValueExceeded => SequenceStaticResponses.MaxValueExceeded,
            SequenceResponseType.MustRetry => SequenceStaticResponses.MustRetry,
            SequenceResponseType.Aborted => SequenceStaticResponses.Aborted,
            _ => SequenceStaticResponses.Error
        };
    }

    internal static SequenceResponseType Map(KeyValueResponseType response)
    {
        return response switch
        {
            KeyValueResponseType.DoesNotExist => SequenceResponseType.NotFound,
            KeyValueResponseType.NotSet => SequenceResponseType.AlreadyExists,
            KeyValueResponseType.InvalidInput => SequenceResponseType.InvalidInput,
            KeyValueResponseType.MustRetry => SequenceResponseType.MustRetry,
            KeyValueResponseType.Aborted => SequenceResponseType.Aborted,
            _ => SequenceResponseType.Error
        };
    }

    /// <summary>Where the durable mark must move to, and the values the caller takes from the new block.</summary>
    private readonly record struct SequenceBump(long Start, long End, long Ceiling);

    /// <summary>
    /// One sequence's resident state: the last snapshot of the durable record plus the window of values
    /// this node has exclusively reserved out of it.
    /// </summary>
    private sealed class SequenceBlock
    {
        public string StorageKey { get; }

        /// <summary>Last read (or written) durable record. Its <c>CurrentValue</c> is the global high-water mark.</summary>
        public SequenceState State { get; set; }

        /// <summary>Revision of <see cref="State"/>, used as the compare-and-swap precondition.</summary>
        public long Revision { get; set; }

        /// <summary>Last value handed out from the reserved window.</summary>
        public long Current { get; set; }

        /// <summary>Last value the reserved window may hand out.</summary>
        public long Ceiling { get; set; }

        public long LastUsed { get; set; }

        public SequenceBlock(string storageKey, SequenceState state, long revision)
        {
            StorageKey = storageKey;
            State = state;
            Revision = revision;
            Current = state.CurrentValue;
            Ceiling = state.CurrentValue;
        }
    }
}
