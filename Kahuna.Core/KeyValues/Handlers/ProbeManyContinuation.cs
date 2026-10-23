using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Stages 2 and 3 of a batched probe whose stage 1 left keys that need the backend. Every such key gets the
/// read a single <c>TryGet</c> of it would get, so a key's answer is the single read's answer:
/// <list type="bullet">
/// <item>A key with no backend read in flight gets its own <see cref="PointReadContinuation"/> or
/// <see cref="SnapshotReadContinuation"/>, registered under the same slot a single read uses, so a single read
/// of the key that arrives meanwhile joins it instead of reading again. The probe is that read's primary, but
/// through this batch: the continuation's own primary reply is empty.</item>
/// <item>A key whose read is already in flight joins that read as one more waiter.</item>
/// </list>
/// <para>Stage 2 runs once, on the read scheduler, for all owned reads: the latest-head misses as one batched
/// backend point read (one RocksDB <c>MultiGet</c>), then each snapshot read's history load. One
/// <c>ResumeRead</c> then reconciles every key in one actor turn and answers the batch, once the joined reads
/// have answered too.</para>
/// <para>A failure answers each pending key <c>MustRetry</c>, never the whole batch, so a key whose Shared lock
/// the probe took keeps reporting it.</para>
/// </summary>
internal sealed class ProbeManyContinuation : ReadContinuation
{
    private sealed class Slot
    {
        public required int Index { get; init; }

        public required bool Locked { get; init; }

        /// <summary>The read this probe owns for the key, or null when it joined a read in flight.</summary>
        public ReadContinuation? Own { get; init; }

        /// <summary>The joined read's answer for the key, when it joined one.</summary>
        public Task<KeyValueResponse?>? Joined { get; init; }

        /// <summary>True once the key is answered; its owned read is then no longer this probe's to cancel.</summary>
        public bool Done { get; set; }
    }

    private readonly KeyValueProbeResult[] results;

    private readonly List<Slot> slots = new(4);

    private List<string>? latestKeys;

    private List<PointReadContinuation>? latestReads;

    private List<SnapshotReadContinuation>? snapshotReads;

    private List<Task>? joinedReads;

    internal ProbeManyContinuation(KeyValueProbeResult[] results, KeyValueReplyRef promise) : base(promise)
    {
        this.results = results;
    }

    /// <summary>True when the probe owns at least one backend read, so stage 2 has work.</summary>
    internal bool HasOwnReads => latestReads is not null || snapshotReads is not null;

    /// <summary>The answers of the joined reads, which stage 3 must wait for besides its own stage 2.</summary>
    internal List<Task>? JoinedReads => joinedReads;

    /// <summary>Registers a latest-head miss this probe reads itself.</summary>
    internal void AddLatestRead(int index, bool locked, string key, PointReadContinuation read)
    {
        (latestKeys ??= []).Add(key);
        (latestReads ??= []).Add(read);
        slots.Add(new() { Index = index, Locked = locked, Own = read });
    }

    /// <summary>Registers a snapshot read this probe loads itself.</summary>
    internal void AddSnapshotRead(int index, bool locked, SnapshotReadContinuation read)
    {
        (snapshotReads ??= []).Add(read);
        slots.Add(new() { Index = index, Locked = locked, Own = read });
    }

    /// <summary>Registers a key that joined a read already in flight.</summary>
    internal void AddJoinedRead(int index, bool locked, Task<KeyValueResponse?> answer)
    {
        (joinedReads ??= []).Add(answer);
        slots.Add(new() { Index = index, Locked = locked, Joined = answer });
    }

    /// <summary>
    /// Stage 2, scheduler thread: the backend reads of every owned key. Touches only the owned continuations'
    /// stage-2 fields; every actor-owned mutation waits for the resume turn. Skipped once the deadline sweep
    /// expired the probe.
    /// </summary>
    internal void Load(IPersistenceBackend backend, KeyValuePointReadExecutor pointReads, int recentRevisions)
    {
        if (Cancelled)
            return;

        if (latestKeys is not null)
        {
            ReadOnlyKeyValueEntry?[] rows = pointReads.ExecuteBatch([.. latestKeys]);

            if (rows.Length != latestKeys.Count)
                throw new InvalidOperationException($"Batched point read returned {rows.Length} rows for {latestKeys.Count} keys.");

            for (int i = 0; i < rows.Length; i++)
                latestReads![i].DiskResult = rows[i];
        }

        if (snapshotReads is not null)
        {
            foreach (SnapshotReadContinuation read in snapshotReads)
                read.Load(backend, recentRevisions);
        }
    }

    internal override void Execute(KeyValueContext context) => Resolve(Complete(context));

    /// <summary>
    /// Stage 3, actor thread: reconciles every owned read exactly as its own resume would, answers the single
    /// reads that joined it, takes the answers of the reads this probe joined, and returns the batch answer.
    /// </summary>
    internal KeyValueResponse Complete(KeyValueContext context)
    {
        context.PendingProbeBatches.Remove(this);

        foreach (Slot slot in slots)
        {
            KeyValueResponse? answer;

            if (slot.Own is { } own)
            {
                if (own.Cancelled)
                {
                    // The deadline sweep expired this read and already answered its joiners.
                    answer = KeyValueStaticResponses.MustRetryResponse;
                }
                else if (Faulted)
                {
                    own.Fail(context, KeyValueStaticResponses.MustRetryResponse);
                    answer = KeyValueStaticResponses.MustRetryResponse;
                }
                else
                {
                    answer = own switch
                    {
                        PointReadContinuation point => point.Reconcile(context),
                        SnapshotReadContinuation snapshot => snapshot.Reconcile(context),
                        _ => KeyValueStaticResponses.MustRetryResponse
                    };

                    own.Resolve(answer);
                }
            }
            else
            {
                Task<KeyValueResponse?> joined = slot.Joined!;
                answer = joined.IsCompletedSuccessfully ? joined.Result : KeyValueStaticResponses.MustRetryResponse;
            }

            slot.Done = true;
            results[slot.Index] = ToResult(answer, slot.Locked);
        }

        return KeyValueResponse.ForProbe(results);
    }

    /// <summary>
    /// Failure and deadline path (<see cref="ReadContinuation.Fail"/>, <see cref="ReadContinuation.Expire"/>):
    /// cancels every owned read that is not answered yet, which answers its joiners <c>MustRetry</c>, and answers
    /// the batch first with every pending key as <c>MustRetry</c>, keeping each key's lock flag. The whole-batch
    /// answer the caller of this path resolves afterwards then finds the reply already set.
    /// </summary>
    internal override void RemovePendingKey(KeyValueContext context)
    {
        context.PendingProbeBatches.Remove(this);

        foreach (Slot slot in slots)
        {
            if (slot.Done)
                continue;

            if (slot.Own is { Cancelled: false } own)
                own.Expire(context, KeyValueStaticResponses.MustRetryResponse);

            slot.Done = true;
            results[slot.Index] = KeyValueProbeResult.MustRetry with { Locked = slot.Locked };
        }

        Resolve(KeyValueResponse.ForProbe(results));
    }

    /// <summary>
    /// Answers the batch at dispatch time when its backend read could not be scheduled: every owned read is
    /// failed (its joiners answer <c>MustRetry</c>), and every key that was waiting answers <c>MustRetry</c>.
    /// </summary>
    internal KeyValueResponse FailBeforeDispatch(KeyValueContext context)
    {
        foreach (Slot slot in slots)
        {
            slot.Own?.Fail(context, KeyValueStaticResponses.MustRetryResponse);
            slot.Done = true;
            results[slot.Index] = KeyValueProbeResult.MustRetry with { Locked = slot.Locked };
        }

        return KeyValueResponse.ForProbe(results);
    }

    /// <summary>The probe result for one key from the answer a single read of it gives.</summary>
    internal static KeyValueProbeResult ToResult(KeyValueResponse? answer, bool locked) =>
        answer is null
            ? KeyValueProbeResult.MustRetry with { Type = KeyValueResponseType.Errored, Locked = locked }
            : new(answer.Type, answer.Entry, answer.HolderTransactionId, locked);
}
