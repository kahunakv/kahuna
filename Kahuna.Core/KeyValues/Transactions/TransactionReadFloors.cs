using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

using Kommander.Time;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Holds the registered latest reads of each transaction to one point in time, on the node the transaction
/// issues its reads through.
///
/// <para><b>The problem.</b> The first read of a key by a transaction pins the committed head of that key at the
/// moment of the read. Nothing ties the pins of different keys together. A transaction that reads key A, and then
/// reads key B after another transaction changed both and committed, is handed the old A and the new B: two values
/// that were never committed together. Its commit is refused, because A no longer has the revision it read, but
/// the application has the values before that.</para>
///
/// <para><b>The rule.</b> Each transaction has a floor: a time on this process's clock at which every key it has
/// read held the revision it read. A read of a new key is consistent with the earlier ones when the revision it
/// answered has been the committed head since the floor or longer; the actor that answered states since when it
/// has seen that head (<see cref="ReadOnlyKeyValueEntry.HeadSeenTicks"/>). When it cannot state that, or states a
/// later time, the floor is refreshed: every key read before is read again, outside the transaction, and must still
/// have the revision the transaction read. The new key was pinned before those checks ran, and each earlier key
/// kept its revision from its own pin to its check, so at the moment between the pin and the first check all of
/// them held the revisions read; that moment becomes the floor. If one of them changed, the read is refused.</para>
///
/// <para><b>Why the commit timestamp is not used.</b> A key's <c>LastModified</c> is stamped when its write is
/// prepared, before the write becomes the committed head, and by a clock that may belong to another node. A head
/// can therefore become visible after the floor while carrying a stamp below it. The time an actor first saw a
/// head is taken on the answering process's own clock after the head was visible, so it can only be late, which
/// costs a refresh and never hides a change.</para>
///
/// <para><b>Since when a head is the head.</b> The key's actor stamps its process clock when a committed write is
/// installed as the head, and when it loads or creates the entry. A load stamps late, which costs a refresh and
/// never hides a change. The stamp travels with the published head record, so a read answered from that record
/// on the caller's thread states it too.</para>
///
/// <para><b>What states nothing.</b> A read answered by another node, a missing key, a pin taken from a committed
/// intent the resident head does not include yet. Each of these refreshes. The keys of one batch are consistent
/// with each other when every head was first seen no later than the earliest answer of the batch; otherwise they
/// are checked like earlier keys.</para>
///
/// <para>A transaction asks for the floor when it starts (<c>KeyValueTransactionOptions.ReadFloor</c>). Its state
/// lives in memory on the node that started it and issues its reads, and is dropped when that node commits or
/// rolls the transaction back. A state nobody dropped is removed after
/// <see cref="StaleAfter"/>.</para>
/// </summary>
internal sealed class TransactionReadFloors
{
    private static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Reads of keys new to their transaction, tagged by <c>outcome</c>: <c>first</c> (nothing read before),
    /// <c>covered</c> (the head was already seen at the floor, no refresh), <c>refreshed</c> (the earlier keys
    /// were checked and held), <c>refused</c> (an earlier key changed, or a check gave no answer).
    /// </summary>
    private static readonly Counter<long> Decisions =
        Meter.CreateCounter<long>(
            "kahuna.kv.read_floor.decisions",
            description: "Transactional reads of a new key judged against the transaction's read floor, tagged by outcome.");

    /// <summary>Keys read again, outside the transaction, to refresh a floor.</summary>
    private static readonly Counter<long> RefreshedKeys =
        Meter.CreateCounter<long>(
            "kahuna.kv.read_floor.refreshed_keys",
            description: "Keys re-read to check that a transaction's earlier reads still hold.");

    private static readonly KeyValuePair<string, object?> First = new("outcome", "first");
    private static readonly KeyValuePair<string, object?> Covered = new("outcome", "covered");
    private static readonly KeyValuePair<string, object?> Refreshed = new("outcome", "refreshed");
    private static readonly KeyValuePair<string, object?> Refused = new("outcome", "refused");

    /// <summary>How long a state nobody dropped is kept.</summary>
    private static readonly TimeSpan StaleAfter = TimeSpan.FromMinutes(10);

    private const int SweepEvery = 4096;

    /// <summary>One key a transaction read, as it read it.</summary>
    internal readonly record struct Observation(string Key, KeyValueDurability Durability, bool Exists, long Revision, long HeadSeenTicks, long ServedTicks);

    /// <summary>Reads a key's committed head outside any transaction: whether it exists, and its revision.</summary>
    internal delegate ValueTask<(KeyValueResponseType Type, long Revision)> HeadReader(string key, KeyValueDurability durability, CancellationToken cancellationToken);

    private sealed class State : IDisposable
    {
        public readonly SemaphoreSlim Gate = new(1, 1);
        public readonly Dictionary<(string, KeyValueDurability), (bool Exists, long Revision)> Keys = [];
        public long FloorTicks;
        public long TouchedTicks = Stopwatch.GetTimestamp();
        public bool Refused;

        // The gate is disposed once, after the state left the table and the last read that had already found it
        // is done with it. Disposing it under a read would fault that read's release, and would leave a read that
        // waits on it waiting for ever.
        private readonly Lock usersLock = new();
        private int users;
        private bool retired;
        private bool disposed;

        /// <summary>
        /// Registers a read that is about to wait on <see cref="Gate"/>. False when the state was already
        /// disposed: the transaction ended on this node, and its reads are no longer held.
        /// </summary>
        public bool TryEnter()
        {
            lock (usersLock)
            {
                if (disposed)
                    return false;

                users++;
                return true;
            }
        }

        /// <summary>Ends a read registered with <see cref="TryEnter"/>, after it released the gate or gave up on it.</summary>
        public void Leave()
        {
            lock (usersLock)
            {
                if (--users != 0 || !retired)
                    return;

                disposed = true;
            }

            Gate.Dispose();
        }

        /// <summary>
        /// Called once the state left the table: disposes the gate now when no read uses it, or leaves that to the
        /// last read that does.
        /// </summary>
        public void Dispose()
        {
            lock (usersLock)
            {
                retired = true;

                if (users != 0 || disposed)
                    return;

                disposed = true;
            }

            Gate.Dispose();
        }
    }

    private readonly ConcurrentDictionary<HLCTimestamp, State> states = new();

    private int admissions;

    /// <summary>Transactions with a tracked floor.</summary>
    internal int Count => states.Count;

    /// <summary>Starts holding the reads of a transaction that asked for a read floor.</summary>
    internal void Track(HLCTimestamp transactionId) => states.TryAdd(transactionId, new State());

    /// <summary>Whether the reads of <paramref name="transactionId"/> are held to a floor.</summary>
    internal bool Tracks(HLCTimestamp transactionId) => !states.IsEmpty && states.ContainsKey(transactionId);

    /// <summary>Drops the state of a transaction that ended.</summary>
    internal void Forget(HLCTimestamp transactionId)
    {
        if (states.TryRemove(transactionId, out State? state))
            state.Dispose();
    }

    /// <summary>
    /// Judges the keys one read of <paramref name="transactionId"/> just answered. True when they are consistent
    /// with everything the transaction read before; false when the read must answer <c>Aborted</c>.
    /// </summary>
    internal async ValueTask<bool> Admit(HLCTimestamp transactionId, IReadOnlyList<Observation> observed, HeadReader readHead, CancellationToken cancellationToken)
    {
        if (observed.Count == 0)
            return true;

        if (Interlocked.Increment(ref admissions) % SweepEvery == 0)
            Sweep();

        // A transaction that did not ask, or that this node already ended, is not held.
        if (!states.TryGetValue(transactionId, out State? state) || !state.TryEnter())
            return true;

        try
        {
            return await AdmitHeld(state, observed, readHead, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            state.Leave();
        }
    }

    /// <summary>Judges one read under the transaction's gate, so two reads of one transaction are judged in turn.</summary>
    private static async ValueTask<bool> AdmitHeld(State state, IReadOnlyList<Observation> observed, HeadReader readHead, CancellationToken cancellationToken)
    {
        await state.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            state.TouchedTicks = Stopwatch.GetTimestamp();

            // A transaction refused once stays refused: its earlier reads no longer hold.
            if (state.Refused)
            {
                Decisions.Add(1, Refused);
                return false;
            }

            List<Observation>? fresh = null;

            for (int i = 0; i < observed.Count; i++)
            {
                Observation one = observed[i];

                // A key read before is answered from its pin; the coordinator's read set judges a second answer.
                if (state.Keys.ContainsKey((one.Key, one.Durability)))
                    continue;

                (fresh ??= new(observed.Count - i)).Add(one);
            }

            if (fresh is null)
                return true;

            // Each new key held its revision from the time its head was first seen to the time it was answered.
            // When the latest first-seen time is not after the earliest answer time, all of them held their
            // revisions together at the earliest answer time. One key always does.
            long latestSeen = 0;
            long earliestServed = long.MaxValue;
            bool known = true;

            for (int i = 0; i < fresh.Count; i++)
            {
                if (fresh[i].HeadSeenTicks <= 0 || fresh[i].ServedTicks <= 0)
                {
                    known = false;
                    break;
                }

                latestSeen = Math.Max(latestSeen, fresh[i].HeadSeenTicks);
                earliestServed = Math.Min(earliestServed, fresh[i].ServedTicks);
            }

            bool oneTurn = known && latestSeen <= earliestServed;
            bool nothingBefore = state.Keys.Count == 0;

            if (nothingBefore && oneTurn)
            {
                state.FloorTicks = earliestServed;
                Remember(state, fresh);
                Decisions.Add(1, First);
                return true;
            }

            if (!nothingBefore && oneTurn && state.FloorTicks != 0 && latestSeen <= state.FloorTicks)
            {
                Remember(state, fresh);
                Decisions.Add(1, Covered);
                return true;
            }

            // Refresh. Every key below was pinned before this line, and is checked after it.
            long floor = Stopwatch.GetTimestamp();
            int checkedKeys = 0;

            foreach (KeyValuePair<(string Key, KeyValueDurability Durability), (bool Exists, long Revision)> earlier in state.Keys)
            {
                checkedKeys++;

                if (!await Holds(earlier.Key.Key, earlier.Key.Durability, earlier.Value.Exists, earlier.Value.Revision, readHead, cancellationToken).ConfigureAwait(false))
                    return Refuse(state, checkedKeys);
            }

            if (!oneTurn)
            {
                for (int i = 0; i < fresh.Count; i++)
                {
                    checkedKeys++;

                    if (!await Holds(fresh[i].Key, fresh[i].Durability, fresh[i].Exists, fresh[i].Revision, readHead, cancellationToken).ConfigureAwait(false))
                        return Refuse(state, checkedKeys);
                }
            }

            state.FloorTicks = floor;
            Remember(state, fresh);
            RefreshedKeys.Add(checkedKeys);
            Decisions.Add(1, nothingBefore ? First : Refreshed);
            return true;
        }
        finally
        {
            state.Gate.Release();
        }
    }

    private static bool Refuse(State state, int checkedKeys)
    {
        state.Refused = true;
        RefreshedKeys.Add(checkedKeys);
        Decisions.Add(1, Refused);
        return false;
    }

    private static void Remember(State state, List<Observation> fresh)
    {
        for (int i = 0; i < fresh.Count; i++)
            state.Keys[(fresh[i].Key, fresh[i].Durability)] = (fresh[i].Exists, fresh[i].Revision);
    }

    /// <summary>
    /// Whether the committed head of <paramref name="key"/> is still what the transaction read. A transient answer
    /// is asked again a few times; an answer that never comes counts as a change.
    /// </summary>
    private static async ValueTask<bool> Holds(string key, KeyValueDurability durability, bool exists, long revision, HeadReader readHead, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt < 5; attempt++)
        {
            (KeyValueResponseType type, long headRevision) = await readHead(key, durability, cancellationToken).ConfigureAwait(false);

            switch (type)
            {
                case KeyValueResponseType.Get:
                    return exists && headRevision == revision;

                case KeyValueResponseType.DoesNotExist:
                    return !exists;

                case KeyValueResponseType.MustRetry:
                case KeyValueResponseType.WaitingForReplication:
                    await Task.Delay(1 << attempt, cancellationToken).ConfigureAwait(false);
                    continue;

                default:
                    return false;
            }
        }

        return false;
    }

    private void Sweep()
    {
        long now = Stopwatch.GetTimestamp();

        foreach (KeyValuePair<HLCTimestamp, State> pair in states)
        {
            if (Stopwatch.GetElapsedTime(pair.Value.TouchedTicks, now) > StaleAfter && states.TryRemove(pair.Key, out State? stale))
                stale.Dispose();
        }
    }
}
