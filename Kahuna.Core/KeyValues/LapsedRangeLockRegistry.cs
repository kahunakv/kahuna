
using System.Collections.Concurrent;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The transactions that held a range lock on this node which the node stopped honoring before they released
/// it: the lease ran out with the lock still in the table, or a session-owned lock outlived its ceiling.
///
/// <para>A range lock is a lease in the memory of the partition leader. When the lease ends, the leader lets
/// other transactions into the range and nothing tells the holder, whose coordinator still lists the lock. An
/// unbroken leadership term says the lock was not dropped by a leader change; it says nothing about the lease.
/// So the leader remembers whom it stopped honoring, and the commit-time lock proof asks: a transaction found
/// here must not commit on what it read under the lock.</para>
///
/// <para>The entry is written in the same actor turn that first finds the lock dead, before that turn grants
/// or admits anything the lock would have refused. A proof that reads the registry after any such effect
/// therefore sees the entry. A proof that read it earlier is the lock's holder past its last operation, for
/// which the lapse is an early release.</para>
///
/// <para>Node-wide and shared by every key-value actor, because the proof is answered for a partition and not
/// by the actor that held the lock. It lives and dies with the node's in-memory lock state: a leader change is
/// caught by the term, and a restart drops both.</para>
/// </summary>
internal sealed class LapsedRangeLockRegistry
{
    private const int SweepIntervalMs = 30_000;

    /// <summary>Retention used when the caller has no session bound to offer.</summary>
    private const int DefaultRetentionMs = 600_000;

    /// <summary>Holder transaction → the <see cref="Environment.TickCount64"/> past which the entry is forgotten.</summary>
    private readonly ConcurrentDictionary<HLCTimestamp, long> forgetAtTick = new();

    private long nextSweepTick = Environment.TickCount64 + SweepIntervalMs;

    /// <summary>Entries currently held, for tests and diagnostics.</summary>
    internal int Count => forgetAtTick.Count;

    /// <summary>
    /// Records that a range lock of <paramref name="transactionId"/> is no longer honored here. The entry is
    /// kept for <paramref name="retentionMs"/> of monotonic time, which the caller sets to the longest a
    /// session can live: past it no session of that transaction is left to ask.
    /// </summary>
    public void Record(HLCTimestamp transactionId, int retentionMs)
    {
        if (transactionId == HLCTimestamp.Zero)
            return;

        long now = Environment.TickCount64;

        forgetAtTick[transactionId] = now + (retentionMs > 0 ? retentionMs : DefaultRetentionMs);

        // One caller wins the sweep; the others only record.
        long due = Volatile.Read(ref nextSweepTick);
        if (now >= due && Interlocked.CompareExchange(ref nextSweepTick, now + SweepIntervalMs, due) == due)
            Sweep(now);
    }

    /// <summary>Whether a range lock of <paramref name="transactionId"/> stopped being honored on this node.</summary>
    public bool Contains(HLCTimestamp transactionId) => forgetAtTick.ContainsKey(transactionId);

    private void Sweep(long now)
    {
        foreach (KeyValuePair<HLCTimestamp, long> entry in forgetAtTick)
        {
            // Removes the pair as read, so an entry recorded again in the meantime keeps its new deadline.
            if (entry.Value <= now)
                forgetAtTick.TryRemove(entry);
        }
    }
}
