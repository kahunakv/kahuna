using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

using Kommander.Time;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// An open watch on this node: the frames of every committed mutation in its scope that this replica applies
/// after the watch was registered, on every partition the scope covers.
///
/// <para><b>Registration is synchronous.</b> When the call that returned this watch completes, every entry this
/// replica applies afterwards is matched against it. An entry whose apply had already started is matched only if
/// it reached the match after the registration; one that did not was applied to this replica's state before the
/// registration completed, so a read made after the registration observes it.</para>
///
/// <para><b>Bounded.</b> The watch buffers at most its capacity of frames, and at most its byte budget of keys and
/// values, for a slow reader. A reader that falls further behind loses the whole backlog and receives one
/// <see cref="KeyValueWatchFrameType.Lagged"/> frame instead; the producer never blocks. Every terminal frame
/// (<see cref="KeyValueWatchFrame.IsTerminal"/>) is the last one the watch delivers.</para>
///
/// <para><b>Per-key order behind a pending intent.</b> A durable transaction's value is visible at commit, but its
/// event comes when its intent settles. A write made outside a transaction that lands in between is applied over
/// the committed value; its event is held behind the key until the intent leaves pending (its commit event is
/// delivered first), so the first delivery of each revision of a key stays in revision order.</para>
///
/// <para><b>Resume.</b> On a partition resumed from a cursor, no event at or below the cursor and no progress at or
/// below it is delivered, from the replay or live, for the life of the watch.</para>
///
/// <para>See <see cref="KeyValueWatchEvent"/> for the ordering and delivery contract of the events.</para>
/// </summary>
public sealed class KeyValueWatch : IAsyncDisposable
{
    /// <summary>The byte budget of a watch when the node sets none.</summary>
    internal const long DefaultMaxBufferedBytes = 64L * 1024 * 1024;

    // Bytes a frame costs beyond its key and value, for the byte budget.
    private const int FrameOverheadBytes = 64;

    private readonly Channel<KeyValueWatchFrame> channel = Channel.CreateUnbounded<KeyValueWatchFrame>(new()
    {
        // The producers are the apply threads of every partition the scope covers.
        SingleWriter = false,
        // The reader, plus the ending producer that drains the dropped backlog.
        SingleReader = false
    });

    private readonly KeyValueWatchBroker broker;

    private readonly int capacity;

    private readonly long maxBufferedBytes;

    private readonly bool tracksTransactionEnds;

    // Cancelled when the watch ends: the replays of the watch stop on it.
    private readonly CancellationTokenSource lifetime = new();

    // Frames written (or held behind a key) and not yet read, and their bytes. Raised by producers, lowered by the
    // reader.
    private int queued;

    private long queuedBytes;

    // The frame that ended the watch; set once. The reader delivers it after anything it already took and
    // drops everything else still buffered.
    private KeyValueWatchFrame? terminal;

    // The cursor of each resumed partition. Read-only after construction.
    private readonly Dictionary<int, HLCTimestamp>? cursors;

    // One item of a replaying partition's held-back live stream: a frame (with the key it is held behind, if any) or
    // the release of a key.
    private readonly record struct HeldBack(long LogIndex, KeyValueWatchFrame? Frame, string? Key, long Bytes);

    // The partitions whose history is still being replayed, each with the live items held back meanwhile and the
    // log index each of them came from. Null once no replay runs (the common case: one volatile read per frame).
    // Guarded by handoverLock, with heldBackBytes.
    private volatile Dictionary<int, List<HeldBack>>? replaying;

    private long heldBackBytes;

    private readonly object handoverLock = new();

    // Frames held behind a pending intent, per key, in arrival order. Guarded by keyLock; heldKeys is the number of
    // keys with a queue, read without the lock on the fast path. All the items of one key come from one thread at a
    // time (its partition's ordered apply, or its replay while the live stream of that partition is held back), so
    // the fast path never races an item of the same key. Lock order: handoverLock, then keyLock.
    private readonly Dictionary<string, Queue<KeyValueWatchFrame>> heldByKey = new(StringComparer.Ordinal);

    private volatile int heldKeys;

    private readonly object keyLock = new();

    // The (partition, transaction) pairs this watch delivered an event of whose transaction end it has not delivered.
    private readonly ConcurrentDictionary<(int PartitionId, HLCTimestamp TransactionId), byte> openTransactions = new();

    // Live frames one replaying partition may hold back before the watch ends with an overflow. Larger than the
    // capacity: the live tail keeps arriving while the replay reads the log, and an overflow here would make the
    // consumer resume from the same cursor and replay the same history again.
    private long HoldbackLimit => Math.Max(capacity, 1) * 16L;

    internal KeyValueWatch(
        KeyValueWatchBroker broker,
        KeyValueWatchScope scope,
        bool keysOnly,
        int capacity,
        int[] partitions,
        int[] resolvedPartitions,
        IReadOnlyCollection<int>? replayPartitions = null,
        IReadOnlyDictionary<int, HLCTimestamp>? resumeCursors = null,
        long maxBufferedBytes = DefaultMaxBufferedBytes,
        bool tracksTransactionEnds = false)
    {
        this.broker = broker;
        this.capacity = capacity;
        this.maxBufferedBytes = maxBufferedBytes > 0 ? maxBufferedBytes : DefaultMaxBufferedBytes;
        this.tracksTransactionEnds = tracksTransactionEnds;
        Scope = scope;
        KeysOnly = keysOnly;
        Partitions = partitions;
        ResolvedPartitions = resolvedPartitions;

        if (resumeCursors is { Count: > 0 })
        {
            cursors = new Dictionary<int, HLCTimestamp>(resumeCursors.Count);
            foreach ((int partitionId, HLCTimestamp cursor) in resumeCursors)
            {
                if (cursor != HLCTimestamp.Zero)
                    cursors[partitionId] = cursor;
            }
        }

        // Set before the broker can deliver anything, so no live frame of a replaying partition slips past the
        // hand-over.
        if (replayPartitions is { Count: > 0 })
        {
            Dictionary<int, List<HeldBack>> held = new(replayPartitions.Count);
            foreach (int partitionId in replayPartitions)
                held[partitionId] = [];

            replaying = held;
        }
    }

    /// <summary>The keys the watch covers.</summary>
    public KeyValueWatchScope Scope { get; }

    /// <summary>Whether events carry no value.</summary>
    public bool KeysOnly { get; }

    /// <summary>The data partitions the watch is registered on: every partition the scope covered when the
    /// watch was registered, or the subset the request named.</summary>
    public IReadOnlyList<int> Partitions { get; }

    /// <summary>Every data partition the scope covered when the watch was registered, sorted. Equal to
    /// <see cref="Partitions"/> unless the request named a subset. A range-map change that alters this set ends
    /// the watch.</summary>
    internal IReadOnlyList<int> ResolvedPartitions { get; }

    /// <summary>Whether the watch ended (a terminal frame is set or was delivered).</summary>
    public bool IsEnded => Volatile.Read(ref terminal) is not null;

    /// <summary>Cancelled when the watch ends.</summary>
    internal CancellationToken Lifetime => lifetime.Token;

    /// <summary>
    /// Reads the watch's frames until a terminal frame, which is delivered last, or until
    /// <paramref name="cancellationToken"/> is cancelled. Only one reader may read a watch.
    /// </summary>
    public async IAsyncEnumerable<KeyValueWatchFrame> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ChannelReader<KeyValueWatchFrame> reader = channel.Reader;
        bool ended = false;

        while (!ended && await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (reader.TryRead(out KeyValueWatchFrame? frame))
            {
                // A terminal frame overrides the backlog: whatever is still buffered was dropped.
                if (Volatile.Read(ref terminal) is not null)
                {
                    ended = true;
                    break;
                }

                Interlocked.Decrement(ref queued);
                Interlocked.Add(ref queuedBytes, -SizeOf(frame));
                yield return frame;
            }
        }

        KeyValueWatchFrame? end = Volatile.Read(ref terminal);
        if (end is not null && !ReferenceEquals(end, ClosedFrame))
            yield return end;
    }

    /// <summary>Ends the watch and unregisters it. Idempotent.</summary>
    public ValueTask DisposeAsync()
    {
        End(null);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Buffers one live frame for the reader, from the entry at <paramref name="logIndex"/>, or ends the watch with an
    /// overflow when the reader is too far behind. <paramref name="holdKey"/> names the key whose pending intent the
    /// frame must wait behind, or is null. While the frame's partition is being replayed the frame is held back
    /// instead, to be delivered after the replay if the replay did not cover its entry. Never blocks. Called from
    /// apply threads.
    /// </summary>
    internal void Deliver(KeyValueWatchFrame frame, long logIndex, string? holdKey = null)
    {
        if (replaying is not null && TryHoldBack(frame.PartitionId, new HeldBack(logIndex, frame, holdKey, SizeOf(frame))))
            return;

        Route(frame, holdKey);
    }

    /// <summary>
    /// Releases the frames held behind <paramref name="key"/>: its pending intent left pending at the entry
    /// <paramref name="logIndex"/> of <paramref name="partitionId"/> (after its commit event, if any, was delivered).
    /// Called from apply threads.
    /// </summary>
    internal void Release(int partitionId, string key, long logIndex)
    {
        if (replaying is not null && TryHoldBack(partitionId, new HeldBack(logIndex, null, key, 0)))
            return;

        ReleaseKey(key);
    }

    /// <summary>
    /// Buffers one replayed frame for the reader, waiting while the reader is too far behind: a replay reads the log
    /// off the apply path and can wait, and an overflow would only make the consumer replay the same history again.
    /// Returns false once the watch ended.
    /// </summary>
    internal async ValueTask<bool> DeliverReplayedAsync(KeyValueWatchFrame frame, string? holdKey, CancellationToken cancellationToken)
    {
        long size = SizeOf(frame);

        while (Volatile.Read(ref queued) >= capacity
               || (Volatile.Read(ref queued) > 0 && Interlocked.Read(ref queuedBytes) + size > maxBufferedBytes))
        {
            if (Volatile.Read(ref terminal) is not null)
                return false;

            await Task.Delay(2, cancellationToken).ConfigureAwait(false);
        }

        if (Volatile.Read(ref terminal) is not null)
            return false;

        Route(frame, holdKey);
        return true;
    }

    /// <summary>The replay's counterpart of <see cref="Release"/>.</summary>
    internal void ReleaseReplayed(string key) => ReleaseKey(key);

    /// <summary>
    /// Ends the replay of <paramref name="partitionId"/>: the replay covered every entry at or below
    /// <paramref name="replayedThrough"/>, so the held-back live items of those entries are dropped and the rest are
    /// delivered in the order they arrived, before any later live item of the partition. Frames the replay held
    /// behind a key stay held until a live release of that key.
    /// </summary>
    internal void CompleteReplay(int partitionId, long replayedThrough)
    {
        lock (handoverLock)
        {
            if (replaying is not { } held || !held.Remove(partitionId, out List<HeldBack>? holdback))
                return;

            foreach (HeldBack item in holdback)
            {
                heldBackBytes -= item.Bytes;

                if (item.LogIndex <= replayedThrough)
                    continue;

                if (item.Frame is { } frame)
                    Route(frame, item.Key);
                else if (item.Key is { } key)
                    ReleaseKey(key);
            }

            if (held.Count == 0)
                replaying = null;
        }
    }

    /// <summary>Whether the watch still replays history on <paramref name="partitionId"/>.</summary>
    internal bool IsReplaying(int partitionId)
    {
        lock (handoverLock)
            return replaying is { } held && held.ContainsKey(partitionId);
    }

    private bool TryHoldBack(int partitionId, HeldBack item)
    {
        lock (handoverLock)
        {
            if (replaying is not { } held || !held.TryGetValue(partitionId, out List<HeldBack>? holdback))
                return false;

            if (holdback.Count >= HoldbackLimit || heldBackBytes + item.Bytes + Interlocked.Read(ref queuedBytes) > maxBufferedBytes)
            {
                End(KeyValueWatchFrame.ForLagged(partitionId, KeyValueWatchLagReason.Overflow, HLCTimestamp.Zero));
                return true;
            }

            holdback.Add(item);
            heldBackBytes += item.Bytes;
            return true;
        }
    }

    /// <summary>
    /// The last stage before the reader: drops what the resume cursor covers, delivers a transaction end only to a
    /// watch that delivered an event of the transaction, and keeps a key's frames behind its pending intent.
    /// </summary>
    private void Route(KeyValueWatchFrame frame, string? holdKey)
    {
        if (cursors is not null && cursors.TryGetValue(frame.PartitionId, out HLCTimestamp cursor))
        {
            if (frame.Event is { } covered && covered.CommitTimestamp <= cursor)
                return;

            if (frame.Type == KeyValueWatchFrameType.Progress && frame.Timestamp <= cursor)
                return;
        }

        if (frame.Type == KeyValueWatchFrameType.TransactionEnd)
        {
            if (openTransactions.TryRemove((frame.PartitionId, frame.TransactionId), out _))
                Buffer(frame, counted: false);

            return;
        }

        if (frame.Event is not { } @event)
        {
            Buffer(frame, counted: false);
            return;
        }

        if (tracksTransactionEnds && @event.TransactionId != HLCTimestamp.Zero)
            openTransactions.TryAdd((@event.PartitionId, @event.TransactionId), 0);

        if (holdKey is null && heldKeys == 0)
        {
            Buffer(frame, counted: false);
            return;
        }

        lock (keyLock)
        {
            if (holdKey is not null)
            {
                Hold(holdKey, frame);
                return;
            }

            if (!heldByKey.Remove(@event.Key, out Queue<KeyValueWatchFrame>? waiting))
            {
                Buffer(frame, counted: false);
                return;
            }

            // A write outside a transaction that still finds frames held for its key keeps behind them. The intent's
            // own commit event goes first, then everything that waited behind it.
            if (@event.TransactionId == HLCTimestamp.Zero)
            {
                heldByKey[@event.Key] = waiting;
                Hold(@event.Key, frame);
                return;
            }

            heldKeys = heldByKey.Count;
            Buffer(frame, counted: false);
            Flush(waiting);
        }
    }

    private void ReleaseKey(string key)
    {
        if (heldKeys == 0)
            return;

        lock (keyLock)
        {
            if (!heldByKey.Remove(key, out Queue<KeyValueWatchFrame>? waiting))
                return;

            heldKeys = heldByKey.Count;
            Flush(waiting);
        }
    }

    // keyLock held.
    private void Hold(string key, KeyValueWatchFrame frame)
    {
        if (!Count(frame))
            return;

        if (!heldByKey.TryGetValue(key, out Queue<KeyValueWatchFrame>? waiting))
        {
            waiting = new Queue<KeyValueWatchFrame>(2);
            heldByKey[key] = waiting;
            heldKeys = heldByKey.Count;
        }

        waiting.Enqueue(frame);
    }

    private void Flush(Queue<KeyValueWatchFrame> waiting)
    {
        while (waiting.TryDequeue(out KeyValueWatchFrame? frame))
            Buffer(frame, counted: true);
    }

    /// <summary>Hands one frame to the reader. <paramref name="counted"/> says the frame was already counted
    /// against the capacity and the byte budget while it was held behind a key.</summary>
    private void Buffer(KeyValueWatchFrame frame, bool counted)
    {
        if (Volatile.Read(ref terminal) is not null)
            return;

        if (!counted && !Count(frame))
            return;

        channel.Writer.TryWrite(frame);
    }

    // Counts one frame against the capacity and the byte budget, or ends the watch with an overflow. A single frame
    // larger than the budget is let through when nothing else is buffered.
    private bool Count(KeyValueWatchFrame frame)
    {
        int frames = Interlocked.Increment(ref queued);
        long bytes = Interlocked.Add(ref queuedBytes, SizeOf(frame));

        if (frames > capacity || (frames > 1 && bytes > maxBufferedBytes))
        {
            End(KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.Overflow, HLCTimestamp.Zero));
            return false;
        }

        return true;
    }

    private static long SizeOf(KeyValueWatchFrame frame) =>
        frame.Event is { } e ? FrameOverheadBytes + e.Key.Length * 2L + (e.Value?.LongLength ?? 0) : FrameOverheadBytes;

    /// <summary>
    /// Ends the watch with <paramref name="frame"/> (null for a close the consumer asked for, which delivers no
    /// frame), unregisters it from the broker and drops the buffered backlog. Only the first call has an effect;
    /// returns whether this call ended the watch.
    /// </summary>
    internal bool End(KeyValueWatchFrame? frame)
    {
        KeyValueWatchFrame end = frame ?? ClosedFrame;

        if (Interlocked.CompareExchange(ref terminal, end, null) is not null)
            return false;

        broker.Unregister(this);
        channel.Writer.TryComplete();
        KeyValueWatchMetrics.ActiveWatches.Add(-1);

        // The registrations on the token are the replays' waits, which do not throw; an end must never fail.
        try
        {
            lifetime.Cancel();
        }
        catch (AggregateException)
        {
        }

        // Drop the backlog now rather than when the reader gets to it; a reader that stopped reading would
        // otherwise keep it alive.
        long dropped = 0;
        while (channel.Reader.TryRead(out _))
            dropped++;

        lock (keyLock)
        {
            foreach (Queue<KeyValueWatchFrame> waiting in heldByKey.Values)
                dropped += waiting.Count;

            heldByKey.Clear();
            heldKeys = 0;
        }

        if (frame is not null)
        {
            if (dropped > 0)
                KeyValueWatchMetrics.FramesDropped.Add(dropped);

            if (frame.Type == KeyValueWatchFrameType.Lagged)
                KeyValueWatchMetrics.StreamsLagged.Add(1, new KeyValuePair<string, object?>("reason", frame.LagReason.ToString()));
        }

        return true;
    }

    // The terminal state of a watch its consumer closed. Never delivered: a closed watch ends its reader
    // without a frame.
    private static readonly KeyValueWatchFrame ClosedFrame = KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.None, HLCTimestamp.Zero);

    /// <summary>Whether the watch was closed by its consumer rather than ended by the node.</summary>
    internal bool ClosedByConsumer => ReferenceEquals(Volatile.Read(ref terminal), ClosedFrame);
}
