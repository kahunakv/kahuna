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
/// <para><b>Bounded.</b> The watch buffers at most its capacity of frames for a slow reader. A reader that falls
/// further behind loses the whole backlog and receives one <see cref="KeyValueWatchFrameType.Lagged"/> frame
/// instead; the producer never blocks and the buffer never grows past the capacity. Every terminal frame
/// (<see cref="KeyValueWatchFrame.IsTerminal"/>) is the last one the watch delivers.</para>
///
/// <para>See <see cref="KeyValueWatchEvent"/> for the ordering and delivery contract of the events.</para>
/// </summary>
public sealed class KeyValueWatch : IAsyncDisposable
{
    private readonly Channel<KeyValueWatchFrame> channel = Channel.CreateUnbounded<KeyValueWatchFrame>(new()
    {
        // The producers are the apply threads of every partition the scope covers.
        SingleWriter = false,
        // The reader, plus the ending producer that drains the dropped backlog.
        SingleReader = false
    });

    private readonly KeyValueWatchBroker broker;

    private readonly int capacity;

    // Frames written and not yet read. Raised by producers, lowered by the reader.
    private int queued;

    // The frame that ended the watch; set once. The reader delivers it after anything it already took and
    // drops everything else still buffered.
    private KeyValueWatchFrame? terminal;

    // The partitions whose history is still being replayed, each with the live frames held back meanwhile and the
    // log index each of them came from. Null once no replay runs (the common case: one volatile read per frame).
    // Guarded by handoverLock.
    private volatile Dictionary<int, List<(long LogIndex, KeyValueWatchFrame Frame)>>? replaying;

    private readonly object handoverLock = new();

    // Live frames one replaying partition may hold back before the watch ends with an overflow. Larger than the
    // capacity: the live tail keeps arriving while the replay reads the log, and an overflow here would make the
    // consumer resume from the same cursor and replay the same history again.
    private int HoldbackLimit => Math.Max(capacity, 1) * 16;

    internal KeyValueWatch(KeyValueWatchBroker broker, KeyValueWatchScope scope, bool keysOnly, int capacity, int[] partitions, int[] resolvedPartitions, IReadOnlyCollection<int>? replayPartitions = null)
    {
        this.broker = broker;
        this.capacity = capacity;
        Scope = scope;
        KeysOnly = keysOnly;
        Partitions = partitions;
        ResolvedPartitions = resolvedPartitions;

        // Set before the broker can deliver anything, so no live frame of a replaying partition slips past the
        // hand-over.
        if (replayPartitions is { Count: > 0 })
        {
            Dictionary<int, List<(long, KeyValueWatchFrame)>> held = new(replayPartitions.Count);
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
    /// overflow when the reader is a full capacity behind. While the frame's partition is being replayed the frame is
    /// held back instead, to be delivered after the replay if the replay did not cover its entry. Never blocks.
    /// Called from apply threads.
    /// </summary>
    internal void Deliver(KeyValueWatchFrame frame, long logIndex)
    {
        if (replaying is not null)
        {
            lock (handoverLock)
            {
                if (replaying is { } held && held.TryGetValue(frame.PartitionId, out List<(long LogIndex, KeyValueWatchFrame Frame)>? holdback))
                {
                    if (holdback.Count >= HoldbackLimit)
                    {
                        End(KeyValueWatchFrame.ForLagged(frame.PartitionId, KeyValueWatchLagReason.Overflow, HLCTimestamp.Zero));
                        return;
                    }

                    holdback.Add((logIndex, frame));
                    return;
                }
            }
        }

        Deliver(frame);
    }

    /// <summary>
    /// Buffers one replayed frame for the reader, waiting while the reader is a full capacity behind: a replay reads
    /// the log off the apply path and can wait, and an overflow would only make the consumer replay the same
    /// history again. Returns false once the watch ended.
    /// </summary>
    internal async ValueTask<bool> DeliverReplayedAsync(KeyValueWatchFrame frame, CancellationToken cancellationToken)
    {
        while (Volatile.Read(ref queued) >= capacity)
        {
            if (Volatile.Read(ref terminal) is not null)
                return false;

            await Task.Delay(2, cancellationToken).ConfigureAwait(false);
        }

        if (Volatile.Read(ref terminal) is not null)
            return false;

        Deliver(frame);
        return true;
    }

    /// <summary>
    /// Ends the replay of <paramref name="partitionId"/>: the replay covered every entry at or below
    /// <paramref name="replayedThrough"/>, so the held-back live frames of those entries are dropped and the rest are
    /// delivered in the order they arrived, before any later live frame of the partition.
    /// </summary>
    internal void CompleteReplay(int partitionId, long replayedThrough)
    {
        lock (handoverLock)
        {
            if (replaying is not { } held || !held.Remove(partitionId, out List<(long LogIndex, KeyValueWatchFrame Frame)>? holdback))
                return;

            foreach ((long logIndex, KeyValueWatchFrame frame) in holdback)
            {
                if (logIndex > replayedThrough)
                    Deliver(frame);
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

    /// <summary>Buffers one frame for the reader, or ends the watch with an overflow when the reader is a full
    /// capacity behind. Never blocks.</summary>
    private void Deliver(KeyValueWatchFrame frame)
    {
        if (Volatile.Read(ref terminal) is not null)
            return;

        if (Interlocked.Increment(ref queued) > capacity)
        {
            End(KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.Overflow, HLCTimestamp.Zero));
            return;
        }

        channel.Writer.TryWrite(frame);
    }

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

        // Drop the backlog now rather than when the reader gets to it; a reader that stopped reading would
        // otherwise keep it alive.
        long dropped = 0;
        while (channel.Reader.TryRead(out _))
            dropped++;

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
