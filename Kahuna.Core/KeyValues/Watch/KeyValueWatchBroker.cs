using System.Collections.Concurrent;

using Kommander.Time;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// The node's registry of open key-value watches, indexed by data partition, and the point where the apply
/// path hands each committed mutation to the watches that cover it.
///
/// <para><b>Producers.</b> The replicator publishes a committed write made outside a transaction after it applied
/// the record; the prepared-intent store publishes a durable transaction's write when its intent goes from
/// pending to committed. Kommander applies one partition's entries one at a time and in log order, but different
/// partitions apply at the same time on different threads, so <see cref="Publish"/> has concurrent callers.</para>
///
/// <para><b>Cost on the apply path.</b> With no watch on the node, <see cref="HasSubscribers"/> is one volatile
/// read. With watches, a publish reads the partition's subscription array without a lock (the array is replaced
/// whole on every registration change), and allocates an event only after a scope matched.</para>
/// </summary>
internal sealed class KeyValueWatchBroker
{
    private readonly object registrationLock = new();

    private readonly ConcurrentDictionary<int, KeyValueWatch[]> byPartition = new();

    // Partition registrations on the node (a watch over three partitions counts three). Zero is the fast path.
    private int registrations;

    // The last progress watermark sent on each partition. Written and read by that partition's apply thread only
    // (progress and events of one partition are published from its ordered apply), so a plain read is current.
    private readonly ConcurrentDictionary<int, HLCTimestamp> publishedWatermarks = new();

    // Open watches on the node (a watch over three partitions counts once), against maxWatches.
    private int openWatches;

    private readonly int maxWatches;

    private readonly long maxBufferedBytes;

    /// <param name="maxWatches">The most watches open on the node at once; zero or less for no limit.</param>
    /// <param name="maxBufferedBytes">The byte budget of each watch's buffer; zero or less for the default.</param>
    public KeyValueWatchBroker(int maxWatches = 0, long maxBufferedBytes = 0)
    {
        this.maxWatches = maxWatches;
        this.maxBufferedBytes = maxBufferedBytes > 0 ? maxBufferedBytes : KeyValueWatch.DefaultMaxBufferedBytes;
    }

    /// <summary>Whether transaction ends are published (see <see cref="PublishTransactionEnd"/>). On only while the
    /// commit admission window is on: the intent store counts a transaction's pending intents per partition only
    /// then. A watch opened while it is on remembers the transactions it delivered an event of until their end.</summary>
    internal bool TracksTransactionEnds { get; set; }

    /// <summary>Whether any watch on the node covers <paramref name="partitionId"/>. One volatile read when the
    /// node has no watch at all.</summary>
    public bool HasSubscribers(int partitionId) =>
        Volatile.Read(ref registrations) != 0 && byPartition.ContainsKey(partitionId);

    /// <summary>The number of open watch registrations on the node, one per covered partition.</summary>
    public int RegistrationCount => Volatile.Read(ref registrations);

    /// <summary>
    /// Opens a watch over <paramref name="partitions"/>. When this returns, every later publish on those
    /// partitions is matched against the watch. <paramref name="resolvedPartitions"/> is every partition the
    /// scope covers (see <see cref="KeyValueWatch.ResolvedPartitions"/>); null means <paramref name="partitions"/>.
    /// Throws when the node's watch limit is reached; see <see cref="TryRegister"/>.
    /// </summary>
    public KeyValueWatch Register(
        KeyValueWatchScope scope,
        bool keysOnly,
        int capacity,
        int[] partitions,
        int[]? resolvedPartitions = null,
        IReadOnlyCollection<int>? replayPartitions = null,
        IReadOnlyDictionary<int, HLCTimestamp>? resumeCursors = null) =>
        TryRegister(scope, keysOnly, capacity, partitions, resolvedPartitions, replayPartitions, resumeCursors)
        ?? throw new InvalidOperationException("The node's watch limit is reached.");

    /// <summary>
    /// <see cref="Register"/>, or null when the node already has its limit of open watches.
    /// <para><paramref name="replayPartitions"/> names the partitions whose history a replay delivers before their
    /// live frames: those frames are held back from this moment until <see cref="KeyValueWatch.CompleteReplay"/>.
    /// <paramref name="resumeCursors"/> holds the cursor of each resumed partition, which the watch applies to every
    /// frame it delivers there.</para>
    /// </summary>
    public KeyValueWatch? TryRegister(
        KeyValueWatchScope scope,
        bool keysOnly,
        int capacity,
        int[] partitions,
        int[]? resolvedPartitions = null,
        IReadOnlyCollection<int>? replayPartitions = null,
        IReadOnlyDictionary<int, HLCTimestamp>? resumeCursors = null)
    {
        if (Interlocked.Increment(ref openWatches) > maxWatches && maxWatches > 0)
        {
            Interlocked.Decrement(ref openWatches);
            return null;
        }

        KeyValueWatch watch = new(this, scope, keysOnly, capacity, partitions, resolvedPartitions ?? partitions, replayPartitions, resumeCursors, maxBufferedBytes, TracksTransactionEnds);
        KeyValueWatchMetrics.ActiveWatches.Add(1);

        lock (registrationLock)
        {
            foreach (int partitionId in partitions)
            {
                KeyValueWatch[] current = byPartition.TryGetValue(partitionId, out KeyValueWatch[]? existing) ? existing : [];
                KeyValueWatch[] next = new KeyValueWatch[current.Length + 1];
                current.CopyTo(next, 0);
                next[^1] = watch;
                byPartition[partitionId] = next;
            }

            Interlocked.Add(ref registrations, partitions.Length);
        }

        return watch;
    }

    /// <summary>The number of watches open on the node.</summary>
    public int OpenWatchCount => Volatile.Read(ref openWatches);

    /// <summary>Removes <paramref name="watch"/> from every partition it covers. Called once, by the watch's end.</summary>
    internal void Unregister(KeyValueWatch watch)
    {
        Interlocked.Decrement(ref openWatches);

        lock (registrationLock)
        {
            int removed = 0;

            foreach (int partitionId in watch.Partitions)
            {
                if (!byPartition.TryGetValue(partitionId, out KeyValueWatch[]? current))
                    continue;

                int index = Array.IndexOf(current, watch);
                if (index < 0)
                    continue;

                removed++;

                if (current.Length == 1)
                {
                    byPartition.TryRemove(partitionId, out _);
                    continue;
                }

                KeyValueWatch[] next = new KeyValueWatch[current.Length - 1];
                Array.Copy(current, 0, next, 0, index);
                Array.Copy(current, index + 1, next, index, current.Length - index - 1);
                byPartition[partitionId] = next;
            }

            Interlocked.Add(ref registrations, -removed);
        }
    }

    /// <summary>
    /// Hands one committed mutation to every watch on <paramref name="partitionId"/> whose scope covers
    /// <paramref name="key"/>. The caller has already applied the mutation to this replica's state. The value is
    /// shared with the event, never copied; a keys-only watch gets an event without it. <paramref name="logIndex"/> is
    /// the entry that carried the mutation, which a watch replaying the partition's history needs for its hand-over.
    /// <paramref name="heldBehindPendingIntent"/> says the key still has a pending intent whose commit event has not
    /// come: each watch holds the event behind it (see <see cref="PublishIntentSettled"/>).
    /// </summary>
    public void Publish(
        int partitionId,
        KeyValueWatchEventKind kind,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp commitTimestamp,
        HLCTimestamp transactionId,
        long logIndex,
        string? origin = null,
        bool heldBehindPendingIntent = false)
    {
        if (Volatile.Read(ref registrations) == 0 || !byPartition.TryGetValue(partitionId, out KeyValueWatch[]? watches))
            return;

        // A watermark promised that nothing at or below it arrives later on this partition. An event below it means
        // the promise was broken (nodes with different commit admission settings), so the watches end instead of
        // delivering it out of contract.
        if (publishedWatermarks.TryGetValue(partitionId, out HLCTimestamp promised) && commitTimestamp <= promised)
        {
            EndPartition(partitionId, KeyValueWatchLagReason.WatermarkViolated);
            return;
        }

        // Only a set carries its value: a delete has none, and an extend does not change it.
        byte[]? eventValue = kind == KeyValueWatchEventKind.Set ? value : null;

        KeyValueWatchFrame? withValue = null;
        KeyValueWatchFrame? keysOnly = null;
        string? holdKey = heldBehindPendingIntent ? key : null;

        foreach (KeyValueWatch watch in watches)
        {
            if (!watch.Scope.Matches(key))
                continue;

            KeyValueWatchFrame frame;

            if (watch.KeysOnly || eventValue is null)
                frame = keysOnly ??= KeyValueWatchFrame.ForEvent(new(key, kind, null, revision, expires, commitTimestamp, transactionId, partitionId, origin));
            else
                frame = withValue ??= KeyValueWatchFrame.ForEvent(new(key, kind, eventValue, revision, expires, commitTimestamp, transactionId, partitionId, origin));

            watch.Deliver(frame, logIndex, holdKey);
            KeyValueWatchMetrics.EventsDelivered.Add(1);
        }
    }

    /// <summary>
    /// Tells the watches on <paramref name="partitionId"/> that <paramref name="transactionId"/> has no more events
    /// there: its last pending intent on the partition committed. Each watch delivers it only when it delivered an
    /// event of the transaction, from the live stream or from a replay. Called from the partition's ordered apply,
    /// after that intent's event.
    /// </summary>
    public void PublishTransactionEnd(int partitionId, HLCTimestamp transactionId, long logIndex)
    {
        if (Volatile.Read(ref registrations) == 0 || !byPartition.TryGetValue(partitionId, out KeyValueWatch[]? watches))
            return;

        KeyValueWatchFrame frame = KeyValueWatchFrame.ForTransactionEnd(partitionId, transactionId);

        foreach (KeyValueWatch watch in watches)
            watch.Deliver(frame, logIndex);
    }

    /// <summary>
    /// Tells the watches on <paramref name="partitionId"/> that cover <paramref name="key"/> that the key's pending
    /// intent left pending at the entry <paramref name="logIndex"/> (committed, aborted or removed): the events held
    /// behind it follow. Called from the partition's ordered apply, after the intent's commit event, if any.
    /// </summary>
    public void PublishIntentSettled(int partitionId, string key, long logIndex)
    {
        if (Volatile.Read(ref registrations) == 0 || !byPartition.TryGetValue(partitionId, out KeyValueWatch[]? watches))
            return;

        foreach (KeyValueWatch watch in watches)
        {
            if (watch.Scope.Matches(key))
                watch.Release(partitionId, key, logIndex);
        }
    }

    /// <summary>
    /// Hands a progress watermark to every watch on <paramref name="partitionId"/>: no event with a commit timestamp
    /// at or below <paramref name="watermark"/> arrives later on the partition. Called from the partition's ordered
    /// apply after the entry's events were published. A watermark at or below the last one sent is not sent again,
    /// so the watermark a watch sees never goes down. Returns whether a frame was sent.
    /// </summary>
    public bool PublishProgress(int partitionId, HLCTimestamp watermark, long logIndex)
    {
        if (Volatile.Read(ref registrations) == 0 || !byPartition.TryGetValue(partitionId, out KeyValueWatch[]? watches))
            return false;

        if (publishedWatermarks.TryGetValue(partitionId, out HLCTimestamp previous) && watermark <= previous)
            return false;

        publishedWatermarks[partitionId] = watermark;

        KeyValueWatchFrame frame = KeyValueWatchFrame.ForProgress(partitionId, watermark);

        foreach (KeyValueWatch watch in watches)
            watch.Deliver(frame, logIndex);

        return true;
    }

    /// <summary>The last progress watermark sent on <paramref name="partitionId"/>, or false when none was sent.</summary>
    public bool TryGetPublishedWatermark(int partitionId, out HLCTimestamp watermark) =>
        publishedWatermarks.TryGetValue(partitionId, out watermark);

    /// <summary>
    /// Ends every watch on <paramref name="partitionId"/> with <see cref="KeyValueWatchFrameType.Lagged"/>: this
    /// replica changed the partition's state in a way that carries no per-key events, or cannot prove it emitted
    /// every event, so a consumer must not assume continuity past this point.
    /// </summary>
    public void EndPartition(int partitionId, KeyValueWatchLagReason reason)
    {
        if (Volatile.Read(ref registrations) == 0 || !byPartition.TryGetValue(partitionId, out KeyValueWatch[]? watches))
            return;

        KeyValueWatchFrame frame = KeyValueWatchFrame.ForLagged(partitionId, reason, HLCTimestamp.Zero);

        foreach (KeyValueWatch watch in watches)
            watch.End(frame);
    }

    /// <summary>Ends every open watch on the node with <see cref="KeyValueWatchFrameType.Lagged"/>.</summary>
    public void EndAll(KeyValueWatchLagReason reason)
    {
        KeyValueWatchFrame frame = KeyValueWatchFrame.ForLagged(-1, reason, HLCTimestamp.Zero);

        foreach (KeyValueWatch watch in OpenWatches())
            watch.End(frame);
    }

    /// <summary>A snapshot of every open watch on the node, each listed once.</summary>
    public List<KeyValueWatch> OpenWatches()
    {
        HashSet<KeyValueWatch> seen = new(ReferenceEqualityComparer.Instance);
        List<KeyValueWatch> open = [];

        foreach (KeyValuePair<int, KeyValueWatch[]> entry in byPartition)
        {
            foreach (KeyValueWatch watch in entry.Value)
            {
                if (seen.Add(watch))
                    open.Add(watch);
            }
        }

        return open;
    }
}
