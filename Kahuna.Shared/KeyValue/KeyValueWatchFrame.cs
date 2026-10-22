using System.Text.Json.Serialization;
using Kahuna.Shared.Communication.Rest;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

/// <summary>The kind of committed mutation a watch event reports.</summary>
public enum KeyValueWatchEventKind
{
    /// <summary>The key was set to a new value at a new revision.</summary>
    Set = 0,

    /// <summary>The key was deleted.</summary>
    Deleted = 1,

    /// <summary>The key's expiry was refreshed. The value and the revision did not change.</summary>
    Extended = 2
}

/// <summary>The kind of frame a watch stream delivers.</summary>
public enum KeyValueWatchFrameType
{
    /// <summary>One committed mutation of a key in the scope (<see cref="KeyValueWatchFrame.Event"/>).</summary>
    Event = 0,

    /// <summary>
    /// The partition's progress watermark (<see cref="KeyValueWatchFrame.Timestamp"/>): no event with a commit
    /// timestamp at or below it arrives later on that partition. Not sent until the apply-time admission check
    /// is on.
    /// </summary>
    Progress = 1,

    /// <summary>The partition delivered every event of one transaction (<see cref="KeyValueWatchFrame.TransactionId"/>).</summary>
    TransactionEnd = 2,

    /// <summary>
    /// The stream lost continuity and ended (<see cref="KeyValueWatchFrame.LagReason"/>). Events between the last
    /// delivered one and the end can be missing. The consumer reopens the watch.
    /// </summary>
    Lagged = 3,

    /// <summary>A resume position is below the history this replica retains. The stream ended.</summary>
    HistoryUnavailable = 4,

    /// <summary>The watch was refused (<see cref="KeyValueWatchFrame.Refusal"/>). The stream ended.</summary>
    Refused = 5,

    /// <summary>
    /// The first frame of a watch on the remote surfaces: the watch is registered on every partition in
    /// <see cref="KeyValueWatchFrame.Partitions"/>. The client consumes it before it returns the watch, and an
    /// in-process watch never yields it.
    /// </summary>
    Opened = 6
}

/// <summary>Why a watch stream ended with <see cref="KeyValueWatchFrameType.Lagged"/>.</summary>
public enum KeyValueWatchLagReason
{
    None = 0,

    /// <summary>The consumer did not read fast enough and its backlog reached the subscription capacity. The
    /// backlog was dropped.</summary>
    Overflow = 1,

    /// <summary>The replica replaced the partition's state from a snapshot, a range import or a purge, which
    /// carries no per-key events.</summary>
    PartitionStateReplaced = 2,

    /// <summary>The replica failed to apply a committed entry, or repaired a key outside the ordered apply, so
    /// it cannot prove that it emitted every event.</summary>
    ApplyDiscontinuity = 3,

    /// <summary>The range map changed, so the set of partitions the scope covers changed.</summary>
    RangeMapChanged = 4,

    /// <summary>The node is shutting down.</summary>
    Shutdown = 5,

    /// <summary>The connection to the node that served the watch ended without a terminal frame. Set by the
    /// client only.</summary>
    Disconnected = 6,

    /// <summary>An event arrived with a commit timestamp at or below a progress watermark the replica already
    /// sent, so the watermark's promise no longer holds. This happens only when the nodes of a group run the commit
    /// admission window with different settings.</summary>
    WatermarkViolated = 7
}

/// <summary>
/// One committed mutation of a key, as a watch reports it.
///
/// <para><b>Ordering and delivery.</b> Delivery is at-least-once. For one key, the first delivery of each mutation
/// arrives in commit order: every mutation of a key has a later commit timestamp than the one before it, an extend
/// included (it keeps the revision). A repeat of an older mutation can arrive later (a re-driven transaction that
/// settles twice, for example, or a resume), so a consumer keeps the last event it applied for each key and drops an
/// event that does not <see cref="Supersedes(KeyValueWatchEvent?)"/> it. The revision alone does not identify a
/// mutation: an extend keeps it, and so does a set with no revision. Across the
/// keys of one partition, events arrive in the partition's log order, which is not commit-timestamp order:
/// events between two <see cref="KeyValueWatchFrameType.Progress"/> frames can arrive out of commit-timestamp
/// order. Across partitions no order is promised; the events of one transaction share a commit timestamp and a
/// transaction id, so a consumer can group them.</para>
///
/// <para><b>Visibility.</b> An event is emitted after the replica that serves the watch applied the mutation
/// to its local state. A committed value can be readable elsewhere before its event arrives: the value of a
/// durable transaction becomes readable at commit, and its event is emitted when the transaction settles on the
/// partition.</para>
///
/// <para><b>The value is shared.</b> <see cref="Value"/> can be the same array the writer sent. Never change
/// it. It is null for a delete, for an extend, and for a keys-only watch.</para>
/// </summary>
public sealed class KeyValueWatchEvent
{
    public string Key { get; }

    public KeyValueWatchEventKind Kind { get; }

    // A delete or keys-only event has no value, which must stay apart from an empty value on every wire.
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; }

    public long Revision { get; }

    public HLCTimestamp Expires { get; }

    /// <summary>The mutation's commit timestamp: the transaction's commit timestamp for a transactional write,
    /// the write's last-modified timestamp otherwise.</summary>
    public HLCTimestamp CommitTimestamp { get; }

    /// <summary>The transaction id, or <see cref="HLCTimestamp.Zero"/> for a write outside a transaction.</summary>
    public HLCTimestamp TransactionId { get; }

    /// <summary>The data partition whose log carried the mutation.</summary>
    public int PartitionId { get; }

    /// <summary>What produced the transaction that wrote the key, as the transaction named it at start (for example a
    /// schema rewrite or a cleanup sweep); null for an ordinary transaction and for a write outside a transaction. Set
    /// only on a cluster that enables transaction origin tags.</summary>
    public string? Origin { get; }

    public KeyValueWatchEvent(
        string key,
        KeyValueWatchEventKind kind,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp commitTimestamp,
        HLCTimestamp transactionId,
        int partitionId,
        string? origin = null)
    {
        Key = key;
        Kind = kind;
        Value = value;
        Revision = revision;
        Expires = expires;
        CommitTimestamp = commitTimestamp;
        TransactionId = transactionId;
        PartitionId = partitionId;
        Origin = origin;
    }

    /// <summary>
    /// Whether this event is a later mutation of its key than <paramref name="last"/>, the last event the consumer
    /// applied for the same key (null when it holds none). Ordered by commit timestamp, then by revision; a repeat of
    /// an event already applied compares equal and is not newer.
    /// </summary>
    public bool Supersedes(KeyValueWatchEvent? last) =>
        last is null || Supersedes(last.Revision, last.CommitTimestamp);

    /// <summary>
    /// Whether this event is a later mutation of its key than the one at <paramref name="revision"/> and
    /// <paramref name="commitTimestamp"/> that the consumer applied last; see <see cref="Supersedes(KeyValueWatchEvent?)"/>.
    /// </summary>
    public bool Supersedes(long revision, HLCTimestamp commitTimestamp)
    {
        int byTime = CommitTimestamp.CompareTo(commitTimestamp);
        return byTime > 0 || (byTime == 0 && Revision > revision);
    }
}

/// <summary>One frame of a watch stream. <see cref="Type"/> says which of the other members carry data.</summary>
public sealed class KeyValueWatchFrame
{
    public KeyValueWatchFrameType Type { get; }

    /// <summary>The partition the frame is about, or -1 when it is about the whole watch.</summary>
    public int PartitionId { get; }

    /// <summary>Set for <see cref="KeyValueWatchFrameType.Event"/>.</summary>
    public KeyValueWatchEvent? Event { get; }

    /// <summary>The watermark of a <see cref="KeyValueWatchFrameType.Progress"/> frame, or the last watermark a
    /// <see cref="KeyValueWatchFrameType.Lagged"/> frame can vouch for (<see cref="HLCTimestamp.Zero"/> when
    /// none was sent).</summary>
    public HLCTimestamp Timestamp { get; }

    /// <summary>Set for <see cref="KeyValueWatchFrameType.TransactionEnd"/>.</summary>
    public HLCTimestamp TransactionId { get; }

    /// <summary>Set for <see cref="KeyValueWatchFrameType.Lagged"/>.</summary>
    public KeyValueWatchLagReason LagReason { get; }

    /// <summary>Set for <see cref="KeyValueWatchFrameType.Refused"/>.</summary>
    public KeyValueResponseType Refusal { get; }

    /// <summary>The partitions the watch is registered on, for <see cref="KeyValueWatchFrameType.Opened"/>.
    /// Empty otherwise.</summary>
    public IReadOnlyList<int> Partitions { get; }

    /// <summary>
    /// For a <see cref="KeyValueWatchFrameType.Refused"/> frame with <see cref="KeyValueResponseType.DoesNotExist"/>:
    /// the replicas of each partition the scope covers, so the client can open the watch on nodes that host
    /// them. Empty otherwise.
    /// </summary>
    public IReadOnlyList<KeyValueWatchRoute> Routes { get; }

    private KeyValueWatchFrame(
        KeyValueWatchFrameType type,
        int partitionId,
        KeyValueWatchEvent? @event,
        HLCTimestamp timestamp,
        HLCTimestamp transactionId,
        KeyValueWatchLagReason lagReason,
        KeyValueResponseType refusal,
        IReadOnlyList<int>? partitions = null,
        IReadOnlyList<KeyValueWatchRoute>? routes = null)
    {
        Type = type;
        PartitionId = partitionId;
        Event = @event;
        Timestamp = timestamp;
        TransactionId = transactionId;
        LagReason = lagReason;
        Refusal = refusal;
        Partitions = partitions ?? [];
        Routes = routes ?? [];
    }

    public static KeyValueWatchFrame ForEvent(KeyValueWatchEvent @event) =>
        new(KeyValueWatchFrameType.Event, @event.PartitionId, @event, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueWatchLagReason.None, KeyValueResponseType.Get);

    public static KeyValueWatchFrame ForProgress(int partitionId, HLCTimestamp watermark) =>
        new(KeyValueWatchFrameType.Progress, partitionId, null, watermark, HLCTimestamp.Zero, KeyValueWatchLagReason.None, KeyValueResponseType.Get);

    public static KeyValueWatchFrame ForTransactionEnd(int partitionId, HLCTimestamp transactionId) =>
        new(KeyValueWatchFrameType.TransactionEnd, partitionId, null, HLCTimestamp.Zero, transactionId, KeyValueWatchLagReason.None, KeyValueResponseType.Get);

    public static KeyValueWatchFrame ForLagged(int partitionId, KeyValueWatchLagReason reason, HLCTimestamp lastWatermark) =>
        new(KeyValueWatchFrameType.Lagged, partitionId, null, lastWatermark, HLCTimestamp.Zero, reason, KeyValueResponseType.Get);

    public static KeyValueWatchFrame ForHistoryUnavailable(int partitionId) =>
        new(KeyValueWatchFrameType.HistoryUnavailable, partitionId, null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueWatchLagReason.None, KeyValueResponseType.Get);

    public static KeyValueWatchFrame ForRefused(int partitionId, KeyValueResponseType refusal, IReadOnlyList<KeyValueWatchRoute>? routes = null) =>
        new(KeyValueWatchFrameType.Refused, partitionId, null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueWatchLagReason.None, refusal, null, routes);

    public static KeyValueWatchFrame ForOpened(IReadOnlyList<int> partitions) =>
        new(KeyValueWatchFrameType.Opened, -1, null, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueWatchLagReason.None, KeyValueResponseType.Get, partitions);

    /// <summary>Whether the frame ends the stream.</summary>
    public bool IsTerminal => Type is KeyValueWatchFrameType.Lagged or KeyValueWatchFrameType.HistoryUnavailable or KeyValueWatchFrameType.Refused;
}

/// <summary>
/// A request to watch a scope. <paramref name="KeysOnly"/> drops the value from every event (the key, kind,
/// revision and commit timestamp stay), so a consumer that only needs to know which key changed does not pay
/// for the value. <paramref name="Capacity"/> bounds the frames the stream buffers for a slow consumer; zero
/// takes the node's default. A consumer that falls further behind gets <see cref="KeyValueWatchFrameType.Lagged"/>.
///
/// <para><paramref name="Partitions"/> limits the watch to some of the partitions the scope covers. A client
/// sets it when no single node hosts every partition of the scope, and it opens one watch per node. Null
/// watches every partition the scope covers. A listed partition the scope no longer covers is refused with
/// <see cref="KeyValueResponseType.MustRetry"/>, because the range map moved since the client resolved it.</para>
///
/// <para><b>Resume.</b> <paramref name="ResumeFrom"/> replays, before the live events, every event above a timestamp
/// that the node's retained log still holds: on each partition, from <paramref name="ResumePartitions"/> when it names
/// the partition, else from <paramref name="ResumeFrom"/>; zero on both means live only. The cursor is the last
/// progress watermark the consumer received for the partition (not the timestamp of its last event: between two
/// watermarks events can arrive out of timestamp order). Events between the cursor and the end of the replay can be
/// delivered again, so a consumer drops an event that does not <see cref="KeyValueWatchEvent.Supersedes(KeyValueWatchEvent?)"/>
/// the last one it holds for that key. No event and no watermark at or below the cursor is delivered on the partition.
/// A partition whose history below the cursor is gone ends the watch with
/// <see cref="KeyValueWatchFrameType.HistoryUnavailable"/>; the documented recovery is a new snapshot. A resume needs
/// the commit admission window on every node.</para>
/// </summary>
public sealed record KeyValueWatchRequest(
    KeyValueWatchScope Scope,
    KeyValueDurability Durability = KeyValueDurability.Persistent,
    bool KeysOnly = false,
    int Capacity = 0,
    IReadOnlyList<int>? Partitions = null,
    HLCTimestamp ResumeFrom = default,
    IReadOnlyDictionary<int, HLCTimestamp>? ResumePartitions = null)
{
    /// <summary>The cursor <paramref name="partitionId"/> resumes from, or zero for live only.</summary>
    public HLCTimestamp ResumeCursorOf(int partitionId) =>
        ResumePartitions is not null && ResumePartitions.TryGetValue(partitionId, out HLCTimestamp cursor) ? cursor : ResumeFrom;

    /// <summary>Whether the request resumes any partition.</summary>
    public bool Resumes => ResumeFrom != HLCTimestamp.Zero || ResumePartitions is { Count: > 0 };
}

/// <summary>
/// Where a watch can be served for one partition: whether the node that answered hosts it, and the client
/// endpoints of its replicas, the believed leader first. Any replica serves a watch. An empty endpoint list
/// means that the answering node advertises no endpoint for the replicas.
/// </summary>
public sealed record KeyValueWatchRoute(int PartitionId, bool HostedHere, IReadOnlyList<string> Endpoints);
