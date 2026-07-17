
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Represents a background write request.
///
/// <para>
/// One is created for every persistent mutation, so the high-frequency <c>QueueStore*</c> variants are
/// pooled (see <see cref="BackgroundWriteRequestPool"/>) rather than allocated per mutation. The fields
/// are settable only so the pool can reset a recycled instance before it is handed out; once a request
/// has been sent to the writer actor it is treated as immutable, and it is returned to the pool only
/// after the writer has copied it into a <see cref="PersistenceRequestItem"/> and no longer references
/// it. Low-frequency signal requests (Flush / FlushAndNotify) are not pooled.
/// </para>
/// </summary>
public sealed class BackgroundWriteRequest
{
    public BackgroundWriteType Type { get; private set; }

    public int PartitionId { get; private set; }

    public string Key { get; private set; }

    public byte[]? Value { get; private set; }

    public long Revision { get; private set; }

    public HLCTimestamp Expires { get; private set; }

    public HLCTimestamp LastUsed { get; private set; }

    public HLCTimestamp LastModified { get; private set; }

    public int State { get; private set; }

    public bool NoRevision { get; private set; }

    public TaskCompletionSource<bool>? CompletionSource { get; private set; }

    public BackgroundWriteRequest(
        BackgroundWriteType type,
        int partitionId,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        int state,
        bool noRevision = false
    )
    {
        Type = type;
        PartitionId = partitionId;
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
        NoRevision = noRevision;
    }

    public BackgroundWriteRequest(BackgroundWriteType type)
    {
        Type = type;
        Key = "";
    }

    public BackgroundWriteRequest(BackgroundWriteType type, TaskCompletionSource<bool> completionSource)
    {
        Type = type;
        Key = "";
        CompletionSource = completionSource;
    }

    /// <summary>
    /// Repopulates a recycled instance for a <c>QueueStore*</c> write. Called only by the pool while it
    /// still owns the object exclusively, before it is handed to a caller.
    /// </summary>
    internal void Reset(
        BackgroundWriteType type,
        int partitionId,
        string key,
        byte[]? value,
        long revision,
        HLCTimestamp expires,
        HLCTimestamp lastUsed,
        HLCTimestamp lastModified,
        int state,
        bool noRevision
    )
    {
        Type = type;
        PartitionId = partitionId;
        Key = key;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
        NoRevision = noRevision;
        CompletionSource = null;
    }

    /// <summary>Drops references before the object goes back to the pool.</summary>
    internal void Clear()
    {
        Key = "";
        Value = null;
        CompletionSource = null;
    }
}
