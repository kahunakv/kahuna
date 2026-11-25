
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

public sealed class ReadOnlyKeyValueEntry
{
    public byte[]? Value { get; }
    
    public long Revision { get; }
    
    public HLCTimestamp Expires { get; }
    
    public HLCTimestamp LastUsed { get; }
    
    public HLCTimestamp LastModified { get; }
    
    public KeyValueState State { get; }

    /// <summary>
    /// The process clock when this revision became the committed head in the answering process, or zero when
    /// unknown. Set only by a transactional latest read answered in this process; the time does not cross a wire.
    /// </summary>
    public long HeadSeenTicks { get; init; }

    /// <summary>The <see cref="HeadSeenTicks"/> of a read answered from the reading transaction's own staged write.</summary>
    public const long OwnStagedWrite = -1;

    /// <summary>
    /// Whether a latest read was answered from the reading transaction's own staged write of the key: the value of a
    /// staged set or extend, or the absence that a staged delete or a staged expiry leaves. The committed head is
    /// still the one below that write, so the answer cannot be compared with it. Unlike the time, this crosses the
    /// wire of a forwarded read.
    /// </summary>
    public bool IsOwnStagedWrite => HeadSeenTicks == OwnStagedWrite;

    /// <summary>The process clock when this read was answered, or zero when unknown. See <see cref="HeadSeenTicks"/>.</summary>
    public long ServedTicks { get; init; }
    
    public ReadOnlyKeyValueEntry(byte[]? value, long revision, HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified, KeyValueState state)
    {
        Value = value;
        Revision = revision;
        Expires = expires;
        LastUsed = lastUsed;
        LastModified = lastModified;
        State = state;
    }
}