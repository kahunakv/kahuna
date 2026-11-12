
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
    /// unknown. Set only by a transactional latest read answered in this process; it does not cross a wire.
    /// </summary>
    public long HeadSeenTicks { get; init; }

    /// <summary>The <see cref="HeadSeenTicks"/> of a read answered from the reading transaction's own staged write.</summary>
    public const long OwnStagedWrite = -1;

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