
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Serialisable representation of a single committed WAL entry stored inside an incremental
/// backup segment file. The <see cref="Kommander.Data.RaftLogType"/> is not persisted because
/// every entry in a segment is committed by construction.
/// </summary>
internal sealed class WalSegmentEntry
{
    public long Id { get; set; }
    public long Term { get; set; }
    public int TimeNode { get; set; }
    public long TimePhysical { get; set; }
    public uint TimeCounter { get; set; }
    public string? LogType { get; set; }
    public byte[]? LogData { get; set; }

    public HLCTimestamp Time => new(TimeNode, TimePhysical, TimeCounter);

    public static WalSegmentEntry From(RaftLog log) => new()
    {
        Id = log.Id,
        Term = log.Term,
        TimeNode = log.Time.N,
        TimePhysical = log.Time.L,
        TimeCounter = log.Time.C,
        LogType = log.LogType,
        LogData = log.LogData
    };

    public RaftLog ToRaftLog() => new()
    {
        Id = Id,
        Term = Term,
        Time = Time,
        Type = RaftLogType.Committed,
        LogType = LogType,
        LogData = LogData
    };
}
