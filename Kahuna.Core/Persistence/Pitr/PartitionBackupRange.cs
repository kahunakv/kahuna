
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// WAL coverage for a single partition within a backup artifact:
/// the contiguous range of committed log indices captured by this backup,
/// together with the HLC timestamps at both ends.
/// </summary>
internal sealed class PartitionBackupRange
{
    public int PartitionId { get; set; }

    public long FromIndex { get; set; }

    public int FromHlcNode { get; set; }
    public long FromHlcPhysical { get; set; }
    public uint FromHlcCounter { get; set; }

    public long ToIndex { get; set; }

    public int ToHlcNode { get; set; }
    public long ToHlcPhysical { get; set; }
    public uint ToHlcCounter { get; set; }

    public HLCTimestamp FromHlc => new(FromHlcNode, FromHlcPhysical, FromHlcCounter);
    public HLCTimestamp ToHlc => new(ToHlcNode, ToHlcPhysical, ToHlcCounter);

    public static PartitionBackupRange Create(
        int partitionId,
        long fromIndex, HLCTimestamp fromHlc,
        long toIndex, HLCTimestamp toHlc) => new()
    {
        PartitionId = partitionId,
        FromIndex = fromIndex,
        FromHlcNode = fromHlc.N,
        FromHlcPhysical = fromHlc.L,
        FromHlcCounter = fromHlc.C,
        ToIndex = toIndex,
        ToHlcNode = toHlc.N,
        ToHlcPhysical = toHlc.L,
        ToHlcCounter = toHlc.C
    };
}
