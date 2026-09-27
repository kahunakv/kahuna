using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Routes a materializing resolve's install to the apply path that owns it: the replicator for an entry this node
/// receives live (which also routes the committed head to the owning actor), the restorer for a restart replay
/// (which rebuilds durable state only, exactly as it does for a replayed key/value record).
/// </summary>
internal sealed class ResolvedIntentInstaller : IResolvedIntentInstaller
{
    private readonly KeyValueReplicator replicator;

    private readonly KeyValueRestorer restorer;

    public ResolvedIntentInstaller(KeyValueReplicator replicator, KeyValueRestorer restorer)
    {
        this.replicator = replicator;
        this.restorer = restorer;
    }

    public void Install(int partitionId, long logIndex, PreparedIntent intent, bool replay)
    {
        if (replay)
            restorer.RestoreResolvedIntent(partitionId, logIndex, intent);
        else
            replicator.InstallResolvedIntent(partitionId, logIndex, intent);
    }

    public void CompleteEntry(int partitionId, long logIndex, bool replay)
    {
        if (replay)
            restorer.CompleteResolvedIntentEntry(partitionId, logIndex);
        else
            replicator.CompleteResolvedIntentEntry(partitionId, logIndex);
    }
}
