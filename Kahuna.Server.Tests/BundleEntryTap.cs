using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>The write executor of a node, which keeps every batch it was asked to replicate, in order.</summary>
internal sealed class BundleEntryTap(IPartitionBatchExecutor inner) : IPartitionBatchExecutor
{
    private readonly Lock gate = new();

    private readonly List<RaftProposalEntry[]> batches = [];

    public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> batch, CancellationToken cancellationToken)
    {
        lock (gate)
            batches.Add([.. batch]);

        return inner.ReplicateAsync(partitionId, batch, cancellationToken);
    }

    /// <summary>
    /// The entries around the prepare of a transaction in the batch that carried it: the entry before its record
    /// init, when the batch holds one, the record init, the prepare and the entry after it. Entries of another batch
    /// are not part of the answer, so a settlement that is named here was proposed together with the prepare.
    /// </summary>
    public string BundleOf(HLCTimestamp transactionId)
    {
        RaftProposalEntry[] batch;
        lock (gate)
            batch = batches.First(candidate => candidate.Any(entry => PreparesOf(entry).Any(intent => intent.TransactionId == transactionId)));

        int prepare = Array.FindIndex(batch, entry => PreparesOf(entry).Any(intent => intent.TransactionId == transactionId));
        int from = Math.Max(0, prepare - 2);
        int to = Math.Min(batch.Length - 1, prepare + 1);

        return string.Join(" | ", batch[from..(to + 1)].Select(entry => Describe(entry, transactionId)));
    }

    /// <summary>Whether a prepare of the transaction was proposed here.</summary>
    public bool Prepared(HLCTimestamp transactionId)
    {
        lock (gate)
            return batches.Any(batch => batch.Any(entry => PreparesOf(entry).Any(intent => intent.TransactionId == transactionId)));
    }

    /// <summary>How many proposed entries settle an intent of the transaction.</summary>
    public int SettlesOf(HLCTimestamp transactionId)
    {
        lock (gate)
            return batches.Sum(batch => batch.Count(entry => CommandsOf(entry).OfType<ResolveIntentCommand>().Any(resolve => resolve.TransactionId == transactionId)));
    }

    private static PreparedIntentCommand[] CommandsOf(RaftProposalEntry entry) =>
        entry.Type == ReplicationTypes.PreparedIntent ? PreparedIntentStore.DecodeDelta(entry.Data) : [];

    private static IEnumerable<PreparedIntent> PreparesOf(RaftProposalEntry entry) =>
        CommandsOf(entry).OfType<PrepareIntentCommand>().Select(static prepare => prepare.Intent);

    private static string Describe(RaftProposalEntry entry, HLCTimestamp transactionId)
    {
        if (entry.Type == ReplicationTypes.PreparedIntent)
        {
            PreparedIntentCommand[] commands = CommandsOf(entry);
            string[] prepared = [.. commands.OfType<PrepareIntentCommand>().Select(static prepare => ShortKey(prepare.Intent.Key))];
            string[] settled = [.. commands.OfType<ResolveIntentCommand>().Select(static resolve => ShortKey(resolve.Key))];

            return prepared.Length > 0 ? "prepare " + string.Join(",", prepared) : "settle " + string.Join(",", settled);
        }

        if (entry.Type != ReplicationTypes.TransactionRecord)
            return "other";

        TransactionRecordCommandMessage command = ReplicationSerializer.UnserializeTransactionRecordDeltaMessage(entry.Data).Commands[0];
        if (new HLCTimestamp(command.TransactionIdNode, command.TransactionIdPhysical, command.TransactionIdCounter) != transactionId)
            return "record of another transaction";

        return command.Kind switch
        {
            TransactionRecordCommandKindMessage.TransactionRecordInitialize => "init",
            TransactionRecordCommandKindMessage.TransactionRecordCommit => "commit",
            _ => command.Kind.ToString()
        };
    }

    private static string ShortKey(string key) => key[(key.LastIndexOf('/') + 1)..];
}
