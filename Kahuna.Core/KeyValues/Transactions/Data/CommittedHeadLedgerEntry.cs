using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// One key's last transactionally committed head as the committed-head ledger remembers it: the revision and
/// state the key reached when that commit's settlement applied, and the commit timestamp the ledger's retention
/// horizon is measured against. The unit a range move hands from the source partition's slices to the
/// destination's (see <see cref="SeedCommittedHeadCommand"/>).
/// </summary>
internal readonly record struct CommittedHeadLedgerEntry(string Key, long Revision, KeyValueState State, HLCTimestamp CommittedAt);
