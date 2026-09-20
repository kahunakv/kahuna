
namespace Kahuna.Server.KeyValues.Data;

/// <summary>
/// One node's apply state for one partition, as the key-value subsystem sees it: the highest kv log
/// id it applied, the number of committed heads its ledger slice holds for the partition, and the
/// number of live prepared intents it holds. Two replicas at the same applied log id must hold the
/// same committed heads — the ledger is a pure function of the log — so a difference at equal ids
/// is a divergence of one replica's apply stream, which is otherwise silent in operation and visible
/// only in hindsight.
/// </summary>
/// <param name="AppliedLogId">Highest kv log id applied on the node for the partition; 0 when nothing applied yet.</param>
/// <param name="CommittedHeads">Keys held by the node's committed-head ledger slice for the partition.</param>
/// <param name="LiveIntents">Live prepared intents the node holds for the partition.</param>
public readonly record struct KeyValueApplyFingerprint(long AppliedLogId, long CommittedHeads, long LiveIntents);
