using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A copy of the intents a node holds and, per partition, the log position applied through the store before the
/// copy was taken (see <see cref="PreparedIntentStore.WalkLiveIntents"/>).
/// </summary>
internal readonly record struct LiveIntentWalk(IReadOnlyCollection<PreparedIntent> Intents, IReadOnlyDictionary<int, long> AppliedThrough);
