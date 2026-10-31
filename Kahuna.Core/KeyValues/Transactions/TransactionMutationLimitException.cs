namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Thrown when a script transaction writes more keys, or holds more locks, than its mutation limit allows. The
/// transaction does not commit: the executor releases everything it holds and answers
/// <see cref="Kahuna.Shared.KeyValue.KeyValueResponseType.TransactionMutationLimitReached"/>.
/// </summary>
internal sealed class TransactionMutationLimitException(int limit)
    : Exception($"The transaction passed its limit of {limit} written keys or held locks; split the work into smaller transactions");
