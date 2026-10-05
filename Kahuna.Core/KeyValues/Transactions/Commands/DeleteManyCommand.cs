using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class DeleteManyCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager,
        ScriptTransactionContext context,
        NodeAst ast,
        CancellationToken cancellationToken
    )
    {
        List<KahunaDeleteKeyValueRequestItem> arguments = [];

        GetDeleteCalls(context, ast, arguments);

        if (arguments.Count == 0)
        {
            return new()
            {
                Type = KeyValueResponseType.Deleted
            };
        }

        List<KahunaDeleteKeyValueResponseItem> responses = await manager.LocateAndTryDeleteManyKeyValue(arguments, cancellationToken);

        // The fan-out answers in completion order, not statement order. The batch stands for its statements run
        // one at a time, so its result is the response of its last statement, found by key and durability.
        KahunaDeleteKeyValueRequestItem lastArgument = arguments[^1];
        KahunaDeleteKeyValueResponseItem? last = null;

        foreach (KahunaDeleteKeyValueResponseItem response in responses)
        {
            switch (response.Type)
            {
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry or KeyValueResponseType.InvalidInput:
                    context.StopOnStatementFailure("DELETE", response.Key ?? "", response.Durability, response.Type);
                    break;
            }

            if (response.Durability == lastArgument.Durability && string.Equals(response.Key, lastArgument.Key, StringComparison.Ordinal))
                last = response;

            if (response.Type == KeyValueResponseType.Deleted)
            {
                context.RecordModifiedKey((response.Key ?? "", response.Durability));
                context.RaiseHighestWriteTime(response.LastModified);
                context.StageMutation(response.Key ?? "", null, KeyValueState.Deleted, response.Revision, 0, noRevision: false, response.LastModified); // deletes have no TTL and retain history
            }
        }

        // A short-circuited answer (an invalid key, no leader) may not name the last statement's key; it then
        // stands for the whole batch.
        if (last is null && responses.Count > 0)
            last = responses[^1];

        // Building a result per response inside the loop made garbage of every response but the last.
        if (last is not null)
        {
            context.LastDeleteType = last.Type;

            context.ModifiedResult = new()
            {
                Type = last.Type,
                Values = [
                    new()
                    {
                        Key = last.Key ?? "",
                        Revision = last.Revision,
                        LastModified = last.LastModified
                    }
                ]
            };
        }

        if (context.ModifiedResult is null)
        {
            return new()
            {
                Type = KeyValueResponseType.Deleted
            };
        }

        return context.ModifiedResult;
    }

    private static void GetDeleteCalls(ScriptTransactionContext context, NodeAst ast, List<KahunaDeleteKeyValueRequestItem> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                        GetDeleteCalls(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }

                case NodeType.Delete:
                    arguments.Add(GetDeleteCall(context, ast, KeyValueDurability.Persistent));
                    break;

                case NodeType.Edelete:
                    arguments.Add(GetDeleteCall(context, ast, KeyValueDurability.Ephemeral));
                    break;

                default:
                    throw new KahunaScriptException($"Invalid DELETE command {ast.nodeType}", ast.yyline);
            }

            break;
        }
    }

    private static KahunaDeleteKeyValueRequestItem GetDeleteCall(
        ScriptTransactionContext context,
        NodeAst ast,
        KeyValueDurability durability
    )
    {
        if (ast.leftAst?.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);

        string keyName = GetKeyName(context, ast.leftAst);

        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }

        return new()
        {
            TransactionId = context.TransactionId,
            Key = keyName,
            Durability = durability
        };
    }
}
