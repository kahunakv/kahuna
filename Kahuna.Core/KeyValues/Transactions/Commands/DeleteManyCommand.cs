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

        foreach (KahunaDeleteKeyValueResponseItem response in responses)
        {
            switch (response.Type)
            {
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry or KeyValueResponseType.InvalidInput:
                    context.StopOnStatementFailure("DELETE", response.Key ?? "", response.Durability, response.Type);
                    break;
            }

            if (response.Type == KeyValueResponseType.Deleted)
            {
                context.RecordModifiedKey((response.Key ?? "", response.Durability));
                context.StageMutation(response.Key ?? "", null, KeyValueState.Deleted, response.Revision, 0, noRevision: false); // deletes have no TTL and retain history
            }

        }

        // The last response is the one that survives: the prepare path raises the commit timestamp from
        // the recorded result, and this command hands it back as the statement's result. Building a
        // result per response inside the loop made garbage of every response but the last.
        if (responses.Count > 0)
        {
            KahunaDeleteKeyValueResponseItem last = responses[^1];

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
