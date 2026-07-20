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
                    context.Action = KeyValueTransactionAction.Abort;
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
            }

            if (response.Type == KeyValueResponseType.Deleted)
            {
                context.RecordModifiedKey((response.Key ?? "", response.Durability));
                context.StageMutation(response.Key ?? "", null, response.Revision, HLCTimestamp.Zero); // tombstone
            }

            context.ModifiedResult = new()
            {
                Type = response.Type,
                Values = [
                    new()
                    {
                        Key = response.Key ?? "",
                        Revision = response.Revision,
                        LastModified = response.LastModified
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
