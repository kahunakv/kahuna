
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class SetManyCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager,
        ScriptTransactionContext context,
        NodeAst ast,        
        CancellationToken cancellationToken
    )
    {
        List<KahunaSetKeyValueRequestItem> arguments = [];

        GetSetCalls(context, ast, arguments);

        if (arguments.Count == 0)
        {
            return new()
            {
                Type = KeyValueResponseType.Set
            };
        }
        
        List<KahunaSetKeyValueResponseItem> responses = await manager.LocateAndTrySetManyKeyValue(arguments, cancellationToken);

        Dictionary<string, KahunaSetKeyValueRequestItem> argumentsByKey = new(arguments.Count);
        foreach (KahunaSetKeyValueRequestItem argument in arguments)
            if (argument.Key is not null)
                argumentsByKey[argument.Key] = argument;

        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            switch (response.Type)
            {                                
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                    context.StopOnStatementFailure("SET", response.Key ?? "", response.Durability, response.Type);
                    break;
            }
            
            context.RecordModifiedKey((response.Key ?? "", response.Durability));

            // Stage the value for the durable-intent path, carrying the item's relative TTL (0 = none), mirroring
            // the single set. The freeze resolves it to an absolute expiry of commitTimestamp + expiresMs.
            if (response.Type == KeyValueResponseType.Set && response.Key is not null
                && argumentsByKey.TryGetValue(response.Key, out KahunaSetKeyValueRequestItem? argument))
                context.StageMutation(response.Key, argument.Value, KeyValueState.Set, response.Revision, argument.ExpiresMs, (argument.Flags & KeyValueFlags.SetNoRevision) != 0, response.LastModified);
        }
        
        // The last response is the one that survives: the prepare path raises the commit timestamp from
        // the recorded result, and this command hands it back as the statement's result. Building a
        // result per response inside the loop made garbage of every response but the last.
        if (responses.Count > 0)
        {
            KahunaSetKeyValueResponseItem last = responses[^1];

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
            return new()
            {
                Type = KeyValueResponseType.Set
            };

        return context.ModifiedResult;
    }
    
    private static void GetSetCalls(ScriptTransactionContext context, NodeAst ast, List<KahunaSetKeyValueRequestItem> arguments)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.StmtList:
                {
                    if (ast.leftAst is not null)
                        GetSetCalls(context, ast.leftAst, arguments);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst!;
                        continue;
                    }

                    break;
                }
                
                case NodeType.Set:
                    arguments.Add(GetSetCall(context, ast, KeyValueDurability.Persistent));
                    break;
                
                case NodeType.Eset:
                    arguments.Add(GetSetCall(context, ast, KeyValueDurability.Ephemeral));
                    break;
                
                default:
                    throw new KahunaScriptException($"Invalid SET command {ast.nodeType}", ast.yyline);
            }

            break;
        }
    }

    private static KahunaSetKeyValueRequestItem GetSetCall(ScriptTransactionContext context, NodeAst ast, KeyValueDurability durability)
    {
        if (ast.leftAst?.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);

        if (ast.rightAst is null)
            throw new KahunaScriptException("Invalid value", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);

        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }

        SetOptions options = new() { Flags = KeyValueFlags.Set };

        if (ast.extendedOne is not null)
            ReadSetOptions(context, ast.extendedOne, ref options);

        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        return new()
        {
            TransactionId = context.TransactionId,
            Key = keyName,
            Value = result.ToBytes(),
            CompareValue = options.CompareValue,
            CompareRevision = options.CompareRevision,
            Flags = options.Flags,
            ExpiresMs = options.ExpiresMs,
            Durability = durability            
        };
    }
}