
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

        // Keyed by key and durability: a batch can hold a SET and an ESET of the same key name, and each
        // response must stage the value of its own statement.
        Dictionary<(string, KeyValueDurability), KahunaSetKeyValueRequestItem> argumentsByKey = new(arguments.Count);
        foreach (KahunaSetKeyValueRequestItem argument in arguments)
            if (argument.Key is not null)
                argumentsByKey[(argument.Key, argument.Durability)] = argument;

        // The fan-out answers in completion order, not statement order. The batch stands for its statements run
        // one at a time, so its result is the response of its last statement, found by key and durability.
        KahunaSetKeyValueRequestItem lastArgument = arguments[^1];
        KahunaSetKeyValueResponseItem? last = null;

        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            switch (response.Type)
            {
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                    context.StopOnStatementFailure("SET", response.Key ?? "", response.Durability, response.Type);
                    break;
            }

            if (response.Durability == lastArgument.Durability && string.Equals(response.Key, lastArgument.Key, StringComparison.Ordinal))
                last = response;

            // Only a confirmed write joins the working set, as with the single set. A conditional set whose
            // condition failed (NotSet) wrote nothing: recording it would leave a modified key with no staged
            // value, which the commit refuses.
            if (response.Type != KeyValueResponseType.Set || response.Key is null)
                continue;

            context.RecordModifiedKey((response.Key, response.Durability));
            context.RaiseHighestWriteTime(response.LastModified);

            // Stage the value for the durable-intent path, carrying the item's relative TTL (0 = none), mirroring
            // the single set. The freeze resolves it to an absolute expiry of commitTimestamp + expiresMs.
            if (argumentsByKey.TryGetValue((response.Key, response.Durability), out KahunaSetKeyValueRequestItem? argument))
                context.StageMutation(response.Key, argument.Value, KeyValueState.Set, response.Revision, argument.ExpiresMs, (argument.Flags & KeyValueFlags.SetNoRevision) != 0, response.LastModified);
        }

        // A short-circuited answer (an invalid key, no leader) may not name the last statement's key; it then
        // stands for the whole batch.
        if (last is null && responses.Count > 0)
            last = responses[^1];

        // Building a result per response inside the loop made garbage of every response but the last.
        if (last is not null)
        {
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