
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
        KeyValueTransactionContext context,
        NodeAst ast,        
        CancellationToken cancellationToken
    )
    {
        List<KahunaSetKeyValueRequest> arguments = [];

        GetSetCalls(context, ast, arguments);       
        
        List<Task<(KeyValueResponseType type, long revision, HLCTimestamp lastModified)>> tasks = new(arguments.Count);
        
        foreach (KahunaSetKeyValueRequest argument in arguments)
        {
            tasks.Add(manager.LocateAndTrySetKeyValue(
                context.TransactionId,
                argument.Key ?? "",
                value: argument.Value,
                argument.CompareValue,
                argument.CompareRevision,
                argument.Flags,
                argument.ExpiresMs,
                argument.Durability,
                cancellationToken
            ));
        }
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified)[] responses = await Task.WhenAll(tasks);
        
        //= await 

        foreach ((KeyValueResponseType type, long revision, HLCTimestamp lastModified) response in responses)
        {
            switch (response.type)
            {                                
                case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                    context.Action = KeyValueTransactionAction.Abort;
                    context.Status = KeyValueExecutionStatus.Stop;
                    break;
            }
            
            context.ModifiedResult = new()
            {
                Type = response.type,
                Values = [
                    new()
                    {
                        Key = arguments[0].Key ?? "",
                        Revision = response.revision,
                        LastModified = response.lastModified
                    }
                ]
            };
        }

        foreach (KahunaSetKeyValueRequest argument in arguments)
        {
            context.ModifiedKeys ??= [];
            context.ModifiedKeys.Add((argument.Key ?? "", argument.Durability));
        }

        return context.ModifiedResult!;
    }
    
    private static void GetSetCalls(KeyValueTransactionContext context, NodeAst ast, List<KahunaSetKeyValueRequest> arguments)
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

    private static KahunaSetKeyValueRequest GetSetCall(KeyValueTransactionContext context, NodeAst ast, KeyValueDurability durability)
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

        int expiresMs = 0;
        
        if (ast.extendedOne is not null)
            expiresMs = int.Parse(ast.extendedOne.yytext!);

        KeyValueFlags flags = KeyValueFlags.Set;

        if (ast.extendedTwo is not null)
        {
            flags = ast.extendedTwo.nodeType switch
            {
                NodeType.SetNotExists => KeyValueFlags.SetIfNotExists,
                NodeType.SetExists => KeyValueFlags.SetIfExists,
                _ => flags
            };
        }
        
        long compareRevision = 0;
        byte[]? compareValue = null;

        if (ast.extendedThree is not null)
        {
            if (ast.extendedThree.leftAst is null)
                throw new KahunaScriptException("Invalid SET cmp/cmprev", ast.yyline);

            switch (ast.extendedThree.nodeType)
            {
                case NodeType.SetCmp:
                    flags = KeyValueFlags.SetIfEqualToValue;
                    compareValue = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToBytes();
                    break;
                
                case NodeType.SetCmpRev:
                    flags = KeyValueFlags.SetIfEqualToRevision;
                    compareRevision = KeyValueTransactionExpression.Eval(context, ast.extendedThree.leftAst).ToLong();
                    break;
            }
        }

        KeyValueExpressionResult result = KeyValueTransactionExpression.Eval(context, ast.rightAst);
        
        return new()
        {
            TransactionId = context.TransactionId,
            Key = keyName,
            Value = result.ToBytes(),
            CompareValue = compareValue,
            CompareRevision = compareRevision,
            Flags = flags,
            ExpiresMs = expiresMs,
            Durability = durability            
        };
    }
}