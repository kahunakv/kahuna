
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

internal sealed class ExtendCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager,
        KeyValueTransactionContext context,
        NodeAst ast,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (ast.leftAst is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        if (ast.leftAst.yytext is null)
            throw new KahunaScriptException("Invalid key", ast.yyline);
        
        string keyName = GetKeyName(context, ast.leftAst);
        
        if (context.Locking == KeyValueTransactionLocking.Optimistic)
        {
            context.LocksAcquired ??= [];
            context.LocksAcquired.Add((keyName, durability));
        }
        
        int expiresMs = 0;
        
        if (ast.rightAst is not null)
            expiresMs = int.Parse(ast.rightAst.yytext!);
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await manager.LocateAndTryExtendKeyValue(
            context.TransactionId,
            key: keyName,
            expiresMs: expiresMs,
            durability,
            cancellationToken
        );
        
        switch (type)
        {
            case KeyValueResponseType.Extended:
                context.ModifiedKeys ??= [];
                context.ModifiedKeys.Add((keyName, durability));
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
                break;
        }
        
        context.Result = new()
        {
            Type = type,
            Revision = revision,
            LastModified = lastModified,
        };
        
        context.ModifiedResult = new()
        {
            Type = type,
            Revision = revision,
            LastModified = lastModified
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Revision = revision
        };
    }
}