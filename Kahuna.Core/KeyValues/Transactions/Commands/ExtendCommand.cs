
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command that handles the ability to extend the expiry time of a key-value pair.
/// </summary>
internal sealed class ExtendCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager,
        ScriptTransactionContext context,
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
                context.RecordModifiedKey((keyName, durability));

                // Stage the extend for the durable-intent path so a transaction containing an extend stays on the
                // durable path instead of falling back to the ticket path. An extend changes only the expiry, so
                // the intent carries the key's current value and revision (read back within this transaction, so it
                // sees the extend's own MVCC snapshot) plus the new relative TTL, resolved to an absolute expiry at
                // freeze. If the value cannot be read back, staging is skipped and the transaction falls back.
                (KeyValueResponseType readType, ReadOnlyKeyValueEntry? entry) = await manager.LocateAndTryGetValue(
                    context.TransactionId, keyName, -1, HLCTimestamp.Zero, durability, cancellationToken);

                if (readType == KeyValueResponseType.Get && entry is not null)
                    context.StageMutation(keyName, entry.Value, entry.Revision, expiresMs);
                break;

            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.Action = KeyValueTransactionAction.Abort;
                context.Status = KeyValueExecutionStatus.Stop;
                break;
        }
        
        context.ModifiedResult = new()
        {
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Revision = revision,
                    LastModified = lastModified
                }
            ]
        };

        return new()
        {
            ServedFrom = "",
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Revision = revision,
                    LastModified = lastModified
                }
            ]
        };
    }
}