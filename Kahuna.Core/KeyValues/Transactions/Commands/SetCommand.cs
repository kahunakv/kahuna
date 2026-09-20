
using Kahuna.Shared.KeyValue;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Represents a command that performs a set operation within a key-value transactional context.
/// This command interacts with the key-value store to set or update data based on the provided parameters.
/// </summary>
internal sealed class SetCommand : BaseCommand
{
    public static async Task<KeyValueTransactionResult> Execute(
        KeyValuesManager manager, 
        ScriptTransactionContext context, 
        NodeAst ast, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
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

        // Serialized once and shared by the write, the staged mutation, and the modified result —
        // value arrays are never mutated downstream, and each ToBytes call allocates a fresh copy.
        byte[]? valueBytes = result.ToBytes();

        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await manager.LocateAndTrySetKeyValue(
            context.TransactionId,
            key: keyName,
            value: valueBytes,
            options.CompareValue,
            options.CompareRevision,
            options.Flags,
            options.ExpiresMs,
            durability,
            cancellationToken
        );

        switch (type)
        {
            case KeyValueResponseType.Set:
                context.RecordModifiedKey((keyName, durability));
                // Stage the value for the durable-intent path, carrying the relative TTL (0 = none). The freeze
                // resolves it to an absolute expiry of commitTimestamp + expiresMs, so a TTL set is durable-atomic
                // rather than falling back to the ticket path.
                context.StageMutation(keyName, valueBytes, KeyValueState.Set, revision, options.ExpiresMs, (options.Flags & KeyValueFlags.SetNoRevision) != 0);
                break;
            
            case KeyValueResponseType.Aborted or KeyValueResponseType.Errored or KeyValueResponseType.MustRetry:
                context.StopOnStatementFailure("SET", keyName, durability, type);
                break;
        }
        
        // Record the outcome of this statement on the context. Three things read it: the NOT SET guard, which
        // asks whether the statement that just ran succeeded; the prepare path, which raises the commit
        // timestamp to the highest LastModified seen; and the batched forms, which hand it back as the
        // script's result. It is not what the prepared intent is built from — staged values are accumulated
        // separately per key, which is why the batched set builds this without a value and still commits.
        // The client-facing result returned below is separate and unaffected.
        context.ModifiedResult = new()
        {
            Type = type,
            Values = [
                new()
                {
                    Key = keyName,
                    Value = valueBytes,
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