
using Kommander.Time;
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Serves as the base class for command implementations used in the execution of key-value transactions.
/// Provides shared functionality and utilities for commands.
/// </summary>
internal abstract class BaseCommand
{
    internal static string GetKeyName(ScriptTransactionContext context, NodeAst ast)
    {
        if (string.IsNullOrEmpty(ast.yytext))
            throw new KahunaScriptException($"Invalid key name type {ast.nodeType}", ast.yyline);
        
        return ast.nodeType switch
        {
            NodeType.Identifier => ast.yytext,
            NodeType.StringType => ast.yytext,
            NodeType.Placeholder => context.GetParameter(ast),
            _ => throw new KahunaScriptException($"Invalid key name type {ast.nodeType}", ast.yyline)
        };
    }

    /// <summary>
    /// Resolves the effective snapshot timestamp for a read statement.
    /// Per-statement AS OF (ast.extendedTwo) overrides the transaction-level context.ReadTimestamp.
    /// AS OF 0 is rejected. When neither is present, returns Zero (latest).
    /// </summary>
    internal static HLCTimestamp ResolveReadTimestamp(ScriptTransactionContext context, NodeAst ast)
    {
        if (ast.extendedTwo is null)
            return context.ReadTimestamp;

        long ms = KeyValueTransactionExpression.Eval(context, ast.extendedTwo).ToLong();
        if (ms == 0)
            throw new KahunaScriptException("AS OF 0 is not a valid snapshot timestamp", ast.extendedTwo.yyline);

        return new HLCTimestamp(0, ms, uint.MaxValue);
    }

    internal static void RecordReadKey(
        ScriptTransactionContext context,
        string key,
        KeyValueDurability durability,
        bool exists,
        long revision
    )
    {
        if (context.Locking != KeyValueTransactionLocking.Optimistic)
            return;

        context.ReadKeys ??= [];
        context.ReadKeys[(key, durability)] = new()
        {
            Key = key,
            Durability = durability,
            Exists = exists,
            Revision = exists ? revision : -1
        };
    }
}
