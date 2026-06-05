
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Commands;

/// <summary>
/// Serves as the base class for command implementations used in the execution of key-value transactions.
/// Provides shared functionality and utilities for commands.
/// </summary>
internal abstract class BaseCommand
{
    internal static string GetKeyName(KeyValueTransactionContext context, NodeAst ast)
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

    internal static void RecordReadKey(
        KeyValueTransactionContext context,
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
