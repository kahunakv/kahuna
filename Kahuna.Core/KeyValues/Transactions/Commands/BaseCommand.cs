
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

    /// <summary>
    /// The options a <c>SET</c> statement carries: its TTL, its comparison operands, and the flag bits
    /// the store reads. Collected in one value so the flag walk can fill them without a closure.
    /// </summary>
    internal struct SetOptions
    {
        internal int ExpiresMs;

        internal long CompareRevision;

        internal byte[]? CompareValue;

        internal KeyValueFlags Flags;
    }

    /// <summary>
    /// Reads a <c>SET</c> statement's flag list into <paramref name="options"/>.
    ///
    /// <para>The flags are walked straight from the syntax tree. Collecting them into a list of flag
    /// objects first rebuilt constant parse output on every execution of the statement — one list, its
    /// backing array, and one object per flag — and a <c>SET</c> inside a loop paid that per iteration.
    /// The tree it reads is already the same tree on every execution.</para>
    ///
    /// <para><paramref name="options"/> travels by reference because the walk recurses into the left
    /// branch of the flag list, and a captured local would put a closure back on the path this exists to
    /// clear.</para>
    /// </summary>
    internal static void ReadSetOptions(ScriptTransactionContext context, NodeAst ast, ref SetOptions options)
    {
        while (true)
        {
            switch (ast.nodeType)
            {
                case NodeType.SetFlagsList:
                {
                    if (ast.leftAst is not null)
                        ReadSetOptions(context, ast.leftAst, ref options);

                    if (ast.rightAst is not null)
                    {
                        ast = ast.rightAst;
                        continue;
                    }

                    break;
                }

                case NodeType.SetEx:
                {
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET EX expression", ast.yyline);

                    KeyValueExpressionResult ex = KeyValueTransactionExpression.Eval(context, ast.leftAst);

                    if (ex.Type != KeyValueExpressionType.LongType)
                        throw new KahunaScriptException("Invalid SET EX expression", ast.yyline);

                    options.ExpiresMs = (int)ex.LongValue;
                    break;
                }

                case NodeType.SetExists:
                    options.Flags |= KeyValueFlags.SetIfExists;
                    break;

                case NodeType.SetNotExists:
                    options.Flags |= KeyValueFlags.SetIfNotExists;
                    break;

                case NodeType.SetCmp:
                {
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET CMP expression", ast.yyline);

                    options.Flags |= KeyValueFlags.SetIfEqualToValue;
                    options.CompareValue = KeyValueTransactionExpression.Eval(context, ast.leftAst).ToBytes();
                    break;
                }

                case NodeType.SetCmpRev:
                {
                    if (ast.leftAst is null)
                        throw new KahunaScriptException("Invalid SET CMPREV expression", ast.yyline);

                    options.Flags |= KeyValueFlags.SetIfEqualToRevision;
                    options.CompareRevision = KeyValueTransactionExpression.Eval(context, ast.leftAst).ToLong();
                    break;
                }

                case NodeType.SetNoRev:
                    options.Flags |= KeyValueFlags.SetNoRevision;
                    break;

                default:
                    throw new NotImplementedException();
            }

            break;
        }
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
