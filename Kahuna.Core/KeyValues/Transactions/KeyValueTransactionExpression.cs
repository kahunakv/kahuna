
using System.Globalization;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Transactions.Functions;
using Kahuna.Server.KeyValues.Transactions.Operators;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Contains static methods to evaluate key-value transaction expressions
/// within a specified context and abstract syntax tree (AST).
/// Provides functionality to traverse and compute results for transaction AST nodes.
/// </summary>
internal static class KeyValueTransactionExpression
{
    /// <summary>
    /// Evaluates the specified abstract syntax tree (AST) within the context of a key-value transaction,
    /// producing a result based on the AST evaluation logic.
    /// </summary>
    /// <param name="context">The context of the key-value transaction which contains necessary state and configuration.</param>
    /// <param name="ast">The abstract syntax tree to be evaluated, representing the input expression or operation.</param>
    /// <returns>A result encapsulated in a <see cref="KeyValueExpressionResult"/> object, containing the evaluation outcome.</returns>
    public static KeyValueExpressionResult Eval(ScriptTransactionContext context, NodeAst ast)
    {
        switch (ast.nodeType)
        {
            case NodeType.Identifier:
                return context.GetVariable(ast, ast.yytext!);
            
            case NodeType.IntegerType:
            case NodeType.StringType:
            case NodeType.FloatType:
                return Literal(ast);
            
            case NodeType.BooleanType:
                return KeyValueExpressionResult.FromBool(ast.yytext! == "true");
            
            case NodeType.Placeholder:
                return new(context.GetParameter(ast));
            
            case NodeType.NullType:
                return KeyValueExpressionResult.Null;
            
            case NodeType.Equals:
                return EqualsOperator.Eval(context, ast, "==");
            
            case NodeType.GreaterThan:
                return GreaterThanOperator.Eval(context, ast, ">");
            
            case NodeType.LessThan:
                return LessThanOperator.Eval(context, ast, "<");
            
            case NodeType.LessThanEquals:
            {
                KeyValueExpressionResult result = GreaterThanOperator.Eval(context, ast, "<=");
                return KeyValueExpressionResult.FromBool(!result.BoolValue);
            }
            
            case NodeType.GreaterThanEquals:
            {
                KeyValueExpressionResult result = LessThanOperator.Eval(context, ast, ">=");
                return KeyValueExpressionResult.FromBool(!result.BoolValue);
            }
            
            case NodeType.NotEquals:
            {
                KeyValueExpressionResult result = EqualsOperator.Eval(context, ast, "!=");
                return KeyValueExpressionResult.FromBool(!result.BoolValue);
            }
            
            case NodeType.Add:
                return AddOperator.Eval(context, ast);
            
            case NodeType.Subtract:
                return SubOperator.Eval(context, ast);
            
            case NodeType.Mult:
                return MultOperator.Eval(context, ast);
            
            case NodeType.Div:
                return DivOperator.Eval(context, ast);
            
            case NodeType.ArrayIndex:
                return ArrayIndexOperator.Eval(context, ast);
            
            case NodeType.Range:
                return RangeOperator.Eval(context, ast);
            
            case NodeType.FuncCall:
                return CallFunction.Eval(context, ast);
            
            case NodeType.And:
                return AndOperator.Eval(context, ast);
            
            case NodeType.Or:
                return OrOperator.Eval(context, ast);
            
            case NodeType.Not:
                return NotOperator.Eval(context, ast);

            case NodeType.Negate:
                return NegateOperator.Eval(context, ast);
            
            case NodeType.NotSet:
                return NotSetOperator.Eval(context, ast);
            
            case NodeType.NotFound:
                return NotFoundOperator.Eval(context, ast);
            
            case NodeType.StmtList:
            case NodeType.Set:
            case NodeType.Get:
            case NodeType.Eset:
            case NodeType.Eget:
            case NodeType.If:
            case NodeType.For:
            case NodeType.SetNotExists:
            case NodeType.SetExists:
            case NodeType.SetCmp:
            case NodeType.SetCmpRev:
            case NodeType.Begin:
            case NodeType.Rollback:
            case NodeType.Commit:
            case NodeType.Return:
            case NodeType.ArgumentList:
            case NodeType.Delete:
            case NodeType.Edelete:
            case NodeType.Exists:
            case NodeType.Eexists:
                break;

            case NodeType.Let:
            case NodeType.Extend:
            case NodeType.Eextend:
            case NodeType.BeginOptionList:
            case NodeType.BeginOption:
            case NodeType.Sleep:
            case NodeType.Throw:
            case NodeType.GetByBucket:
            case NodeType.EGetByBucket:
            case NodeType.ScanByPrefix:
            case NodeType.EscanByPrefix:
            default:
                throw new NotImplementedException();
        }

        return KeyValueExpressionResult.Null;
    }

    /// <summary>
    /// The value of a literal node, read once and kept on the node for every later execution.
    ///
    /// <para>A parsed tree is shared by every execution of the same script and a literal's text never
    /// changes, so the read and the wrapper are both fixed work. Keeping them removes one parse and one
    /// object per literal per evaluation, which inside a loop is per literal per iteration.</para>
    ///
    /// <para>Read on first use, not at parse time: a literal in a branch that is not taken is never
    /// evaluated, and reading it early would report an out-of-range literal in dead code as a script
    /// error. Two executions of the same tree may both fill the field; both derive the same value from
    /// the same text, so the loser's work is wasted and nothing else.</para>
    /// </summary>
    private static KeyValueExpressionResult Literal(NodeAst ast)
    {
        KeyValueExpressionResult? memo = Volatile.Read(ref ast.literalMemo);

        if (memo is not null)
            return memo;

        KeyValueExpressionResult value = ast.nodeType switch
        {
            NodeType.IntegerType => new(ParseIntegerLiteral(ast)),
            NodeType.FloatType   => new(ParseFloatLiteral(ast)),
            NodeType.StringType  => new(ast.yytext!),
            _ => throw new KahunaScriptException("Not a literal: " + ast.nodeType, ast.yyline)
        };

        Volatile.Write(ref ast.literalMemo, value);

        return value;
    }

    /// <summary>
    /// The value of a node read as a revision number, kept on the node the same way a literal is.
    ///
    /// <para>A revision option is a plain decimal integer, which is a narrower rule than the one a
    /// literal expression follows, so it has its own memo. Reading it here keeps the parse out of the
    /// statement path, where it ran on every execution of the statement.</para>
    /// </summary>
    internal static long RevisionOption(NodeAst ast)
    {
        KeyValueExpressionResult? memo = Volatile.Read(ref ast.revisionMemo);

        if (memo is not null)
            return memo.LongValue;

        KeyValueExpressionResult value = new((long)int.Parse(ast.yytext!, CultureInfo.InvariantCulture));

        Volatile.Write(ref ast.revisionMemo, value);

        return value.LongValue;
    }

    /// <summary>
    /// Converts an integer literal to its value. The scanner accepts both a decimal run of digits and a
    /// "0x" prefixed hexadecimal one, so both are honored here; a plain parse would reject the hexadecimal
    /// form the scanner already promised. The scanner never folds a sign into the literal, so the text is
    /// always unsigned and a leading minus arrives as a separate negate node.
    /// </summary>
    private static long ParseIntegerLiteral(NodeAst ast)
    {
        ReadOnlySpan<char> text = ast.yytext.AsSpan();

        if (text.Length > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X'))
        {
            // A hexadecimal literal is a bit pattern, so a value with the top bit set reads back negative,
            // exactly as the same literal does in C#.
            if (long.TryParse(text[2..], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out long hexValue))
                return hexValue;

            throw new KahunaScriptException("Invalid hexadecimal integer: " + ast.yytext, ast.yyline);
        }

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long value))
            return value;

        throw new KahunaScriptException("Integer out of range: " + ast.yytext, ast.yyline);
    }

    /// <summary>
    /// Converts a floating point literal to its value. A literal too large for a double is a script error
    /// rather than a framework exception, so the caller still sees the line it came from.
    /// </summary>
    private static double ParseFloatLiteral(NodeAst ast)
    {
        if (double.TryParse(ast.yytext, NumberStyles.Float, CultureInfo.InvariantCulture, out double value))
            return value;

        throw new KahunaScriptException("Float out of range: " + ast.yytext, ast.yyline);
    }
}
