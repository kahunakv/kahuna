
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides a static implementation of the 'abs' function for use in
/// mathematical operations within the key-value transaction system.
/// </summary>
internal static class AbsFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'abs' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            // long.MinValue has no positive counterpart, so its absolute value is an overflow.
            KeyValueExpressionType.LongType => new(ScriptNumeric.Abs(arg.LongValue, ast)),
            KeyValueExpressionType.DoubleType => new(Math.Abs(arg.DoubleValue)),
            _ => throw new KahunaScriptException($"Cannot use 'abs' function with argument {arg.Type}", ast.yyline)
        };
    }
}