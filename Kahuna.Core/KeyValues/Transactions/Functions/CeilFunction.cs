
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides functionality to execute the 'ceil' function in scripts.
/// </summary>
/// <remarks>
/// The 'ceil' function returns the smallest integral value that is greater than or equal to the specified number.
/// It supports arguments of type <see cref="KeyValueExpressionType.LongType"/> and <see cref="KeyValueExpressionType.DoubleType"/>.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown when:
/// - The number of arguments is not exactly one.
/// - The argument type is not supported.
/// </exception>
internal static class CeilFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'ceil' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.LongType => new(Math.Ceiling((double)arg.LongValue)),
            KeyValueExpressionType.DoubleType => new(Math.Ceiling(arg.DoubleValue)),
            _ => throw new KahunaScriptException($"Cannot use 'ceil' function with argument {arg.Type}", ast.yyline)
        };
    }
}