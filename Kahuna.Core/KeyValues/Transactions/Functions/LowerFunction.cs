
using Kahuna.Server.ScriptParser;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Provides the implementation for the 'lower' function that converts a string argument
/// to its lowercase equivalent.
/// </summary>
/// <remarks>
/// The 'lower' function is executed by taking a single string argument and transforming
/// its value to lowercase using culture-invariant rules. If the argument is not of
/// string type or if the number of arguments is invalid, an exception is thrown.
/// </remarks>
/// <exception cref="KahunaScriptException">
/// Thrown when the number of arguments is not equal to one or when the argument
/// is not of string type.
/// </exception>
/// <param name="ast">The abstract syntax tree node related to this function call.</param>
/// <param name="arguments">A list containing the arguments passed to the 'lower' function.</param>
/// <returns>
/// A <see cref="KeyValueExpressionResult"/> representing the transformed string value in lowercase.
/// </returns>
internal static class LowerFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'lower' function", ast.yyline);

        KeyValueExpressionResult arg = arguments[0];

        return arg.Type switch
        {
            KeyValueExpressionType.StringType => new(arg.StrValue?.ToUpperInvariant()),
            _ => throw new KahunaScriptException($"Cannot use 'lower' function on argument {arg.Type}", ast.yyline)
        };
    }
}