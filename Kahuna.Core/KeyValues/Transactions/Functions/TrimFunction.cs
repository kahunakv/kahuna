using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// A string without its leading and trailing whitespace. The characters between are untouched.
/// </summary>
internal static class TrimFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 1)
            throw new KahunaScriptException("Invalid number of arguments for 'trim' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "trim");

        ReadOnlySpan<char> trimmed = source.AsSpan().Trim();

        // A string with nothing to trim is returned as itself. That is the common case for a value read
        // out of the store, and it costs no allocation at all.
        if (trimmed.Length == source.Length)
            return new(source);

        return new(trimmed.ToString());
    }
}
