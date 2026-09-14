using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Whether a string ends with a given suffix. Ordinal, for the reason stated on
/// <see cref="StartsWithFunction"/>.
/// </summary>
internal static class EndsWithFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'ends_with' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "ends_with");
        string suffix = ScriptStringArgument.Require(ast, arguments[1], "ends_with");

        return new(source.AsSpan().EndsWith(suffix.AsSpan(), StringComparison.Ordinal));
    }
}
