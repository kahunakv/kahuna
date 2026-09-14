using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Whether a string begins with a given prefix. The comparison is ordinal: it compares characters,
/// never a locale's idea of how they sort. Keys, identifiers and packed field values are not
/// language, and a culture-aware comparison would make the same script answer differently on two
/// nodes with two locales.
/// </summary>
internal static class StartsWithFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'starts_with' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "starts_with");
        string prefix = ScriptStringArgument.Require(ast, arguments[1], "starts_with");

        return new(source.AsSpan().StartsWith(prefix.AsSpan(), StringComparison.Ordinal));
    }
}
