using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Cuts a string into the parts between its separators, and returns them as an array.
///
/// <para>The result is the array type the language already has, the one <c>GET BY BUCKET</c> and the
/// range operator produce, so <c>count()</c>, subscripting and <c>FOR … IN</c> accept it with no
/// further change.</para>
///
/// <para>Every cut is ordinal, for the reason stated on <see cref="StartsWithFunction"/>. Separators
/// are consumed from left to right and never overlap, so two adjacent separators produce an empty
/// part between them, and a leading or trailing separator produces an empty part at that end. Those
/// empty parts are kept: dropping them would make the position of a field depend on whether an
/// earlier field happened to be empty.</para>
/// </summary>
internal static class SplitFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count != 2)
            throw new KahunaScriptException("Invalid number of arguments for 'split' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "split");
        string separator = ScriptStringArgument.Require(ast, arguments[1], "split");

        // An empty separator is refused because the intent is unknowable: it could mean one part per
        // character, or no cut at all, and guessing either would be wrong half the time.
        if (separator.Length == 0)
            throw new KahunaScriptException("Separator for 'split' function must not be empty", ast.yyline);

        ReadOnlySpan<char> text = source.AsSpan();
        ReadOnlySpan<char> mark = separator.AsSpan();

        // The parts are counted before any of them is built, for two reasons. The list is then sized once
        // instead of doubling its buffer as it grows. And a string that would produce more parts than the
        // limit allows is refused before a single element is allocated.
        long parts = CountParts(ast, text, mark);

        // A string with no separator in it is one part, and that part is the string itself. Returning the
        // same instance is both the common case and the cheapest one.
        if (parts == 1)
            return new(new List<KeyValueExpressionResult>(1) { new(source) });

        List<KeyValueExpressionResult> result = new((int)parts);

        while (true)
        {
            int at = text.IndexOf(mark, StringComparison.Ordinal);

            if (at < 0)
            {
                result.Add(new(text.ToString()));
                break;
            }

            result.Add(new(text[..at].ToString()));

            text = text[(at + mark.Length)..];
        }

        return new(result);
    }

    /// <summary>
    /// How many parts the split produces. It walks the string exactly as the build below does, so the
    /// two always agree on where the cuts are.
    /// </summary>
    private static long CountParts(NodeAst ast, ReadOnlySpan<char> text, ReadOnlySpan<char> mark)
    {
        long parts = 1;

        while (true)
        {
            int at = text.IndexOf(mark, StringComparison.Ordinal);

            if (at < 0)
                return parts;

            parts++;

            if (parts > ScriptArrayLimits.MaxElements)
                throw new KahunaScriptException($"Split of more than {ScriptArrayLimits.MaxElements} elements exceeds the limit", ast.yyline);

            text = text[(at + mark.Length)..];
        }
    }
}
