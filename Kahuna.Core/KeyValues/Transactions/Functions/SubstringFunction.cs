using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;

namespace Kahuna.Server.KeyValues.Transactions.Functions;

/// <summary>
/// Reads part of a string: <c>substring(s, start)</c> is the rest of <c>s</c> from <c>start</c>, and
/// <c>substring(s, start, length)</c> is <c>length</c> characters from there.
///
/// <para>Positions count characters from zero. A start equal to the length of the string is the
/// position just past the last character, and it yields the empty string. That boundary is what makes
/// <c>substring(s, index_of(s, ':') + 1)</c> work when the separator is the last character. A start
/// beyond that point, or a length that runs past the end, is a script error: it is a computed position
/// that does not describe this string, and a clamp would hide the arithmetic that produced it.</para>
/// </summary>
internal static class SubstringFunction
{
    internal static KeyValueExpressionResult Execute(NodeAst ast, List<KeyValueExpressionResult> arguments)
    {
        if (arguments.Count is not (2 or 3))
            throw new KahunaScriptException("Invalid number of arguments for 'substring' function", ast.yyline);

        string source = ScriptStringArgument.Require(ast, arguments[0], "substring");

        long start = ScriptIntegerArgument.Require(ast, arguments[1], "Start index");

        if (start < 0 || start > source.Length)
            throw new KahunaScriptException($"Start index must be between 0 and {source.Length}. Found: {start}", ast.yyline);

        long available = source.Length - start;
        long length = available;

        if (arguments.Count == 3)
        {
            length = ScriptIntegerArgument.Require(ast, arguments[2], "Length");

            if (length < 0)
                throw new KahunaScriptException($"Length must not be negative. Found: {length}", ast.yyline);

            if (length > available)
                throw new KahunaScriptException($"Length must not exceed the {available} characters after index {start}. Found: {length}", ast.yyline);
        }

        // The whole string is returned as itself. Every other case allocates one string, for the part it
        // returns and nothing more: the slice is taken as a span, so no intermediate copy is made.
        if (start == 0 && length == source.Length)
            return new(source);

        return new(source.AsSpan((int)start, (int)length).ToString());
    }
}
