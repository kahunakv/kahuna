/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Control;

/// <summary>
/// Decides whether the text typed in the interactive console is ready to send, so pressing Enter
/// inside an open IF / FOR / SWITCH / BEGIN block or an open string literal continues the input on
/// a new line instead of sending a script the parser would reject as unfinished.
/// </summary>
public static class ScriptInputCompleteness
{
    // Console commands handled by the shell itself rather than parsed as a script. Their arguments
    // are names (a lock called "begin", a sequence called "for"), so block keywords there are not
    // block openers.
    private static readonly string[] ShellCommandPrefixes =
    [
        "run ",
        "lock ",
        "elock ",
        "unlock ",
        "eunlock ",
        "get-lock ",
        "eget-lock ",
        "extend-lock ",
        "eextend-lock ",
        "create-sequence ",
        "get-sequence ",
        "next-sequence ",
        "reserve-sequence ",
        "update-sequence ",
        "delete-sequence ",
        "cluster ",
        "backup ",
        "list ",
    ];

    /// <summary>
    /// Returns true when every block opened in <paramref name="text"/> is closed by an END and every
    /// string literal is closed, or when the text ends with two blank lines (an explicit request to
    /// send the input as it is).
    /// Over-closed or otherwise malformed text counts as complete so the server reports the error.
    /// </summary>
    public static bool IsComplete(string text)
    {
        ReadOnlySpan<char> input = text.AsSpan();

        if (EndsWithTwoBlankLines(input))
            return true;

        ReadOnlySpan<char> trimmed = input.Trim();

        foreach (string prefix in ShellCommandPrefixes)
        {
            if (trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        int depth = 0;
        int i = 0;

        while (i < input.Length)
        {
            char c = input[i];

            // Quoted strings and escaped identifiers: keywords inside them do not count. A string
            // literal may span lines, so an unclosed one at the end of the text leaves the input open.
            // A backtick identifier cannot span lines, so an unclosed one ends at the line break.
            if (c is '\'' or '"')
            {
                i++;

                while (i < input.Length && input[i] != c)
                    i += input[i] == '\\' ? 2 : 1;

                if (i >= input.Length)
                    return false;

                i++;
                continue;
            }

            if (c == '`')
            {
                i++;

                while (i < input.Length && input[i] != c && input[i] != '\n' && input[i] != '\r')
                    i += input[i] == '\\' ? 2 : 1;

                i++;
                continue;
            }

            if (char.IsAsciiLetter(c) || c == '_')
            {
                int start = i;

                while (i < input.Length && (char.IsAsciiLetterOrDigit(input[i]) || input[i] == '_'))
                    i++;

                // A word right after '@' is a placeholder name, not a keyword.
                if (start > 0 && input[start - 1] == '@')
                    continue;

                ReadOnlySpan<char> word = input[start..i];

                if (IsBlockOpener(word))
                    depth++;
                else if (word.Equals("end", StringComparison.OrdinalIgnoreCase))
                    depth--;

                continue;
            }

            i++;
        }

        return depth <= 0;
    }

    private static bool IsBlockOpener(ReadOnlySpan<char> word)
    {
        return word.Equals("if", StringComparison.OrdinalIgnoreCase)
            || word.Equals("for", StringComparison.OrdinalIgnoreCase)
            || word.Equals("switch", StringComparison.OrdinalIgnoreCase)
            || word.Equals("begin", StringComparison.OrdinalIgnoreCase);
    }

    private static bool EndsWithTwoBlankLines(ReadOnlySpan<char> input)
    {
        int blankLines = 0;
        int end = input.Length;

        while (blankLines < 2)
        {
            int lineBreak = input[..end].LastIndexOf('\n');
            if (lineBreak < 0)
                return false;

            if (!input[(lineBreak + 1)..end].IsWhiteSpace())
                return false;

            blankLines++;
            end = lineBreak;
        }

        return true;
    }
}
