namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Scanner for the Script Parser
/// </summary>
internal partial class scriptScanner
{
    private int lastTokenLine;

    private int lastTokenColumn;

    private string? lastTokenText;

    public string? YYError { get; set; }

    public int YYErrorLine { get; private set; }

    public int YYErrorColumn { get; private set; }

    public string? YYErrorToken { get; private set; }

    /// <summary>
    /// Records the current token location for parser error reporting.
    /// </summary>
    /// <param name="line">One-based line number from the generated scanner.</param>
    /// <param name="column">Zero-based column number from the generated scanner.</param>
    /// <param name="length">Token length.</param>
    internal void SetTokenLocation(int line, int column, int length)
    {
        lastTokenLine = line;
        lastTokenColumn = column + 1;
        lastTokenText = yytext;

        int tokenEndColumn = Math.Max(lastTokenColumn, lastTokenColumn + length);
        yylloc = new(lastTokenLine, lastTokenColumn, lastTokenLine, tokenEndColumn);
    }

    /// <summary>
    /// Decodes the backslash escapes inside a string literal and returns the text between the quotes.
    /// The lexer patterns have always recognised these sequences, but the actions passed them through as
    /// literal two-character text, so a script had no way at all to put a line break in a value: the escape
    /// did nothing and the raw control character is not accepted inside a literal.
    ///
    /// <para>An unrecognised escape is an error rather than a character silently dropped or kept. Which of
    /// the two the author meant is unknowable, and both readings are wrong half the time.</para>
    ///
    /// <para>The common case is a literal with no backslash at all, and that case returns a plain slice with
    /// no builder and no copy beyond the substring the caller needs anyway.</para>
    /// </summary>
    /// <param name="literal">The matched token, quotes included.</param>
    internal string UnescapeLiteral(string literal)
    {
        // The first and last characters are the quotes the pattern matched. They are dropped by position
        // rather than trimmed, because trimming would also eat an escaped quote at either end.
        ReadOnlySpan<char> body = literal.AsSpan(1, literal.Length - 2);

        int firstEscape = body.IndexOf('\\');

        if (firstEscape < 0)
            return new string(body);

        System.Text.StringBuilder builder = new(body.Length);

        builder.Append(body[..firstEscape]);

        for (int index = firstEscape; index < body.Length; index++)
        {
            char current = body[index];

            if (current != '\\')
            {
                builder.Append(current);
                continue;
            }

            if (index + 1 >= body.Length)
            {
                yyerror("Unterminated escape sequence in string literal");
                return builder.ToString();
            }

            char escape = body[++index];

            switch (escape)
            {
                case 'n': builder.Append('\n'); break;
                case 't': builder.Append('\t'); break;
                case 'r': builder.Append('\r'); break;
                case 'a': builder.Append('\a'); break;
                case 'b': builder.Append('\b'); break;
                case 'f': builder.Append('\f'); break;
                case 'v': builder.Append('\v'); break;
                case '\\': builder.Append('\\'); break;
                case '"': builder.Append('"'); break;
                case '\'': builder.Append('\''); break;
                case '`': builder.Append('`'); break;

                case >= '0' and <= '7':
                    // One to three octal digits, so a lone \0 is the null character and \101 is a capital
                    // A. The patterns accept the three-digit form, so the decoder has to as well.
                    AppendOctal(builder, body, ref index);
                    break;

                case 'x':
                    if (!TryAppendCodePoint(builder, body, ref index, 2))
                        return builder.ToString();
                    break;

                case 'u':
                    if (!TryAppendCodePoint(builder, body, ref index, 4))
                        return builder.ToString();
                    break;

                case 'U':
                    if (!TryAppendCodePoint(builder, body, ref index, 8))
                        return builder.ToString();
                    break;

                default:
                    yyerror("Unknown escape sequence '\\{0}' in string literal", escape);
                    return builder.ToString();
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Reads the one to three octal digits of an escape that begins at <paramref name="index"/> and appends
    /// the character they name. Advances <paramref name="index"/> past the digits it consumed.
    /// </summary>
    private static void AppendOctal(System.Text.StringBuilder builder, ReadOnlySpan<char> body, ref int index)
    {
        int value = 0;
        int digits = 0;

        while (digits < 3 && index < body.Length && body[index] >= '0' && body[index] <= '7')
        {
            value = (value * 8) + (body[index] - '0');
            index++;
            digits++;
        }

        // The loop stops on the character after the last digit, and the caller advances once more.
        index--;

        builder.Append((char)value);
    }

    /// <summary>
    /// Reads a fixed-width hexadecimal code point that follows an <c>\x</c>, <c>\u</c> or <c>\U</c> marker
    /// and appends the character it names. Advances <paramref name="index"/> past the digits it consumed.
    /// Returns false when the sequence is malformed, after reporting the error.
    /// </summary>
    private bool TryAppendCodePoint(System.Text.StringBuilder builder, ReadOnlySpan<char> body, ref int index, int digits)
    {
        if (index + digits >= body.Length)
        {
            yyerror("Truncated escape sequence in string literal");
            return false;
        }

        ReadOnlySpan<char> hex = body.Slice(index + 1, digits);

        if (!uint.TryParse(hex, System.Globalization.NumberStyles.HexNumber, System.Globalization.CultureInfo.InvariantCulture, out uint value))
        {
            yyerror("Invalid escape sequence '{0}' in string literal", new string(hex));
            return false;
        }

        if (value > 0x10FFFF || (value >= 0xD800 && value <= 0xDFFF))
        {
            yyerror("Escape sequence '{0}' is not a character", new string(hex));
            return false;
        }

        builder.Append(char.ConvertFromUtf32((int)value));

        index += digits;

        return true;
    }

    /// <summary>
    /// Intercepts the yyerror method
    /// </summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public override void yyerror(string format, params object[] args)
    {
        base.yyerror(format, args);

        // Keep the first error, not the last. A rejected character is reported from the scanner and then
        // provokes a syntax error further along; the character is the cause and the syntax error is the
        // symptom, so overwriting here would point the author at the wrong place. A scanner is built per
        // parse, so this never carries an error across calls.
        if (!string.IsNullOrEmpty(YYError))
            return;

        YYError = string.Format(format, args);
        YYErrorLine = lastTokenLine;
        YYErrorColumn = lastTokenColumn;
        YYErrorToken = lastTokenText;
    }
}
