using System.Collections.Generic;

namespace RadLine
{
    internal static class StringTokenizer
    {
        public static IEnumerable<string> Tokenize(string text)
        {
            bool isOpen = false;
            string buffer = string.Empty;

            for (int index = 0; index < text.Length; index++)
            {
                char character = text[index];

                // Comments are only recognized outside of quoted strings.
                if (!isOpen)
                {
                    // Line comment: -- to the end of the line.
                    if (character == '-' && index + 1 < text.Length && text[index + 1] == '-')
                    {
                        if (buffer.Length > 0)
                        {
                            yield return buffer;
                            buffer = string.Empty;
                        }

                        yield return text.Substring(index);
                        yield break;
                    }

                    // Block comment: /* ... */ (or to end of line when unterminated).
                    if (character == '/' && index + 1 < text.Length && text[index + 1] == '*')
                    {
                        if (buffer.Length > 0)
                        {
                            yield return buffer;
                            buffer = string.Empty;
                        }

                        int end = text.IndexOf("*/", index + 2, System.StringComparison.Ordinal);
                        if (end < 0)
                        {
                            yield return text.Substring(index);
                            yield break;
                        }

                        yield return text.Substring(index, end + 2 - index);
                        index = end + 1;
                        continue;
                    }
                }

                if (char.IsLetterOrDigit(character) || character == '_' || character == '\"' || character == '\'' || character == '`' || isOpen)
                {
                    if (character is '\"' or '\'' or '`')
                        isOpen = !isOpen;

                    buffer += character;
                }
                else
                {
                    if (buffer.Length > 0)
                    {
                        yield return buffer;
                        buffer = string.Empty;
                    }

                    yield return new(character, 1);
                }
            }

            if (buffer.Length > 0)
                yield return buffer;
        }
    }
}
