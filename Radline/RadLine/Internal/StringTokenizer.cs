using System.Collections.Generic;

namespace RadLine
{
    internal static class StringTokenizer
    {
        public static IEnumerable<string> Tokenize(string text)
        {
            bool isOpen = false;
            string buffer = string.Empty;

            foreach (char character in text)
            {
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
