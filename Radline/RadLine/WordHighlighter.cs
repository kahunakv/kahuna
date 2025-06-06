using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Spectre.Console;

namespace RadLine
{
    public sealed class WordHighlighter : IHighlighter
    {
        private readonly Dictionary<string, Style> _words;
        
        private readonly Dictionary<string, (Regex, Style)> _regexes;

        public WordHighlighter(StringComparer? comparer = null)
        {
            _words = new(comparer ?? StringComparer.OrdinalIgnoreCase);
            _regexes = new(comparer ?? StringComparer.OrdinalIgnoreCase);
        }

        public WordHighlighter AddWord(string word, Style style)
        {
            _words[word] = style;
            return this;
        }
        
        public WordHighlighter AddRegex(string regex, Style style)
        {
            _regexes[regex] = (new Regex(regex, RegexOptions.Compiled), style);
            return this;
        }

        Style? IHighlighter.Highlight(string token)
        {
            foreach (KeyValuePair<string, (Regex regex, Style styleRegex)> kv in _regexes)
            {
                if (kv.Value.regex.IsMatch(token))
                    return kv.Value.styleRegex;
            }
            
            _words.TryGetValue(token, out Style? style);
            return style;
        }
    }
}
