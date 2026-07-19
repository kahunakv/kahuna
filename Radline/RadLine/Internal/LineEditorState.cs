using System;
using System.Collections.Generic;
using System.Linq;

namespace RadLine
{
    internal sealed class LineEditorState : ILineEditorState
    {
        private readonly List<LineBuffer> _lines;
        
        private int _lineIndex;

        public ILineEditorPrompt Prompt { get; }

        public int LineIndex => _lineIndex;
        
        public int LineCount => _lines.Count;
        
        public bool IsFirstLine => _lineIndex == 0;
        
        public bool IsLastLine => _lineIndex == _lines.Count - 1;
        
        public LineBuffer Buffer => _lines[_lineIndex];

        public bool IsEmpty => string.IsNullOrWhiteSpace(Text.TrimEnd('\r', '\n'));
        public string Text => string.Join(Environment.NewLine, _lines.Select(x => x.Content));

        public LineEditorState(ILineEditorPrompt prompt, string text)
        {
            _lines = new List<LineBuffer>();
            _lineIndex = 0;

            Prompt = prompt ?? throw new ArgumentNullException(nameof(prompt));

            // Add all lines
            foreach (string line in text.NormalizeNewLines().Split(['\n']))
                _lines.Add(new LineBuffer(line));

            // No lines?
            if (_lines.Count == 0)
                _lines.Add(new LineBuffer());
        }

        public LineBuffer GetBufferAt(int line)
        {
            return _lines[line];
        }

        public IList<LineBuffer> GetBuffers()
        {
            return _lines;
        }

        public bool SetContent(IList<LineBuffer> buffers, int lineIndex)
        {
            if (buffers is null)
                throw new ArgumentNullException(nameof(buffers));

            if (buffers.Count == 0)
                return false;

            _lines.Clear();
            _lines.AddRange(buffers);
            _lineIndex = lineIndex;

            return true;
        }

        public void Move(int line)
        {
            _lineIndex = Math.Max(0, Math.Min(line, LineCount - 1));
        }

        public bool MoveUp()
        {
            if (_lineIndex > 0)
            {
                _lineIndex--;
                return true;
            }

            return false;
        }

        public bool MoveDown()
        {
            if (_lineIndex < _lines.Count - 1)
            {
                _lineIndex++;
                return true;
            }

            return false;
        }

        public void RemoveAllLines()
        {
            _lines.Clear();
            _lineIndex = -1;
        }

        public void AddLine(string? content = null)
        {
            _lines.Add(new LineBuffer(content));
            _lineIndex++;
        }

        // Joins the current line onto the end of the previous one (backspace at column 0).
        // The merged line becomes the current line and the cursor is placed at the join point.
        public void MergeWithPrevious()
        {
            if (_lineIndex <= 0)
                return;

            int join = _lines[_lineIndex - 1].Length;
            string merged = _lines[_lineIndex - 1].Content + _lines[_lineIndex].Content;

            _lines.RemoveAt(_lineIndex);
            _lineIndex--;

            _lines[_lineIndex] = new LineBuffer(merged);
            _lines[_lineIndex].Move(join);
        }

        // Pulls the next line onto the end of the current one (delete at end of line).
        // The cursor stays at the join point on the current line.
        public void MergeWithNext()
        {
            if (_lineIndex >= _lines.Count - 1)
                return;

            int join = _lines[_lineIndex].Length;
            string merged = _lines[_lineIndex].Content + _lines[_lineIndex + 1].Content;

            _lines.RemoveAt(_lineIndex + 1);

            _lines[_lineIndex] = new LineBuffer(merged);
            _lines[_lineIndex].Move(join);
        }
    }
}
