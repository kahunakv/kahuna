using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RadLine
{
    internal sealed class InputBuffer
    {
        // How long to keep waiting for the next key of a burst before deciding the burst is over.
        // Pastes stream faster than this; human keystrokes are far slower, so typing is unaffected
        // (the drain only runs at all once a burst has already been detected).
        private const int GracePeriodMs = 30;

        private readonly IInputSource _source;
        private readonly Queue<ConsoleKeyInfo> _queue;
        private readonly List<ConsoleKeyInfo> _escBuffer;
        private KeyBinding? _newLineBinding;
        private KeyBinding? _submitBinding;

        // Bracketed-paste state. When the terminal wraps a paste in ESC[200~ ... ESC[201~,
        // every newline between those markers is content, not a request to submit.
        private bool _inPaste;
        private bool _lastPasteCarriageReturn;

        private enum MarkerMatch { None, Partial, StartComplete, EndComplete }

        public InputBuffer(IInputSource source)
        {
            _source = source ?? throw new ArgumentNullException(nameof(source));
            _queue = new Queue<ConsoleKeyInfo>();
            _escBuffer = new List<ConsoleKeyInfo>();
        }

        public void Initialize(KeyBindings bindings)
        {
            bindings.TryFindKeyBindings<NewLineCommand>(out _newLineBinding);
            bindings.TryFindKeyBindings<SubmitCommand>(out _submitBinding);

            // Start each read with a clean slate so an interrupted paste can't leave us stuck.
            _inPaste = false;
            _lastPasteCarriageReturn = false;
            _escBuffer.Clear();
        }

        public async Task<ConsoleKeyInfo?> ReadKey(bool multiline, CancellationToken cancellationToken)
        {
            if (_queue.Count > 0)
            {
                return _queue.Dequeue();
            }

            // Wait for the user to enter a key
            var key = await ReadKeyFromSource(wait: true, cancellationToken);
            if (key == null)
            {
                return null;
            }

            Feed(key.Value);

            if (_source.IsKeyAvailable())
            {
                // Read all remaining keys from the buffer
                await ReadRemainingKeys(multiline, cancellationToken);
            }

            // Got something?
            if (_queue.Count > 0)
            {
                return _queue.Dequeue();
            }

            return null;
        }

        private async Task ReadRemainingKeys(bool multiline, CancellationToken cancellationToken)
        {
            var keys = new Queue<ConsoleKeyInfo>();

            // Keep pulling keys as long as they keep arriving within a short grace window. A paste
            // streams its bytes in bursts that can have sub-millisecond gaps; without the grace
            // period the drain would stop at the first gap and split one paste into several
            // batches, letting a mid-paste newline slip through as a submit.
            while (true)
            {
                var key = await ReadKeyWithGrace(GracePeriodMs, cancellationToken);
                if (key == null)
                {
                    break;
                }

                keys.Enqueue(key.Value);
            }

            if (keys.Count == 0)
            {
                return;
            }

            // Fallback for terminals that don't emit bracketed-paste markers (e.g. macOS
            // Terminal.app): a batch of keys arriving together is almost certainly a paste, so a
            // newline in it is content, not a submit. Applies only when not already inside a marked
            // paste (Feed handles that case). Any Enter counts here regardless of modifiers, since a
            // pasted newline may arrive as CR (Enter), LF (Enter+Control), or a CRLF pair.
            var shouldProcess = multiline && keys.Count >= 5 && !_source.ByPassProcessing;
            var lastWasCarriageReturn = false;

            while (keys.Count > 0)
            {
                var key = keys.Dequeue();

                if (shouldProcess && !_inPaste && _newLineBinding != null && IsNewline(key))
                {
                    // Collapse a CRLF pair into a single line break.
                    if (key.KeyChar == '\n' && lastWasCarriageReturn)
                    {
                        lastWasCarriageReturn = false;
                        continue;
                    }

                    lastWasCarriageReturn = key.KeyChar == '\r';
                    Feed(_newLineBinding.AsConsoleKeyInfo());
                    continue;
                }

                lastWasCarriageReturn = false;
                Feed(key);
            }
        }

        // Routes a raw key through the bracketed-paste state machine, enqueueing zero or more
        // processed keys. Paste markers are stripped; newlines inside a paste become line breaks.
        private void Feed(ConsoleKeyInfo key)
        {
            if (_escBuffer.Count > 0 || key.Key == ConsoleKey.Escape)
            {
                _escBuffer.Add(key);

                switch (MatchMarker(_escBuffer))
                {
                    case MarkerMatch.StartComplete:
                        _inPaste = true;
                        _lastPasteCarriageReturn = false;
                        _escBuffer.Clear();
                        return;

                    case MarkerMatch.EndComplete:
                        _inPaste = false;
                        _lastPasteCarriageReturn = false;
                        _escBuffer.Clear();
                        return;

                    case MarkerMatch.Partial:
                        return;

                    default:
                        // Not a paste marker after all; replay the buffered keys as ordinary input.
                        var buffered = _escBuffer.ToArray();
                        _escBuffer.Clear();
                        foreach (var buffate in buffered)
                        {
                            EmitProcessed(buffate);
                        }
                        return;
                }
            }

            EmitProcessed(key);
        }

        private void EmitProcessed(ConsoleKeyInfo key)
        {
            if (_inPaste && _newLineBinding != null && IsNewline(key))
            {
                // Collapse a CR/LF pair (pasted CRLF) into a single line break.
                if (key.KeyChar == '\n' && _lastPasteCarriageReturn)
                {
                    _lastPasteCarriageReturn = false;
                    return;
                }

                _lastPasteCarriageReturn = key.KeyChar == '\r';
                _queue.Enqueue(_newLineBinding.AsConsoleKeyInfo());
                return;
            }

            _lastPasteCarriageReturn = false;
            _queue.Enqueue(key);
        }

        // Recognizes every form a pasted line break can take. Besides the usual Enter/CR/LF, some
        // sources (notably macOS apps) put Unicode line/paragraph separators on the clipboard:
        // U+2028 (LINE SEPARATOR), U+2029 (PARAGRAPH SEPARATOR), and U+0085 (NEXT LINE). These
        // arrive as ordinary characters (Key=None) and would otherwise be inserted literally,
        // silently joining the pasted lines into one.
        private static bool IsNewline(ConsoleKeyInfo key)
        {
            if (key.Key == ConsoleKey.Enter)
            {
                return true;
            }

            return key.KeyChar is '\n' or '\r' or '\u2028' or '\u2029' or '\u0085';
        }

        // Matches the accumulated keys against ESC [ 2 0 0 ~ (paste start) / ESC [ 2 0 1 ~ (paste end).
        private static MarkerMatch MatchMarker(List<ConsoleKeyInfo> buffer)
        {
            for (var i = 0; i < buffer.Count; i++)
            {
                var c = buffer[i].KeyChar;
                var ok = i switch
                {
                    0 => buffer[0].Key == ConsoleKey.Escape,
                    1 => c == '[',
                    2 => c == '2',
                    3 => c == '0',
                    4 => c == '0' || c == '1',
                    5 => c == '~',
                    _ => false,
                };

                if (!ok)
                {
                    return MarkerMatch.None;
                }
            }

            if (buffer.Count < 6)
            {
                return MarkerMatch.Partial;
            }

            return buffer[4].KeyChar == '0' ? MarkerMatch.StartComplete : MarkerMatch.EndComplete;
        }

        // Reads the next key if one arrives within graceMs; otherwise returns null. Used to bridge
        // the tiny gaps between the chunks of a paste so the whole paste is treated as one burst.
        private async Task<ConsoleKeyInfo?> ReadKeyWithGrace(int graceMs, CancellationToken cancellationToken)
        {
            var deadline = Environment.TickCount64 + graceMs;

            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return null;
                }

                if (_source.IsKeyAvailable())
                {
                    return _source.ReadKey();
                }

                if (Environment.TickCount64 >= deadline)
                {
                    return null;
                }

                await Task.Delay(2, cancellationToken).ConfigureAwait(false);
            }
        }

        private async Task<ConsoleKeyInfo?> ReadKeyFromSource(bool wait, CancellationToken cancellationToken)
        {
            if (wait)
            {
                while (true)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        return null;
                    }

                    if (_source.IsKeyAvailable())
                    {
                        break;
                    }

                    await Task.Delay(5, cancellationToken).ConfigureAwait(false);
                }
            }

            if (_source.IsKeyAvailable())
            {
                return _source.ReadKey();
            }

            return null;
        }
    }
}
