using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Spectre.Console;

namespace RadLine
{
    public sealed class LineEditor : IHighlighterAccessor
    {
        private readonly IInputSource _source;
        private readonly IServiceProvider? _provider;
        private readonly IAnsiConsole _console;
        private readonly LineEditorRenderer _renderer;
        private readonly LineEditorHistory _history;
        private readonly InputBuffer _input;

        public KeyBindings KeyBindings { get; }
        public bool MultiLine { get; init; } = false;
        public string Text { get; init; } = string.Empty;

        public ILineEditorPrompt Prompt { get; init; } = new LineEditorPrompt("[yellow]>[/]");
        public ITextCompletion? Completion { get; init; }
        public IHighlighter? Highlighter { get; init; }
        public ILineEditorHistory History => _history;

        public LineEditor(IAnsiConsole? terminal = null, IInputSource? source = null, IServiceProvider? provider = null)
        {
            _console = terminal ?? AnsiConsole.Console;
            _source = source ?? new DefaultInputSource(_console);
            _provider = provider;
            _renderer = new LineEditorRenderer(_console, this);
            _history = new LineEditorHistory();
            _input = new InputBuffer(_source);

            KeyBindings = new KeyBindings();
            KeyBindings.AddDefault();
        }

        public static bool IsSupported(IAnsiConsole console)
        {
            if (console is null)
            {
                throw new ArgumentNullException(nameof(console));
            }

            return
                console.Profile.Out.IsTerminal &&
                console.Profile.Capabilities.Ansi &&
                console.Profile.Capabilities.Interactive;
        }

        public async Task<string?> ReadLine(CancellationToken cancellationToken)
        {
            var cancelled = false;
            var state = new LineEditorState(Prompt, Text);

            _history.Reset();
            _input.Initialize(KeyBindings);

            // Ask the terminal to bracket pastes (ESC[200~ ... ESC[201~) so the input layer can
            // tell pasted newlines apart from a real Enter keypress. Terminals that don't support
            // it simply ignore the request. Always turned back off in the finally below.
            _console.WriteAnsi("\u001b[?2004h");

            try
            {
            _renderer.Refresh(state);

            while (true)
            {
                var result = await ReadLine(state, cancellationToken).ConfigureAwait(false);

                if (result.Result == SubmitAction.Cancel)
                {
                    // A real (token) cancellation always aborts. A Ctrl+C keypress only aborts when
                    // the prompt is empty; if there's content, discard it and drop to a fresh prompt.
                    if (cancellationToken.IsCancellationRequested || state.IsEmpty)
                    {
                        cancelled = true;
                        break;
                    }

                    // Leave the abandoned input on screen, move past it, and start over empty.
                    _renderer.RenderLine(state, cursorPosition: 0);
                    while (state.MoveDown())
                    {
                        _console.Cursor.MoveDown();
                    }

                    _console.WriteLine();

                    state = new LineEditorState(Prompt, string.Empty);
                    _history.Reset();
                    _renderer.Refresh(state);
                    continue;
                }
                else if (result.Result == SubmitAction.Submit)
                {
                    break;
                }
                else if (result.Result == SubmitAction.PreviousHistory)
                {
                    if (_history.MovePrevious(state) && !SetContent(state, _history.Current))
                    {
                        continue;
                    }
                }
                else if (result.Result == SubmitAction.NextHistory)
                {
                    if (_history.MoveNext() && !SetContent(state, _history.Current))
                    {
                        continue;
                    }
                }
                else if (result.Result == SubmitAction.NewLine && MultiLine && state.IsLastLine)
                {
                    // Add a new line
                    state.AddLine();

                    // Refresh
                    var builder = new StringBuilder();
                    builder.Append("\u001b[?25l"); // Hide cursor
                    _renderer.AnsiBuilder.MoveDown(builder, state);
                    _renderer.AnsiBuilder.BuildRefresh(builder, state);
                    builder.Append("\u001b[?25h"); // Show cursor
                    _console.WriteAnsi(builder.ToString());
                }
                else if (result.Result == SubmitAction.Backspace && MultiLine && !state.IsFirstLine)
                {
                    MergeLine(state, withNext: false);
                }
                else if (result.Result == SubmitAction.Delete && MultiLine && !state.IsLastLine)
                {
                    MergeLine(state, withNext: true);
                }
                else if (result.Result == SubmitAction.MoveUp && MultiLine)
                {
                    MoveUp(state);
                }
                else if (result.Result == SubmitAction.MoveDown && MultiLine)
                {
                    MoveDown(state);
                }
                else if (result.Result == SubmitAction.MoveFirst && MultiLine)
                {
                    MoveFirst(state);
                }
                else if (result.Result == SubmitAction.MoveLast && MultiLine)
                {
                    MoveLast(state);
                }
            }

            _renderer.RenderLine(state, cursorPosition: 0);

            // Move the cursor to the last line
            while (state.MoveDown())
            {
                _console.Cursor.MoveDown();
            }

            // Moving the cursor won't work here if we're at
            // the bottom of the screen, so let's insert a new line.
            _console.WriteLine();

            // Add the current state to the history
            if (!state.IsEmpty)
            {
                _history.Add(state.GetBuffers());
            }

            // Return all the lines
            return cancelled ? null : state.Text;
            }
            finally
            {
                // Stop bracketed paste so it doesn't leak into whatever reads input next.
                _console.WriteAnsi("\u001b[?2004l");
            }
        }

        private async Task<(LineBuffer Buffer, SubmitAction Result)> ReadLine(
            LineEditorState state,
            CancellationToken cancellationToken)
        {
            var provider = new DefaultServiceProvider(_provider);
            provider.RegisterOptional<ITextCompletion, ITextCompletion>(Completion);
            var context = new LineEditorContext(state.Buffer, provider);

            while (true)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return (state.Buffer, SubmitAction.Cancel);
                }

                // Get command
                var command = default(LineEditorCommand);
                var key = await _input.ReadKey(MultiLine, cancellationToken).ConfigureAwait(false);

                // Console.WriteLine(key.Value.Key);
                if (key != null)
                {
                    if (key.Value.KeyChar != 0 && !char.IsControl(key.Value.KeyChar))
                    {
                        command = new InsertCommand(key.Value.KeyChar);
                    }
                    else
                    {
                        command = GetCommand(state, key.Value);
                    }
                }

                // Execute command
                if (command != null)
                {
                    // Console.WriteLine("exec command " + command.GetType().Name);
                    context.Execute(command);
                }

                // Time to exit?
                if (context.Result != null)
                {
                    return (state.Buffer, context.Result.Value);
                }

                // Render the line
                _renderer.RenderLine(state);
            }
        }

        private LineEditorCommand? GetCommand(LineEditorState state, ConsoleKeyInfo key)
        {
            if (MultiLine && state.LineCount > 1 && key.Modifiers == 0)
            {
                if (key.Key == ConsoleKey.UpArrow && !state.IsFirstLine)
                {
                    return new MoveUpCommand();
                }

                if (key.Key == ConsoleKey.DownArrow && !state.IsLastLine)
                {
                    return new MoveDownCommand();
                }
            }

            return KeyBindings.GetCommand(key.Key, key.Modifiers);
        }

        // Joins two lines into one and redraws. Because the line count shrinks, we clear the
        // whole block first (using the pre-merge state), perform the merge, then refresh —
        // the same full-rebuild strategy used by history navigation in SetContent.
        private void MergeLine(LineEditorState state, bool withNext)
        {
            var builder = new StringBuilder();

            // Clear the currently displayed lines; cursor ends at the top of the block.
            _renderer.AnsiBuilder.BuildClear(builder, state);

            // Hide the cursor while we rebuild.
            builder.Append("\u001b[?25l");

            // Collapse the two lines. This updates the current line index and cursor position.
            if (withNext)
            {
                state.MergeWithNext();
            }
            else
            {
                state.MergeWithPrevious();
            }

            // BuildRefresh expects the physical cursor to sit on the current line, so move it
            // down from the top of the block to the merged line before refreshing.
            if (state.LineIndex > 0)
            {
                builder.Append("\u001b[").Append(state.LineIndex).Append('B');
            }

            _renderer.AnsiBuilder.BuildRefresh(builder, state);

            // Show the cursor again.
            builder.Append("\u001b[?25h");

            _console.WriteAnsi(builder.ToString());
        }

        private void MoveUp(LineEditorState state)
        {
            Move(state, () =>
            {
                if (state.MoveUp())
                {
                    _console.Cursor.MoveUp();
                }
            });
        }

        private void MoveDown(LineEditorState state)
        {
            Move(state, () =>
            {
                if (state.MoveDown())
                {
                    _console.Cursor.MoveDown();
                }
            });
        }

        private void MoveFirst(LineEditorState state)
        {
            Move(state, () =>
            {
                while (state.MoveUp())
                {
                    _console.Cursor.MoveUp();
                }
            });
        }

        private void MoveLast(LineEditorState state)
        {
            Move(state, () =>
            {
                while (state.MoveDown())
                {
                    _console.Cursor.MoveDown();
                }
            });
        }

        private void Move(LineEditorState state, Action action)
        {
            using (_console.HideCursor())
            {
                if (state.LineCount > _console.Profile.Height)
                {
                    // Get the current position
                    var position = state.Buffer.Position;

                    // Refresh everything
                    action();
                    _renderer.Refresh(state);

                    // Re-render the current line at the correct position
                    state.Buffer.Move(position);
                    _renderer.RenderLine(state);
                }
                else
                {
                    // Get the current position
                    var position = state.Buffer.Position;

                    // Reset the line
                    _renderer.RenderLine(state, cursorPosition: 0);
                    action();

                    // Render the current line at the correct position
                    state.Buffer.Move(position);
                    _renderer.RenderLine(state);
                }
            }
        }

        private bool SetContent(LineEditorState state, IList<LineBuffer>? lines)
        {
            // Nothing to set?
            if (lines == null || lines.Count == 0)
            {
                return false;
            }

            var builder = new StringBuilder();

            // Clearing the current lines will
            // move the cursor to the top.
            _renderer.AnsiBuilder.BuildClear(builder, state);

            // Remove all lines
            state.RemoveAllLines();

            // Hide the cursor
            builder.Append("\u001b[?25l");

            // Add all the lines
            foreach (var line in lines)
            {
                state.AddLine(line.Content);
            }

            // Make room for all the lines
            var first = true;
            foreach (var line in lines)
            {
                var shouldAddNewLine = true;
                if (first)
                {
                    shouldAddNewLine = false;
                    first = false;
                }

                if (shouldAddNewLine)
                {
                    _renderer.AnsiBuilder.MoveDown(builder, state);
                }
            }

            _renderer.AnsiBuilder.BuildRefresh(builder, state);

            // Show the cursor again
            builder.Append("\u001b[?25h");

            // Flush
            _console.WriteAnsi(builder.ToString());
            return true;
        }
    }
}
