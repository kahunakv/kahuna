
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;
using Spectre.Console;

namespace Kahuna.Control;

public sealed class MyLineNumberPrompt : ILineEditorPrompt
{
    private const string FirstLinePrompt = "kahuna-cli> ";

    // Continuation lines are padded to the width of the first-line prompt so a multi-line script
    // keeps its indentation aligned with the first line.
    private static readonly string ContinuationPrompt = "...> ".PadLeft(FirstLinePrompt.Length);

    private readonly Style _style;

    public MyLineNumberPrompt(Style? style = null)
    {
        _style = style ?? new Style(foreground: Color.Yellow, background: Color.Blue);
    }

    public (Markup Markup, int Margin) GetPrompt(ILineEditorState state, int line)
    {
        return (new(line == 0 ? FirstLinePrompt : ContinuationPrompt, _style), 1);
    }
}
