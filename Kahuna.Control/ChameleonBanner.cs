/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Spectre.Console;

namespace Kahuna.Control;

/// <summary>
/// The chameleon mascot that greets a user the first time the interactive shell starts.
/// </summary>
internal static class ChameleonBanner
{
    // Shows the chameleon only on the first interactive start for this user, so later starts open
    // straight at the prompt. A marker file in the user's application data folder records that the art
    // was shown; deleting it shows the art again on the next start. A start that skipped the art (a
    // narrow terminal) does not count, and without a known data folder the art is never shown.
    public static async Task ShowOnce()
    {
        string dataFolder = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
        if (string.IsNullOrEmpty(dataFolder))
            return;

        string marker = Path.Combine(dataFolder, "kahuna", "cli-chameleon-shown");
        if (File.Exists(marker))
            return;

        if (!await Print())
            return;

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(marker)!);
            File.WriteAllBytes(marker, []);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // The art is cosmetic: a folder that cannot be written only means it shows again next time.
        }
    }

    // Prints the chameleon mascot as colored ASCII art and returns whether it was printed. Each character
    // of an art row takes its color from the code at the same position in the matching shades row:
    // d = dark, f = forest, m = mid, l = light green, w = white, space = no color. The colors are the
    // mascot's own. A terminal narrower than the art skips it, because wrapped rows would break it.
    // On an interactive terminal the art is animated for about two seconds: the characters appear in
    // random order, then a wave of colors runs from the head to the tail and settles back on green.
    private static async Task<bool> Print()
    {
        string[] art =
        [
            "                        ,,o:@#",
            "                     :+*+:x+@@",
            "                 :.:++:;:*;@##",
            "              ..o*++:x***+x@##",
            "            .+:+++o+o+;:*+*@&+*xx%xX*;;.",
            "        ,.@@@#@%@+++;;++o+%#@..:.::.:*.;:oo;;",
            "      +;%&@@@@@@#&@o+oo+++@&%..'+.;:.::'..;:+Xo::",
            "    ,+x@@%@&::.,&@&&:+*x+:@##::;::.:'*+;+:;*x+xX*o:",
            "   ,:xo@#@,'`::'#@#%:x++++@@:'..'*.;'*;;++*Xo%oX+ooXX;",
            "  o+x*::@@@,,:.@##&x;@#:;@@@.*+:;';;.*.:+;%ooox%%oxo*xo+",
            " *x.o+*o*@@#@@%&@o+@@&o:;&&@:*:.;':+'*::.:o**xxoXxoXoxxxo.",
            "@@+*o+o:+o*+;++#@@#&:::.&@@:.:;:;:..:::*::*x:+**+xo%xo+ox%*",
            " *@%#@%###&%#@##;.+:';@@@.:`,.`..'':.:,..o**+;+:*+::+oXxXo+o:",
            "    ,:.:::;...*:.*''+*+:.'..*'..',..:''`,+***;oo:+o;;x+oo*o%XX",
            "          :::'::::x;*`:.:..+*o+x::.:''::,+xo+:;+;o++;+oxo*;*%Xo",
            "                    :`:.:.,:o+XXX+x:::...*o;+;++x;;*o+*x:++:;+.;",
            "                   *.:`::,`:'*+%xxX*`:...x+*xx*+*x*+x;+++o:**o'':",
            "                  ***oo+':'.,.+o::++.:..';*;##@#@&@#@&@@#o*:*+o:..",
            "                 o:x:::    ,:;;x:oo.`::..o#@@#@%#@@@%@@@@@#x;*+*++",
            "                ;;+;:o      o+xx+:'...:..#@@@*'@#@#@&@@#@&%@+;+++.",
            "            ;xo+x*o+;     ::o++:+    ';;@@#@:@&@..+&&#@@#&@#@x::x+",
            "           xXo+'+:.,  x+ooX+*:+:    :.:.@@@+.@@;+*:+@@@&@@%&%*:*+.",
            "          oxo*:.::. ;oXooX*:.':    :.+*.@@#@*##@+:::@@@&@:@@@x*x*",
            "                   ;xXxxx''+:     ++;:*.@@@@:.@#%#%@&@@:;@%&xxo+",
            "                        ::..             @%%@@:.+'+:+::@@@@o+x;",
            "                                          *%&@#@&##%%@%@@@o+:,",
            "                                            .#&@@@@%@&#:;:'",
            "                                               :.***.*.,",
        ];

        string[] shades =
        [
            "                        mmmmww",
            "                     mmmmmmmww",
            "                 mmmmmmmmmmwww",
            "              mmmmmmmmmmmmmwww",
            "            mmmmmmmmmmmmmmmwwmmlllllllll",
            "        mmwwwwwwwmmmmmmmmmwwwffffffffffffllll",
            "      mmwwwwwwwwwwwmmmmmmmwwwffffffffffffffffllll",
            "    mmmwwwwwddddwwwwmmmmmmwwwffffffffffffffflllllll",
            "   mmmmwwwddddddwwwwmmmmmmwwffffffffffffffflllllllllll",
            "  mmmmmmwwwddddwwwwmmwwmmwwwffffffffffffffllllllllllllll",
            " mmdmmmmmwwwwwwwwmmwwwmmmwwwffffffffffffffllllllllllllllll",
            "wwmmmmmmmmmmmmmwwwwwffffwwwffffffffffffffmmmmmmmmllllllllll",
            " wwwwwwwwwwwwwwwffffffwwwddddddddddddddddmmmmmmmmmmmmllllllll",
            "    ffffffffffffffffmmmdddddlddddddddddddmmmmmmmmmmmmmmmllllll",
            "          ffffffffmmmddddddllllldddddddddmmmmmmmmmmmmmmmmmmllll",
            "                    ddddddddlllllllddddddmmmmmmmmmmmmmmmmmmmmfff",
            "                   mdddddddddmmllllldddddmmmmmmmmmmmmmmmmmmmmmfff",
            "                  mmmmmmddddddmmmmmmdddddmmmwwwwwwwwwwwwwmmmmmmmff",
            "                 mmmmmm    ddmmmmmmddddddmwwwwwwwwwwwwwwwwwmmmmmmf",
            "                mmmmmm      mmmmmmdddddddwwwwffwwwwwwwwwwwwwmmmmmf",
            "            llllllmmm     lmmmmmm    dffwwwwfwwwfffwwwwwwwwwwmmmmm",
            "           llllfffff  llllllmmmm    ffffwwwffwwffmmmwwwwwwwwwmmmmm",
            "          llllfffff lllllllfffm    fffffwwwwfwwwffffwwwwwfwwwmmmm",
            "                   llllllffff     ffffffwwwwffwwwwwwwwwffwwwmmmm",
            "                        ffff             wwwwwfffffffffwwwwmmmm",
            "                                          wwwwwwwwwwwwwwwwmmmm",
            "                                            fwwwwwwwwwwffff",
            "                                               fffffffff",
        ];
        int width = 0;
        foreach (string line in art)
            width = Math.Max(width, line.Length);

        if (AnsiConsole.Profile.Width < width)
            return false;

        AnsiConsole.WriteLine();

        // Forest, mid and light shades per palette. The wave starts and ends on the mascot's greens.
        string[][] palettes =
        [
            ["#02894e", "#1bbe49", "#6ad24c"],
            ["#0a7f86", "#1bb5b0", "#6fd8cf"],
            ["#1f5fa8", "#3b8be0", "#8ab8f0"],
            ["#5b3aa0", "#8a5ad8", "#b896f0"],
            ["#9a2f7a", "#d84a9e", "#f08ac4"],
            ["#a8521a", "#e8862a", "#f5b45a"],
            ["#02894e", "#1bbe49", "#6ad24c"],
        ];

        Profile profile = AnsiConsole.Profile;
        if (!profile.Capabilities.Ansi || !profile.Capabilities.Interactive)
        {
            AnsiConsole.Write(Render(art, shades, palettes, (_, _) => 0, (_, _) => true));
            AnsiConsole.WriteLine();
            return true;
        }

        const int revealFrames = 8;
        const int frameDelayMs = 45;

        // Each cell appears on a random reveal frame. A fixed seed keeps the animation the same each time.
        Random random = new(7);
        int[][] revealFrame = new int[art.Length][];
        for (int row = 0; row < art.Length; row++)
        {
            revealFrame[row] = new int[art[row].Length];
            for (int column = 0; column < art[row].Length; column++)
                revealFrame[row][column] = random.Next(revealFrames);
        }

        // A column moves to the next palette every two frames and starts one frame later than the
        // column four places to its left, so the colors travel from the head to the tail.
        int lastPalette = palettes.Length - 1;
        int waveFrames = 2 * lastPalette + (width - 1) / 4 + 1;

        await AnsiConsole.Live(Render(art, shades, palettes, (_, _) => 0, (_, _) => false))
            .StartAsync(async context =>
            {
                for (int frame = 0; frame < revealFrames; frame++)
                {
                    int shown = frame;
                    context.UpdateTarget(Render(art, shades, palettes, (_, _) => 0, (row, column) => revealFrame[row][column] <= shown));
                    await Task.Delay(frameDelayMs);
                }

                for (int frame = 0; frame <= waveFrames; frame++)
                {
                    int current = frame;
                    context.UpdateTarget(Render(art, shades, palettes, (_, column) => Math.Clamp((current - column / 4) / 2, 0, lastPalette), (_, _) => true));
                    await Task.Delay(frameDelayMs);
                }
            });

        AnsiConsole.WriteLine();
        return true;
    }

    // Builds one frame of the chameleon. paletteAt picks the palette of each cell by row and column, and
    // visible decides whether a cell is drawn yet; a hidden cell is drawn as a space so the frame keeps
    // its shape. Runs of cells with the same color share one markup tag.
    private static Markup Render(
        string[] art,
        string[] shades,
        string[][] palettes,
        Func<int, int, int> paletteAt,
        Func<int, int, bool> visible)
    {
        StringBuilder sb = new(art.Length * 512);

        for (int row = 0; row < art.Length; row++)
        {
            string line = art[row];
            string shade = shades[row];

            string? openColor = null;
            for (int column = 0; column < line.Length; column++)
            {
                bool drawn = shade[column] != ' ' && visible(row, column);

                string? color = !drawn ? null : shade[column] switch
                {
                    'd' => "#01281d",
                    'f' => palettes[paletteAt(row, column)][0],
                    'm' => palettes[paletteAt(row, column)][1],
                    'l' => palettes[paletteAt(row, column)][2],
                    _ => "#fefefe",
                };

                if (!string.Equals(color, openColor, StringComparison.Ordinal))
                {
                    if (openColor is not null)
                        sb.Append("[/]");
                    if (color is not null)
                        sb.Append('[').Append(color).Append(']');
                    openColor = color;
                }

                sb.Append(drawn ? Markup.Escape(line[column].ToString()) : " ");
            }

            if (openColor is not null)
                sb.Append("[/]");

            if (row < art.Length - 1)
                sb.Append('\n');
        }

        return new Markup(sb.ToString());
    }
}
