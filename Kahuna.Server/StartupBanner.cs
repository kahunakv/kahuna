using System.Reflection;
using System.Text;

namespace Kahuna.Server;

/// <summary>
/// Prints the startup banner. On a terminal that renders ANSI colors and UTF-8 the banner is a
/// pixel-font "Kahuna" drawn with half-block characters and a mint-to-forest green gradient;
/// anywhere else (redirected output, NO_COLOR, TERM=dumb, a non-UTF-8 console) it falls back to
/// the plain ASCII banner.
/// </summary>
internal static class StartupBanner
{
    // Each glyph is 12 pixel rows by 8 columns. Two pixel rows are packed into one text line with
    // the upper/lower half-block characters, which keeps pixels roughly square on a terminal.
    private const int GlyphHeight = 12;

    private const int GlyphSpacing = 2;

    private const int ColoredIndent = 2;

    // Width of the widest line of the plain ASCII banner.
    private const int PlainWidth = 36;

    private static readonly string[] K =
    [
        "##....##",
        "##...##.",
        "##..##..",
        "##.##...",
        "####....",
        "###.....",
        "###.....",
        "####....",
        "##.##...",
        "##..##..",
        "##...##.",
        "##....##",
    ];

    // Lowercase glyphs sit on the same baseline; the x-height is the bottom eight pixel rows.
    private static readonly string[] LowerA =
    [
        "........",
        "........",
        "........",
        "........",
        ".######.",
        "......##",
        ".#######",
        "########",
        "##....##",
        "##....##",
        "########",
        ".#######",
    ];

    private static readonly string[] LowerH =
    [
        "##......",
        "##......",
        "##......",
        "##......",
        "##.####.",
        "########",
        "###..###",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
    ];

    private static readonly string[] LowerU =
    [
        "........",
        "........",
        "........",
        "........",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "###..###",
        ".######.",
        "..####..",
    ];

    private static readonly string[] LowerN =
    [
        "........",
        "........",
        "........",
        "........",
        "##.####.",
        "########",
        "###..###",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
        "##....##",
    ];

    private static readonly string[][] Word = [K, LowerA, LowerH, LowerU, LowerN, LowerA];

    // One color per pixel row: pale mint at the top fading to a deep forest green at the bottom.
    private static readonly (byte R, byte G, byte B)[] Gradient =
    [
        (214, 252, 214),
        (190, 247, 196),
        (165, 241, 180),
        (142, 232, 160),
        (125, 222, 146),
        (108, 208, 132),
        (92, 190, 118),
        (80, 170, 104),
        (70, 148, 91),
        (60, 126, 78),
        (50, 106, 66),
        (42, 88, 56),
    ];

    // The version text uses the middle shade of the gradient.
    private static readonly (byte R, byte G, byte B) VersionColor = (108, 208, 132);

    public static void Print()
    {
        string version = "v" + GetVersion();

        if (TryGetColorMode(out bool trueColor))
        {
            Console.Write(RenderColored(trueColor));

            StringBuilder sb = new(64);
            sb.Append(' ', Math.Max(0, ColoredIndent + ColoredWidth() - version.Length));
            AppendColor(sb, VersionColor, foreground: true, trueColor);
            sb.Append(version).Append("\e[0m");
            Console.WriteLine(sb.ToString());
        }
        else
        {
            PrintPlain();
            Console.WriteLine(version.PadLeft(PlainWidth));
        }

        Console.WriteLine();
    }

    /// <summary>
    /// The product version from the assembly's informational version (the csproj
    /// <c>&lt;Version&gt;</c>), without the source-revision suffix the SDK appends after '+'.
    /// </summary>
    private static string GetVersion()
    {
        Assembly assembly = typeof(StartupBanner).Assembly;

        string? informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        if (!string.IsNullOrEmpty(informational))
        {
            int plus = informational.IndexOf('+');
            return plus >= 0 ? informational[..plus] : informational;
        }

        return assembly.GetName().Version?.ToString(3) ?? "unknown";
    }

    private static int ColoredWidth()
    {
        int width = GlyphSpacing * (Word.Length - 1);
        for (int g = 0; g < Word.Length; g++)
            width += Word[g][0].Length;

        return width;
    }

    private static void PrintPlain()
    {
        Console.WriteLine("  _  __     _                         ");
        Console.WriteLine(" | |/ /__ _| |__  _   _ _ __   __ _ ");
        Console.WriteLine(" | ' / _` | '_ \\| | | | '_ \\ / _` |");
        Console.WriteLine(" | . \\ (_| | | | | |_| | | | | (_| |");
        Console.WriteLine(" |_|\\_\\__,_|_| |_|\\__,_|_| |_|\\__,_|");
    }

    private static string RenderColored(bool trueColor)
    {
        StringBuilder sb = new(4096);
        sb.AppendLine();

        for (int row = 0; row < GlyphHeight; row += 2)
        {
            (byte R, byte G, byte B) top = Gradient[row];
            (byte R, byte G, byte B) bottom = Gradient[row + 1];

            sb.Append(' ', ColoredIndent);

            for (int g = 0; g < Word.Length; g++)
            {
                string upper = Word[g][row];
                string lower = Word[g][row + 1];

                for (int x = 0; x < upper.Length; x++)
                {
                    bool hasTop = upper[x] == '#';
                    bool hasBottom = lower[x] == '#';

                    if (hasTop && hasBottom)
                    {
                        AppendColor(sb, top, foreground: true, trueColor);
                        AppendColor(sb, bottom, foreground: false, trueColor);
                        sb.Append('▀');
                    }
                    else if (hasTop)
                    {
                        AppendColor(sb, top, foreground: true, trueColor);
                        sb.Append("\e[49m▀");
                    }
                    else if (hasBottom)
                    {
                        AppendColor(sb, bottom, foreground: true, trueColor);
                        sb.Append("\e[49m▄");
                    }
                    else
                    {
                        sb.Append("\e[49m ");
                    }
                }

                if (g < Word.Length - 1)
                    sb.Append("\e[49m").Append(' ', GlyphSpacing);
            }

            sb.Append("\e[0m").AppendLine();
        }

        return sb.ToString();
    }

    private static void AppendColor(StringBuilder sb, (byte R, byte G, byte B) color, bool foreground, bool trueColor)
    {
        sb.Append("\e[").Append(foreground ? "38" : "48");

        if (trueColor)
        {
            sb.Append(";2;").Append(color.R).Append(';').Append(color.G).Append(';').Append(color.B).Append('m');
            return;
        }

        // Nearest entry in the 6x6x6 cube of the xterm 256-color palette.
        int r = (color.R * 5 + 127) / 255;
        int g = (color.G * 5 + 127) / 255;
        int b = (color.B * 5 + 127) / 255;
        sb.Append(";5;").Append(16 + 36 * r + 6 * g + b).Append('m');
    }

    /// <summary>
    /// Decides whether stdout is a terminal that renders ANSI colors and the half-block characters,
    /// and whether it accepts 24-bit color or only the 256-color palette.
    /// </summary>
    private static bool TryGetColorMode(out bool trueColor)
    {
        trueColor = false;

        if (Console.IsOutputRedirected)
            return false;

        // https://no-color.org: any non-empty value disables color.
        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("NO_COLOR")))
            return false;

        if (Console.OutputEncoding.CodePage != Encoding.UTF8.CodePage)
            return false;

        string? term = Environment.GetEnvironmentVariable("TERM");
        if (string.Equals(term, "dumb", StringComparison.Ordinal))
            return false;

        bool windowsTerminal = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WT_SESSION"));

        // The legacy Windows console only interprets escape sequences when virtual-terminal
        // processing is enabled, so on Windows require a host that is known to handle them.
        if (OperatingSystem.IsWindows() && !windowsTerminal && string.IsNullOrEmpty(term))
            return false;

        if (!OperatingSystem.IsWindows() && string.IsNullOrEmpty(term))
            return false;

        string? colorTerm = Environment.GetEnvironmentVariable("COLORTERM");
        trueColor = windowsTerminal
                    || string.Equals(colorTerm, "truecolor", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(colorTerm, "24bit", StringComparison.OrdinalIgnoreCase);

        return true;
    }
}
