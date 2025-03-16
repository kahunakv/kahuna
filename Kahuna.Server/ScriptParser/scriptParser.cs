/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Entrypoint for the Script Parser
/// </summary>
internal partial class scriptParser
{
    public scriptParser() : base(null) { }

    public NodeAst Parse(string sqlStatement)
    {
        byte[] inputBuffer = Encoding.Default.GetBytes(sqlStatement);

        MemoryStream stream = new(inputBuffer);
        scriptScanner scanner = new(stream);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new Exception(scanner.YYError);

        return CurrentSemanticValue.n;
    }
}