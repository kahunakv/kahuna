
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Entrypoint for the Script Parser
/// </summary>
internal partial class scriptParser
{
    private static readonly ConcurrentDictionary<string, NodeAst> _cache = new();
    
    public scriptParser() : base(null) { }

    public NodeAst Parse(byte[] inputBuffer, string? hash)
    {
        if (!string.IsNullOrEmpty(hash) && _cache.TryGetValue(hash, out NodeAst? cached))
        {
            Console.WriteLine("Retrieved from cache {0}", hash);
            return cached;
        }

        MemoryStream stream = new(inputBuffer);
        scriptScanner scanner = new(stream);

        Scanner = scanner;

        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
            throw new Exception(scanner.YYError);

        NodeAst? root = CurrentSemanticValue.n;

        if (!string.IsNullOrEmpty(hash))
        {
            Console.WriteLine("Added to cache {0}", hash);
            _cache.TryAdd(hash, root);
        }

        return root;
    }
}