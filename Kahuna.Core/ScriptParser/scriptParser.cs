
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Microsoft.IO;

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents the primary logic for parsing scripts, either generating a new parsed result
/// or retrieving a previously cached result, using the provided configuration and logger.
/// </summary>
internal sealed partial class scriptParser
{
    private static readonly RecyclableMemoryStreamManager manager = new();

    private readonly KahunaConfiguration configuration;

    private readonly ILogger<IKahuna> logger;
    
    public static ConcurrentDictionary<string, ScriptCacheEntry> Cache { get; } = new();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public scriptParser(KahunaConfiguration configuration, ILogger<IKahuna> logger) : base(null)
    {
        this.configuration = configuration;
        this.logger = logger;
    }

    /// <summary>
    /// Parses or retrieves a script from the global cache
    /// </summary>
    /// <param name="inputBuffer"></param>
    /// <param name="hash"></param>
    /// <returns></returns>
    /// <exception cref="KahunaScriptException"></exception>
    public NodeAst Parse(ReadOnlySpan<byte> inputBuffer, string? hash)
    {
        if (!string.IsNullOrEmpty(hash) && Cache.TryGetValue(hash, out ScriptCacheEntry? cached))
        {
            cached.ExpiresAt = Environment.TickCount64 + (long)configuration.ScriptCacheExpiration.TotalMilliseconds;

            logger.LogScriptRetrievedFromCache(hash);

            return cached.Ast;
        }
        
        using RecyclableMemoryStream stream = manager.GetStream(inputBuffer); 
        
        scriptScanner scanner = new(stream);

        Scanner = scanner;
        
        Parse();

        if (!string.IsNullOrEmpty(scanner.YYError))
        {
            string message = scanner.YYError;
            int line = scanner.YYErrorLine;
            int column = scanner.YYErrorColumn;

            if (line > 0 && column > 0)
            {
                message += $" at line {line}, column {column}";

                if (!string.IsNullOrEmpty(scanner.YYErrorToken))
                    message += $" near '{scanner.YYErrorToken}'";
            }

            throw new KahunaScriptException(message, line, column);
        }

        NodeAst? root = CurrentSemanticValue.n;

        if (!string.IsNullOrEmpty(hash) && Cache.Count < configuration.ScriptCacheMaxEntries)
        {
            logger.LogScriptAddedToCache(hash);

            Cache.TryAdd(hash, new(hash, root, Environment.TickCount64 + (long)configuration.ScriptCacheExpiration.TotalMilliseconds));
        }

        return root;
    }
}
