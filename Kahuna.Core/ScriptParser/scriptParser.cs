
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

namespace Kahuna.Server.ScriptParser;

/// <summary>
/// Represents the primary logic for parsing scripts, either generating a new parsed result
/// or retrieving a previously cached result, using the provided configuration and logger.
/// </summary>
internal sealed partial class scriptParser
{
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
        
        // Decode the UTF-8 script into a string and scan it directly. Every caller supplies
        // UTF-8 (SDK, protobuf payload, REST body), so a fixed decode is correct and avoids the
        // stream + StreamReader + StringBuilder buffering the stream-based scanner would use.
        scriptScanner scanner = new();
        scanner.SetSource(Encoding.UTF8.GetString(inputBuffer), 0);

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
