using System.Text;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.ScriptParser;
using Kahuna.Shared.KeyValue;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The parse cache keys a script's syntax tree by a hash the caller sends. The cache used to store an entry
/// under whatever key the caller supplied, so a caller that sent one script under another script's hash left
/// the first script's tree where the second script's callers would find and run it. An entry is now stored
/// only under the hash the server computes from the bytes it parsed.
/// </summary>
public class TestScriptParseCache : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestScriptParseCache(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Theory, CombinatorialData]
    public async Task TestOneScriptCannotBeStoredUnderAnotherScriptsHash([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            // Made unique per run so a shared process-wide cache carries nothing over between runs.
            string tag = Guid.NewGuid().ToString("N")[..8];
            string scriptA = $"RETURN 'A-{tag}'";
            string scriptB = $"RETURN 'B-{tag}'";

            byte[] bytesA = Encoding.UTF8.GetBytes(scriptA);
            byte[] bytesB = Encoding.UTF8.GetBytes(scriptB);

            string hashOfB = Blake3.Hasher.Hash(bytesB).ToString();

            // The hostile call: script A sent under script B's hash.
            KeyValueTransactionResult poisoned = await kahuna1.TryExecuteTransactionScript(bytesA, hashOfB, null);

            Assert.Equal(KeyValueResponseType.Get, poisoned.Type);
            Assert.Equal($"A-{tag}", Encoding.UTF8.GetString(poisoned.Value ?? []));

            // The honest call that follows must still run script B.
            KeyValueTransactionResult honest = await kahuna1.TryExecuteTransactionScript(bytesB, hashOfB, null);

            Assert.Equal(KeyValueResponseType.Get, honest.Type);
            Assert.Equal($"B-{tag}", Encoding.UTF8.GetString(honest.Value ?? []));

            // Script A's tree is stored under script A's own hash, never under B's.
            string hashOfA = Blake3.Hasher.Hash(bytesA).ToString();

            Assert.True(scriptParser.Cache.ContainsKey(hashOfA), "script A must be cached under its own hash");
            Assert.True(scriptParser.Cache.ContainsKey(hashOfB), "script B must be cached under its own hash");
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// A caller that sends the matching hash must still hit the cache, so the change costs a correct client
    /// nothing beyond one hash per distinct script.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestCorrectHashStillHitsTheCache([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna _, IKahuna _) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);

        try
        {
            string tag = Guid.NewGuid().ToString("N")[..8];
            byte[] script = Encoding.UTF8.GetBytes($"RETURN 'cached-{tag}'");
            string hash = Blake3.Hasher.Hash(script).ToString();

            Assert.False(scriptParser.Cache.ContainsKey(hash));

            KeyValueTransactionResult first = await kahuna1.TryExecuteTransactionScript(script, hash, null);
            Assert.Equal(KeyValueResponseType.Get, first.Type);

            Assert.True(scriptParser.Cache.TryGetValue(hash, out ScriptCacheEntry? entry), "the script must be cached under its own hash");

            // A second call with the same hash must reuse the very same tree rather than parse again.
            KeyValueTransactionResult second = await kahuna1.TryExecuteTransactionScript(script, hash, null);
            Assert.Equal(KeyValueResponseType.Get, second.Type);
            Assert.Equal($"cached-{tag}", Encoding.UTF8.GetString(second.Value ?? []));

            Assert.True(scriptParser.Cache.TryGetValue(hash, out ScriptCacheEntry? again));
            Assert.Same(entry!.Ast, again!.Ast);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
