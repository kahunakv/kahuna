
using Kommander;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end tests for the script-language no-revision modifier (<c>SET key value NOREV</c>).
///
/// These exercise the full path the lexer/grammar/executor wiring enables: a <c>NOREV</c> token is
/// lexed, parsed into a <c>SetNoRev</c> flag node, mapped to <see cref="KeyValueFlags.SetNoRevision"/>,
/// and honored by the in-memory and persistence write sites. The modifier suppresses historical
/// revision archiving while the monotonic revision counter and conditional-set semantics keep working.
/// </summary>
[Collection("ClusterTests")]
public class TestKeyValueScriptNoRevision : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeyValueScriptNoRevision(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>
    /// A <c>NOREV</c> write keeps only the current value: the revision counter still advances and the
    /// point GET reflects the latest write, but the revision superseded by a NOREV write is archived
    /// nowhere (neither the in-memory archive nor the backend) and a by-revision read returns
    /// DoesNotExist. A revision written normally remains retrievable.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestScriptNoRevisionSuppressesHistory([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        try
        {
            // Two normal writes then two no-revision writes. The normal write at revision 1 archives
            // revision 0 in the in-memory history; the NOREV writes at 2 and 3 archive nothing.
            foreach ((string value, string suffix) in new[] { ("v0", ""), ("v1", ""), ("v2", " NOREV"), ("v3", " NOREV") })
            {
                KeyValueTransactionResult set = await kahuna1.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"SET nrk '{value}'{suffix}"), null, null);
                Assert.Equal(KeyValueResponseType.Set, set.Type);
            }

            // The counter advanced across the NOREV writes — the latest revision is 3, not pinned at 1.
            KeyValueTransactionResult get = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("GET nrk"), null, null);
            Assert.Equal(KeyValueResponseType.Get, get.Type);
            Assert.Equal(3, get.Revision);
            Assert.Equal("v3"u8.ToArray(), get.Value);

            // Latest read via the API agrees.
            (KeyValueResponseType latestType, ReadOnlyKeyValueEntry? latest) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, "nrk", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, latestType);
            Assert.NotNull(latest);
            Assert.Equal(3, latest!.Revision);

            // Revision 0 was archived by the normal write at revision 1 — still retrievable.
            (KeyValueResponseType rev0Type, ReadOnlyKeyValueEntry? rev0) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, "nrk", 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Get, rev0Type);
            Assert.NotNull(rev0);
            Assert.Equal("v0"u8.ToArray(), rev0!.Value);

            // Revision 2 was superseded by the NOREV write at revision 3, so it was never archived in
            // memory, and the NOREV write at revision 2 never wrote a backend revision row either —
            // the by-revision read finds nothing.
            (KeyValueResponseType rev2Type, _) = await kahuna1.LocateAndTryGetValue(
                HLCTimestamp.Zero, "nrk", 2, HLCTimestamp.Zero, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.DoesNotExist, rev2Type);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }

    /// <summary>
    /// <c>NOREV</c> is a modifier, freely OR-combinable with the conditional-set flags. A combined
    /// <c>NOREV CMPREV</c> write succeeds when the revision matches and is rejected when it does not —
    /// proving the conditional gate still fires with the flag present.
    /// </summary>
    [Theory, CombinatorialData]
    public async Task TestScriptNoRevisionCombinesWithConditional([CombinatorialValues("memory")] string storage, [CombinatorialValues(1)] int partitions)
    {
        (IRaft node1, IRaft node2, IRaft node3, IKahuna kahuna1, IKahuna kahuna2, IKahuna kahuna3) =
            await AssembleThreNodeCluster(storage, partitions, raftLogger, kahunaLogger);
        try
        {
            KeyValueTransactionResult set = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("SET cck 'a'"), null, null);
            Assert.Equal(KeyValueResponseType.Set, set.Type);
            Assert.Equal(0, set.Revision);

            // NOREV combined with CMPREV against the current revision (0) — both modifiers apply.
            set = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("SET cck 'b' NOREV CMPREV 0"), null, null);
            Assert.Equal(KeyValueResponseType.Set, set.Type);
            Assert.Equal(1, set.Revision);

            // The same conditional now fails — current revision is 1, not 0 — so the write is rejected
            // even though NOREV is present.
            set = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("SET cck 'c' NOREV CMPREV 0"), null, null);
            Assert.Equal(KeyValueResponseType.NotSet, set.Type);

            KeyValueTransactionResult get = await kahuna1.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("GET cck"), null, null);
            Assert.Equal(KeyValueResponseType.Get, get.Type);
            Assert.Equal(1, get.Revision);
            Assert.Equal("b"u8.ToArray(), get.Value);
        }
        finally
        {
            await LeaveCluster(node1, node2, node3);
        }
    }
}
