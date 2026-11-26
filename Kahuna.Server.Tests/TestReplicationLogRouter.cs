using System.Reflection;

using Kahuna.Server.Replication;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the log-type → subsystem map the apply callbacks route through. An unrouted Kahuna
/// log type is not a loud failure — the entry is silently treated as foreign and never applied — so
/// the mapping is asserted against the constants themselves rather than trusted to review.
/// </summary>
public sealed class TestReplicationLogRouter
{
    private static IEnumerable<(string Name, string Value)> ReplicationTypeConstants() =>
        typeof(ReplicationTypes)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Select(f => (f.Name, (string)f.GetRawConstantValue()!));

    [Fact]
    public void EveryReplicationTypeIsOwnedBySomeSubsystem()
    {
        List<string> unrouted =
        [
            .. ReplicationTypeConstants()
                .Where(c => ReplicationLogRouter.OwnerOf(c.Value) == ReplicationLogOwner.None)
                .Select(c => $"{c.Name} (\"{c.Value}\")")
        ];

        Assert.True(unrouted.Count == 0,
            "These replication types have no owning subsystem, so committed entries carrying them " +
            "would be silently ignored on apply: " + string.Join(", ", unrouted));
    }

    [Fact]
    public void LockTypeRoutesToLocksAndEverythingElseToKeyValues()
    {
        Assert.Equal(ReplicationLogOwner.Locks, ReplicationLogRouter.OwnerOf(ReplicationTypes.Locks));

        foreach ((string name, string value) in ReplicationTypeConstants().Where(c => c.Value != ReplicationTypes.Locks))
            Assert.True(ReplicationLogRouter.OwnerOf(value) == ReplicationLogOwner.KeyValues,
                $"{name} should be applied by the key-value layer");
    }

    /// <summary>Each reader of the log other than the router decided every replication type: it reads entries of the
    /// type or skips them on purpose. A type that a reader does not know is skipped with no error.</summary>
    [Fact]
    public void EveryReaderOfTheLog_DecidedEveryReplicationType()
    {
        List<string> undecided =
        [
            .. from reader in ReplicationLogReaders.All
               from constant in ReplicationTypeConstants()
               where reader.UseOf(constant.Value) == LogTypeUse.Unknown
               select $"{reader.Name}: {constant.Name} (\"{constant.Value}\")"
        ];

        Assert.True(undecided.Count == 0, "These readers have no decision for these replication types: " + string.Join(", ", undecided));
    }

    [Fact]
    public void TheOnePhaseBundleEntry_IsReadByEveryReaderThatFollowsIntents()
    {
        foreach (Func<string, LogTypeUse> reader in new Func<string, LogTypeUse>[]
                 {
                     ReplicationLogReaders.OrderedApplyAwaiter, ReplicationLogReaders.SubmissionKeys, ReplicationLogReaders.WatchReplayer,
                     ReplicationLogReaders.RestoreEngine, ReplicationLogReaders.IntentFrontier, ReplicationLogReaders.WriteMetrics,
                     ReplicationLogReaders.ApplyTotals
                 })
            Assert.Equal(LogTypeUse.Read, reader(ReplicationTypes.OnePhaseBundle));
    }

    /// <summary>The list that a node answers when a leader asks which log types it applies names every type that the
    /// router gives to a subsystem, and only those.</summary>
    [Fact]
    public void TheAppliedLogTypes_AreTheTypesTheRouterApplies()
    {
        string[] routed = [.. ReplicationTypeConstants().Select(c => c.Value).Where(v => ReplicationLogRouter.OwnerOf(v) != ReplicationLogOwner.None).Order(StringComparer.Ordinal)];

        Assert.Equal(routed, Kahuna.Server.KeyValues.Writes.OnePhaseBundlePeerGuard.AppliedLogTypes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void ForeignLogTypesAreNotApplied()
    {
        // Kommander's own system entries share the partition log; they must pass through untouched
        // rather than being handed to a Kahuna subsystem.
        Assert.Equal(ReplicationLogOwner.None, ReplicationLogRouter.OwnerOf("raft-system"));
        Assert.Equal(ReplicationLogOwner.None, ReplicationLogRouter.OwnerOf(""));

        // RaftLog.LogType is nullable, and an entry without one is foreign by definition.
        Assert.Equal(ReplicationLogOwner.None, ReplicationLogRouter.OwnerOf(null));

        // Type matching is ordinal: a case variant is a different (unknown) type, never a Kahuna one.
        Assert.Equal(ReplicationLogOwner.None, ReplicationLogRouter.OwnerOf("KV"));
    }
}
