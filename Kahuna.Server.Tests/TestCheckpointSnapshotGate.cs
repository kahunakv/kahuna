using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// The background writer's checkpoint gate must persist the canonical transaction record and prepared-intent
/// snapshots — not only completion receipts — before a partition's WAL checkpoint advances the retention floor.
/// Otherwise a checkpoint could compact away the only durable copy of a committed decision or an unresolved
/// intent, and a cold restart could not reconstruct it.
/// </summary>
public sealed class TestCheckpointSnapshotGate : IDisposable
{
    private const int PartitionId = 1;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-ckpt-" + Guid.NewGuid().ToString("N"));

    public TestCheckpointSnapshotGate() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static RaftManager CreateRaft(string name) => new(
        new RaftConfiguration { NodeName = name, NodeId = 1, Host = "localhost", Port = 0, InitialPartitions = 1, EnableQuiescence = false },
        new StaticDiscovery([]), new InMemoryWAL(NullLogger<IRaft>.Instance), new InMemoryCommunication(),
        new HybridLogicalClock(), NullLogger<IRaft>.Instance);

    private static KahunaConfiguration Config() => ConfigurationValidator.Validate(new()
    {
        LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory"
    });

    private static PreparedIntent PendingIntent(string key) => new(
        Ts(1000), 1, key, ManifestHash: 42, RecordAnchorKey: "anchor", CommitTimestamp: Ts(1100),
        State: KeyValueState.Set, Value: [1, 2], Bucket: null, Revision: 3, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: 2, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
        Resolution: PreparedIntentResolution.Pending);

    private static void SeedTerminalRecord(TransactionRecordStore records)
    {
        List<TransactionParticipantRef> manifest = [new("anchor", KeyValueDurability.Persistent)];
        records.Apply(new InitializeTransactionCommand(Ts(1000), 1, "coord", "anchor", Ts(1100), Ts(9000), 42, manifest, Ts(1000), Ts(1000)));
        records.Apply(new CommitTransactionCommand(Ts(1000), 1, 42, Ts(1000), Ts(1100)));
    }

    private static BackgroundWriterActor SpawnWriter(ActorSystem actorSystem, string name, TransactionRecordStore records, PreparedIntentStore intents)
    {
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> bg = actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
            name, CreateRaft(name), new MemoryPersistenceBackend(),
            null!, null!, records, intents,
            Config(), NullLogger<IKahuna>.Instance, new FlushNotificationSink());

        return (bg.Runner.Actor as BackgroundWriterActor)!;
    }

    [Fact]
    public void Gate_PersistsRecordAndIntentSnapshots_ThatSurviveColdRestart()
    {
        TransactionRecordStore records = new(dir, "rev", null);
        records.AttachAnchorResolver(_ => (PartitionId, 0L));
        PreparedIntentStore intents = new(dir, "rev", null);
        intents.AttachPartitionResolver(_ => PartitionId);

        SeedTerminalRecord(records);

        // A pending (undecided) intent and a committed-but-unsettled intent — both must survive compaction.
        intents.Apply(new PrepareIntentCommand(PendingIntent("acct/pending")));
        intents.Apply(new PrepareIntentCommand(PendingIntent("acct/committed")));
        intents.Apply(new ResolveIntentCommand(Ts(1000), 1, "acct/committed", Commit: true));

        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        BackgroundWriterActor writer = SpawnWriter(actorSystem, "bg-ckpt", records, intents);

        // The gate makes the record and both intents durable before a checkpoint may advance the WAL floor.
        Assert.True(writer.TryCaptureCheckpointSnapshots(PartitionId));

        // Cold restart: fresh stores at the same path reconstruct the terminal record and both intents from the
        // snapshots the gate wrote — so a checkpoint that compacted the delta log would lose nothing.
        TransactionRecordStore recordsAfter = new(dir, "rev", null);
        PreparedIntentStore intentsAfter = new(dir, "rev", null);

        Assert.Equal(TransactionDecision.Commit, recordsAfter.Get(Ts(1000), 1)!.Decision);

        PreparedIntent? pending = intentsAfter.Get("acct/pending");
        Assert.NotNull(pending);
        Assert.Equal(PreparedIntentResolution.Pending, pending!.Resolution);

        PreparedIntent? committed = intentsAfter.Get("acct/committed");
        Assert.NotNull(committed);
        Assert.Equal(PreparedIntentResolution.Committed, committed!.Resolution);
    }

    [Fact]
    public void Gate_ReturnsFalse_WhenARecordSnapshotCannotBeWritten()
    {
        // Point the record store's storage at a path that is a file, not a directory, so its snapshot write fails.
        string filePath = Path.Combine(dir, "not-a-directory");
        File.WriteAllText(filePath, "occupied");

        TransactionRecordStore records = new(filePath, "rev", null);
        records.AttachAnchorResolver(_ => (PartitionId, 0L));
        SeedTerminalRecord(records);

        PreparedIntentStore intents = new(dir, "rev", null);
        intents.AttachPartitionResolver(_ => PartitionId);

        using IDisposable lifetime = TestActorSystemLifetime.Create(out ActorSystem actorSystem);
        BackgroundWriterActor writer = SpawnWriter(actorSystem, "bg-ckpt-fail", records, intents);

        // The record snapshot cannot be made durable, so the gate refuses to advance the checkpoint — the entries
        // stay replayable rather than being compacted away with no durable copy.
        Assert.False(writer.TryCaptureCheckpointSnapshots(PartitionId));
    }
}
