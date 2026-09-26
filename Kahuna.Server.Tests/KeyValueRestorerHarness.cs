using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Test-only: builds a <see cref="KeyValueRestorer"/> over an in-memory backend, with the unflushed overlay and the
/// prepared-intent store it replays into exposed, so a test can replay a single log record and read what the
/// replay recorded without starting a node. The returned lifetime owns the actor system; dispose it.
/// </summary>
internal static class KeyValueRestorerHarness
{
    public static (KeyValueRestorer Restorer, UnflushedKeyValueWritesIndex Overlay, PreparedIntentStore Intents, IDisposable Lifetime)
        Build(out MemoryPersistenceBackend backend)
    {
        IDisposable lifetime = TestActorSystemLifetime.Create(out Nixie.ActorSystem actorSystem);

        backend = new MemoryPersistenceBackend();
        UnflushedKeyValueWritesIndex overlay = new();
        UnflushedOverlayPersistenceBackend decorated = new(backend, overlay, new UnflushedLockWritesIndex());
        PreparedIntentStore intents = new();

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "byref-restore", NodeId = 1, Host = "localhost", Port = 0,
                InitialPartitions = 1, EnableQuiescence = false, PartitionExecutorPoolSize = 1
            },
            new Kommander.Discovery.StaticDiscovery([]),
            new InMemoryWAL(NullLogger<IRaft>.Instance),
            new Kommander.Communication.Memory.InMemoryCommunication(),
            new HybridLogicalClock(),
            NullLogger<IRaft>.Instance);

        Kahuna.Server.Configuration.KahunaConfiguration config =
            Kahuna.Server.Configuration.ConfigurationValidator.Validate(new()
            {
                LocksWorkers = 1, KeyValueWorkers = 1, BackgroundWriterWorkers = 1, Storage = "memory",
                CacheEntryTtl = TimeSpan.FromMinutes(5), CacheEntriesToRemove = 1000,
                MaxEntriesPerActor = 50_000, MaxBytesPerActor = 256L * 1024 * 1024, CollectBatchMax = 1000,
                RevisionRetention = 16, DirtyObjectsWriterDelay = 30_000
            });

        Nixie.IActorRef<BackgroundWriterActor, BackgroundWriteRequest> writer =
            actorSystem.Spawn<BackgroundWriterActor, BackgroundWriteRequest>(
                "byref-restore-bg", raft, raft.ReadScheduler, decorated,
                null!, null!, new TransactionRecordStore(), intents,
                config, NullLogger<IKahuna>.Instance, new FlushNotificationSink(), null!);

        KeyValueRestorer restorer = new(
            writer, raft, new CompletionReceiptStore(), NullLogger<IKahuna>.Instance,
            overlay, durabilityTracker: null, preparedIntentStore: intents);

        return (restorer, overlay, intents, lifetime);
    }
}
