
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies actor-inbox admission control: a hot-key flood of ordinary (user) requests trips the
/// bounded inbox and is rejected with <see cref="ActorBusyException"/> (mapped to MustRetry at the
/// manager boundary), while priority control messages — completions, cache-coherence, maintenance —
/// are exempt from the bound and always admitted, so an in-flight read completion is never stranded.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestKeyValueInboxAdmission
{
    [Fact]
    public async Task HotKeyFlood_UserRequestsBackpressureToMustRetry_ControlCompletionsNeverRejected()
    {
        (ActorSystem actorSystem, RaftManager raft, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor) =
            SpawnBoundedActor(maxInboxSize: 4);

        using (actorSystem)
        {
            List<Task<KeyValueResponse?>> userTasks = [];
            List<Task<KeyValueResponse?>> controlTasks = [];

            // Enqueue far more ordinary requests than the bound + drain can absorb, interleaving control
            // messages so they arrive while the ordinary inbox is saturated.
            for (int i = 0; i < 6000; i++)
            {
                userTasks.Add(actor.Ask(MakeExists("hot/" + i))!);
                if (i % 200 == 0)
                    controlTasks.Add(actor.Ask(new KeyValueRequest(KeyValueRequestType.ResumeRead))!);
            }

            int busyUser = await CountActorBusy(userTasks);
            int busyControl = await CountActorBusy(controlTasks);

            Assert.True(busyUser > 0, "a flood past the inbox bound must reject ordinary requests with ActorBusyException");
            Assert.Equal(0, busyControl);
        }

        ((FairReadScheduler)raft.ReadScheduler).Stop();
    }

    [Fact]
    public async Task BoundedInbox_DoesNotStrandInFlightReadOrProposal()
    {
        (ActorSystem actorSystem, RaftManager raft, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor) =
            SpawnBoundedActor(maxInboxSize: 4);

        using (actorSystem)
        {
            // Saturate the ordinary inbox.
            List<Task<KeyValueResponse?>> flood = [];
            for (int i = 0; i < 5000; i++)
                flood.Add(actor.Ask(MakeExists("hot/" + i))!);

            // A read completion (ResumeRead carrying a continuation) is a control message — it must be
            // admitted ahead of the ordinary backlog and resolve its waiter rather than being stranded.
            TaskCompletionSource<KeyValueResponse?> readPromise = new();
            PointReadContinuation cont = new("absent/key", KeyValueResponseType.Get, readPromise);
            _ = actor.Ask(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont })!;

            Task completed = await Task.WhenAny(readPromise.Task, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.Same(readPromise.Task, completed);
            Assert.True(readPromise.Task.IsCompletedSuccessfully);

            await CountActorBusy(flood); // observe the flood so faulted tasks are not left unobserved
        }

        ((FairReadScheduler)raft.ReadScheduler).Stop();
    }

    [Fact]
    public void ControlPredicate_ClassifiesInternalMessages_NotUserRequests()
    {
        // Completions + internal coherence/maintenance are control (exempt from the bound)...
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.ResumeRead)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.CompleteProposal)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.ReleaseProposal)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.InvalidateOrApply)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.FlushAck)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.Collect)));
        Assert.True(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.GetSafeTimestamp)));

        // ...while user-facing ops are ordinary (subject to the bound).
        Assert.False(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.TrySet)));
        Assert.False(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.TryGet)));
        Assert.False(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.TryExists)));
        Assert.False(KeyValuesManager.IsControlRequest(new KeyValueRequest(KeyValueRequestType.TryCommitMutations)));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static async Task<int> CountActorBusy(IEnumerable<Task<KeyValueResponse?>> tasks)
    {
        int busy = 0;
        foreach (Task<KeyValueResponse?> task in tasks)
        {
            try
            {
                await task;
            }
            catch (ActorBusyException)
            {
                busy++;
            }
            catch
            {
                // Other outcomes are irrelevant to the admission count.
            }
        }
        return busy;
    }

    private static KeyValueRequest MakeExists(string key) => new(
        KeyValueRequestType.TryExists, HLCTimestamp.Zero, HLCTimestamp.Zero, key,
        null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral, 0, 0, null);

    private static (ActorSystem, RaftManager, IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>) SpawnBoundedActor(int maxInboxSize)
    {
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "inbox-admission-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        ((FairReadScheduler)raft.ReadScheduler).Start();

        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });

        ActorSystem actorSystem = new();
        IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor =
            actorSystem.SpawnWithOptions<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "inbox-admission-actor",
                new ActorRunnerOptions<KeyValueRequest>
                {
                    MaxInboxSize = maxInboxSize,
                    IsControlMessage = KeyValuesManager.IsControlRequest
                },
                null!,  // backgroundWriter — untouched by TryExists/ResumeRead
                null!,  // proposalRouter
                new MemoryPersistenceBackend(),
                raft,
                new KeySpaceRegistry(),
                new RangeMapStore(raft, null, null, logger),
                config,
                logger
            );

        return (actorSystem, raft, actor);
    }
}
