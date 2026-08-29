
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// A lock requested with no expiry is session-owned: it carries no clock deadline and is released by its
/// owner's transaction cleanup. When that owner vanishes before cleanup runs — a client that dies between
/// planting and finalize, a session lost with its node — nothing used to age the lock out, and the key
/// stayed unservable to snapshot scans for the life of the process. The liveness ceiling bounds that: past
/// the span the session machinery already bounds itself by, no legitimate session can still own the lock,
/// so it is dropped.
///
/// <para>These tests fix the boundary in both directions. A lock that a live session could still hold must
/// never be taken from it — that is the failure mode this change must not produce — while an orphaned one
/// must actually die, be counted, and name its owner.</para>
/// </summary>
public sealed class TestSessionOwnedIntentCeiling : RaftTrackingTest
{
    private const int CeilingMs = 60_000;

    private static HLCTimestamp At(long milliseconds) => new(1, milliseconds, 0);

    // ── the policy itself ────────────────────────────────────────────────────────────

    /// <summary>
    /// A leased intent keeps the deadline semantics it always had. The ceiling arm must not reach it: a
    /// lease is the owner's explicit statement of how long the lock lives, in either direction.
    /// </summary>
    [Fact]
    public void LeasedIntent_LivesUntilItsDeadline_WhateverItsAge()
    {
        KeyValueWriteIntent intent = new()
        {
            TransactionId = At(0),
            AcquiredAt = At(0),
            Expires = At(500_000)
        };

        Assert.True(KeyValueWriteIntentLease.IsLive(intent, At(400_000), CeilingMs));
        Assert.False(KeyValueWriteIntentLease.IsLive(intent, At(500_001), CeilingMs));
    }

    /// <summary>
    /// A prepared intent is exempt at any age. Its fate belongs to the decision machinery, which resolves
    /// it against the canonical transaction record; expiring one that later commits would throw away the
    /// only route to a value that is already committed.
    /// </summary>
    [Fact]
    public void PreparedSessionOwnedIntent_NeverCeilingExpires()
    {
        KeyValueWriteIntent intent = new()
        {
            TransactionId = At(0),
            AcquiredAt = At(0),
            Expires = HLCTimestamp.Zero,
            CommitTimestamp = At(10)
        };

        Assert.True(KeyValueWriteIntentLease.IsLive(intent, At(CeilingMs * 100), CeilingMs));
    }

    /// <summary>
    /// The wedge shape: an un-prepared session-owned intent. It stays live for the whole ceiling — a live
    /// transaction must never lose its lock — and dies once no session that could own it can still exist.
    /// </summary>
    [Fact]
    public void UnpreparedSessionOwnedIntent_LivesToTheCeilingThenDies()
    {
        KeyValueWriteIntent intent = new()
        {
            TransactionId = At(0),
            AcquiredAt = At(0),
            Expires = HLCTimestamp.Zero
        };

        Assert.True(KeyValueWriteIntentLease.IsLive(intent, At(CeilingMs - 1), CeilingMs));
        Assert.False(KeyValueWriteIntentLease.IsLive(intent, At(CeilingMs), CeilingMs));
    }

    /// <summary>
    /// The age is measured from the plant stamp, not from the transaction id. A long-running session that
    /// takes a lock late in its life gets the full ceiling from the moment it took it, rather than a
    /// window already partly spent.
    /// </summary>
    [Fact]
    public void Age_IsMeasuredFromThePlantStamp_NotFromTheTransactionStart()
    {
        KeyValueWriteIntent intent = new()
        {
            TransactionId = At(0),
            AcquiredAt = At(50_000),
            Expires = HLCTimestamp.Zero
        };

        Assert.True(KeyValueWriteIntentLease.IsLive(intent, At(100_000), CeilingMs));
    }

    /// <summary>
    /// An intent with no plant stamp still ages out, anchored on its transaction id. Nothing plants an
    /// unstamped intent today, so this is the defensive arm: an unanchored intent would be immortal again,
    /// which is the exact defect the ceiling exists to remove.
    /// </summary>
    [Fact]
    public void IntentWithoutPlantStamp_AgesFromItsTransactionId()
    {
        KeyValueWriteIntent intent = new()
        {
            TransactionId = At(1_000),
            AcquiredAt = HLCTimestamp.Zero,
            Expires = HLCTimestamp.Zero
        };

        Assert.True(KeyValueWriteIntentLease.IsLive(intent, At(1_000 + CeilingMs - 1), CeilingMs));
        Assert.False(KeyValueWriteIntentLease.IsLive(intent, At(1_000 + CeilingMs), CeilingMs));
    }

    /// <summary>
    /// A zero-lease range lock has the same immortality shape and gets the same ceiling, anchored on the
    /// transaction id because a range lock is carried between actors by a split or a merge.
    /// </summary>
    [Fact]
    public void ZeroLeaseRangeLock_LivesToTheCeilingThenDies()
    {
        KeyValueRangeLock rangeLock = new()
        {
            TransactionId = At(0),
            Expires = HLCTimestamp.Zero,
            Mode = RangeLockMode.Exclusive
        };

        Assert.True(RangeLockChecks.IsLive(rangeLock, At(CeilingMs - 1), CeilingMs));
        Assert.False(RangeLockChecks.IsLive(rangeLock, At(CeilingMs), CeilingMs));
    }

    /// <summary>A leased range lock — the shape the split and merge quiesce uses — keeps its own deadline.</summary>
    [Fact]
    public void LeasedRangeLock_IsUnaffectedByTheCeiling()
    {
        KeyValueRangeLock rangeLock = new()
        {
            TransactionId = At(0),
            Expires = At(500_000),
            Mode = RangeLockMode.Exclusive
        };

        Assert.True(RangeLockChecks.IsLive(rangeLock, At(400_000), CeilingMs));
        Assert.False(RangeLockChecks.IsLive(rangeLock, At(500_001), CeilingMs));
    }

    // ── observability ────────────────────────────────────────────────────────────────

    /// <summary>
    /// An expiry must name its owner. The transaction that planted an orphaned intent is dead and unknown
    /// by the time anyone notices, and the plant site is not recorded anywhere, so this counter and this
    /// log line are the only evidence a future occurrence leaves behind.
    /// </summary>
    [Fact]
    public void CeilingExpiry_IsCountedOnceAndNamesTheKeyAndOwner()
    {
        using MetricCapture capture = new();

        CapturingKahunaLogger logger = new();
        (KeyValueContext context, RaftManager raft) = CreateContext(logger: logger);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp owner = now - context.SessionOwnedIntentCeilingMs - 1_000;

        KeyValueWriteIntent orphan = new()
        {
            TransactionId = owner,
            AcquiredAt = owner,
            Expires = HLCTimestamp.Zero
        };

        // Several paths may meet the same orphaned intent before one of them drops it; each occurrence is
        // the same orphan, so the counter must stay equal to the number of intents, not the number of reads.
        Assert.False(KeyValueWriteIntentLease.IsLive(context, "wedged/key", orphan, now));
        Assert.False(KeyValueWriteIntentLease.IsLive(context, "wedged/key", orphan, now));

        Assert.Equal(1, capture.Sum("kahuna.kv.session_owned_intent_ceiling_expiries", "kind=intent"));

        string line = Assert.Single(logger.Lines, l => l.Contains("liveness ceiling", StringComparison.Ordinal));
        Assert.Contains("wedged/key", line, StringComparison.Ordinal);
        Assert.Contains(owner.ToString(), line, StringComparison.Ordinal);
    }

    /// <summary>
    /// An ordinary lease expiry is routine and must stay silent: counting it would drown the orphan signal
    /// in the traffic of every normal lock that timed out.
    /// </summary>
    [Fact]
    public void OrdinaryLeaseExpiry_IsNotCountedAsACeilingExpiry()
    {
        using MetricCapture capture = new();

        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueWriteIntent leased = new()
        {
            TransactionId = now,
            AcquiredAt = now,
            Expires = now - 1_000
        };

        Assert.False(KeyValueWriteIntentLease.IsLive(context, "leased/key", leased, now));
        Assert.Equal(0, capture.Count("kahuna.kv.session_owned_intent_ceiling_expiries"));
    }

    // ── the collector ────────────────────────────────────────────────────────────────

    /// <summary>
    /// The collector keeps its own reading of intent liveness, so the ceiling has to reach it too. An
    /// orphaned intent that still pins its entry keeps the key resident forever and re-queues itself every
    /// cycle — the same defect as the unservable scan, paid in memory instead of availability.
    /// </summary>
    [Fact]
    public void Collector_EvictsAnEntryHeldOnlyByAnOrphanedIntent()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 5, batchMax: 100);
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        TryCollectHandler handler = new(context);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp orphanedAt = now - context.SessionOwnedIntentCeilingMs - 1_000;

        // Coldest entry, held by an intent whose session can no longer exist.
        InsertClean(context, "orphaned", now);
        context.Store.Get("orphaned")!.WriteIntent = new KeyValueWriteIntent
        {
            TransactionId = orphanedAt,
            AcquiredAt = orphanedAt,
            Expires = HLCTimestamp.Zero
        };

        // A second cold entry whose session-owned intent is still inside the ceiling: it must survive, or
        // the sweep would be evicting entries out from under live transactions.
        InsertClean(context, "still-held", now);
        context.Store.Get("still-held")!.WriteIntent = new KeyValueWriteIntent
        {
            TransactionId = now,
            AcquiredAt = now,
            Expires = HLCTimestamp.Zero
        };

        for (int i = 0; i < 9; i++)
            InsertClean(context, $"filler/{i}", now);

        handler.Execute();

        Assert.False(context.Store.ContainsKey("orphaned"),
            "an entry pinned only by an intent past the ceiling must become evictable");
        Assert.True(context.Store.ContainsKey("still-held"),
            "an entry held by an intent inside the ceiling must never be evicted");
    }

    // ── configuration ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Left at zero the ceiling derives the span the session machinery already bounds itself by: the
    /// clamped maximum session timeout, the reaper's grace window, and the longest a dispatched effect can
    /// still land at a participant.
    /// </summary>
    [Fact]
    public void ZeroCeiling_DerivesTheSessionBound()
    {
        (KeyValueContext context, RaftManager _) = CreateContext();

        Assert.Equal(0, context.Configuration.SessionOwnedIntentCeilingMs);
        Assert.Equal(
            context.Configuration.MaxTransactionTimeout
                + TransactionCoordinator.ReapGraceMs
                + TransactionCoordinator.MaxParticipantEffectTtlMs,
            context.SessionOwnedIntentCeilingMs);
    }

    /// <summary>
    /// A ceiling below the session bound is refused at load. Such a value could expire the lock of a
    /// transaction that is still legitimately running, which is the one outcome this setting must never
    /// produce — and a node that started with it would produce it silently.
    /// </summary>
    [Fact]
    public void CeilingBelowTheSessionBound_IsRejectedAtLoad()
    {
        KahunaServerException thrown = Assert.Throws<KahunaServerException>(() =>
            ConfigurationValidator.Validate(BaseConfiguration(ceilingMs: 60_000)));

        Assert.Contains("SessionOwnedIntentCeilingMs", thrown.Message, StringComparison.Ordinal);
    }

    /// <summary>An explicit ceiling at or above the session bound is accepted and used as given.</summary>
    [Fact]
    public void CeilingAtOrAboveTheSessionBound_IsAccepted()
    {
        int floor = 300_000 + TransactionCoordinator.ReapGraceMs;
        KahunaConfiguration config = ConfigurationValidator.Validate(BaseConfiguration(ceilingMs: floor));

        Assert.Equal(floor, config.SessionOwnedIntentCeilingMs);

        (KeyValueContext context, RaftManager _) = CreateContext(config);
        Assert.Equal(floor, context.SessionOwnedIntentCeilingMs);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    private static KahunaConfiguration BaseConfiguration(int ceilingMs) => new()
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
        RevisionRetention = 16,
        MaxTransactionTimeout = 300_000,
        SessionOwnedIntentCeilingMs = ceilingMs
    };

    private static KahunaConfiguration CreateConfiguration(int maxEntries = 50_000, int batchMax = 1000)
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = maxEntries,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = batchMax,
            RevisionRetention = 16
        });
        cfg.DirtyObjectsWriterDelay = 1_000;
        return cfg;
    }

    private (KeyValueContext, RaftManager) CreateContext(
        KahunaConfiguration? config = null,
        ILogger<IKahuna>? logger = null)
    {
        config ??= CreateConfiguration();
        logger ??= NullLogger<IKahuna>.Instance;

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = "intent-ceiling-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        ));

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            null!,
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (context, raft);
    }

    private static void InsertClean(KeyValueContext context, string key, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0
        });
    }

    /// <summary>Records the lines the context's logger emits, so an expiry's attribution can be asserted.</summary>
    private sealed class CapturingKahunaLogger : ILogger<IKahuna>
    {
        public readonly ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Lines.Enqueue(formatter(state, exception));
        }
    }

    /// <summary>
    /// Captures measurements on the "Kahuna" meter for the duration of a scope. Only measurements recorded
    /// on the constructing thread are kept: the meter is process-wide and this assembly runs collections in
    /// parallel, so another live actor's expiry would otherwise land in these counts. These tests evaluate
    /// the policy directly, so thread identity separates them from every other caller.
    /// </summary>
    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener listener;
        private readonly List<(string Name, double Value, string Tags)> measurements = [];
        private readonly Lock gate = new();
        private readonly int ownerThreadId = Environment.CurrentManagedThreadId;

        public MetricCapture()
        {
            listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == "Kahuna")
                        l.EnableMeasurementEvents(instrument);
                },
            };

            listener.SetMeasurementEventCallback<long>((i, m, t, _) => Add(i, m, t));
            listener.Start();
        }

        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (Environment.CurrentManagedThreadId != ownerThreadId)
                return;

            string rendered = string.Join(",", tags.ToArray().Select(t => $"{t.Key}={t.Value}"));

            lock (gate)
                measurements.Add((instrument.Name, value, rendered));
        }

        public double Sum(string name, string tags = "")
        {
            lock (gate)
                return measurements.Where(m => m.Name == name && m.Tags == tags).Sum(m => m.Value);
        }

        public int Count(string name)
        {
            lock (gate)
                return measurements.Count(m => m.Name == name);
        }

        public void Dispose() => listener.Dispose();
    }
}
