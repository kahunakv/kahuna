using Google.Protobuf;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna;
using Kahuna.Communication.External.Grpc;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the foreign-range-lock arm of the batched commit-time conflict probe: the question a transaction's
/// write set asks at decide time, to catch a range lock acquired after the write was staged — an interleaving
/// the write-time fence in TrySet/TryDelete cannot see because at write time no lock existed.
///
/// The two arms of the probe are exercised independently, because the whole point of selecting checks per key is
/// that a read key keeps asking only about concurrent write intents while a write key asks only about range
/// locks. A test that let both bits travel together would pass whether or not they are actually separable.
///
/// Convention: prefix "t:cpf"; range lock [t:cpf/10, t:cpf/50); inside key t:cpf/25; outside key t:cpf/99.
/// Ordinal comparison puts those strings in the intended positions.
/// </summary>
public sealed class TestConflictProbeRangeLockFence : BaseCluster
{
    private const string Prefix = "t:cpf";
    private const string StartKey = Prefix + "/10";
    private const string InsideKey = Prefix + "/25";
    private const string EndKey = Prefix + "/50";
    private const string OutsideKey = Prefix + "/99";
    private const int ExpiresMs = 30_000;

    private readonly ILoggerFactory loggerFactory;

    public TestConflictProbeRangeLockFence(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(InsideKey, ct);

        return node;
    }

    /// <summary>Acquires a range lock over [StartKey, EndKey) held by <paramref name="owner"/>.</summary>
    private static async Task AcquireRangeLock(
        EmbeddedKahunaNode node, HLCTimestamp owner, RangeLockMode mode, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await ((KahunaManager)node.Kahuna).LocateAndTryAcquireRangeLock(
            owner, Prefix, StartKey, true, EndKey, false, ExpiresMs,
            KeyValueDurability.Persistent, mode, ct);

        Assert.Equal(KeyValueResponseType.Locked, type);
    }

    private static async Task<KeyValueResponseType> Probe(
        EmbeddedKahunaNode node, HLCTimestamp transactionId, string key, KeyValueConflictChecks checks, CancellationToken ct)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await node.Kahuna.LocateAndTryCheckManyWriteIntents(
                transactionId, [new(key, KeyValueDurability.Persistent, checks)], ct);

        return Assert.Single(results).type;
    }

    // ── The fence itself ────────────────────────────────────────────────────────

    /// <summary>
    /// A key covered by another transaction's exclusive range lock is flagged. The probed key is never
    /// written, which is deliberate: a phantom insert has no entry, and a bucket read off a loaded entry
    /// would answer "no lock" for exactly the case a range lock exists to cover.
    /// </summary>
    [Fact]
    public async Task RangeLockCheck_FlagsKeyCoveredByForeignExclusiveLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Exclusive, ct);

        Assert.Equal(
            KeyValueResponseType.Aborted,
            await Probe(node, new HLCTimestamp(0, 500, 0), InsideKey, KeyValueConflictChecks.ForeignRangeLock, ct));
    }

    /// <summary>
    /// A shared range lock blocks a write just as an exclusive one does — a write needs exclusive on [K,K],
    /// which conflicts with S as well as X.
    /// </summary>
    [Fact]
    public async Task RangeLockCheck_FlagsKeyCoveredByForeignSharedLock()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Shared, ct);

        Assert.Equal(
            KeyValueResponseType.Aborted,
            await Probe(node, new HLCTimestamp(0, 500, 0), InsideKey, KeyValueConflictChecks.ForeignRangeLock, ct));
    }

    /// <summary>
    /// The lock's own holder is not fenced by it. Serializable read-write transactions take a range lock and
    /// then write inside it; flagging the holder would abort every one of them.
    /// </summary>
    [Fact]
    public async Task RangeLockCheck_IgnoresTheLockHoldersOwnTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        HLCTimestamp owner = new(0, 100, 0);
        await AcquireRangeLock(node, owner, RangeLockMode.Exclusive, ct);

        Assert.Equal(
            KeyValueResponseType.DoesNotExist,
            await Probe(node, owner, InsideKey, KeyValueConflictChecks.ForeignRangeLock, ct));
    }

    /// <summary>A key outside the locked bounds is not fenced — the check must not over-block.</summary>
    [Fact]
    public async Task RangeLockCheck_DoesNotFlagKeyOutsideTheLockedRange()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Exclusive, ct);

        Assert.Equal(
            KeyValueResponseType.DoesNotExist,
            await Probe(node, new HLCTimestamp(0, 500, 0), OutsideKey, KeyValueConflictChecks.ForeignRangeLock, ct));
    }

    // ── The two arms stay separable ─────────────────────────────────────────────

    /// <summary>
    /// Asking only about write intents does not surface a range lock. This is what keeps a transaction's read
    /// set behaving exactly as before: a range lock covering a key it merely read is not its conflict.
    /// </summary>
    [Fact]
    public async Task WriteIntentCheckAlone_DoesNotSeeRangeLocks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Exclusive, ct);

        Assert.Equal(
            KeyValueResponseType.DoesNotExist,
            await Probe(node, new HLCTimestamp(0, 500, 0), InsideKey, KeyValueConflictChecks.WriteIntent, ct));
    }

    /// <summary>A probe that asks for nothing answers no conflict, whatever locks exist.</summary>
    [Fact]
    public async Task NoChecks_AnswersNoConflict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Exclusive, ct);

        Assert.Equal(
            KeyValueResponseType.DoesNotExist,
            await Probe(node, new HLCTimestamp(0, 500, 0), InsideKey, KeyValueConflictChecks.None, ct));
    }

    /// <summary>
    /// One batched probe carries both questions at once and answers each key by the checks it asked for —
    /// the shape the commit path uses, where the read set and the write set travel in a single call.
    /// </summary>
    [Fact]
    public async Task OneProbe_AnswersEachKeyByItsOwnChecks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await AcquireRangeLock(node, new HLCTimestamp(0, 100, 0), RangeLockMode.Exclusive, ct);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> results =
            await node.Kahuna.LocateAndTryCheckManyWriteIntents(
                new HLCTimestamp(0, 500, 0),
                [
                    new(InsideKey, KeyValueDurability.Persistent, KeyValueConflictChecks.ForeignRangeLock),
                    new(OutsideKey, KeyValueDurability.Persistent, KeyValueConflictChecks.ForeignRangeLock),
                    // Same covered key, asked about as a read dependency instead of a write.
                    new(InsideKey, KeyValueDurability.Ephemeral, KeyValueConflictChecks.WriteIntent)
                ],
                ct);

        Assert.Equal(3, results.Count);
        Assert.Equal(KeyValueResponseType.Aborted,
            Assert.Single(results, r => r.key == InsideKey && r.durability == KeyValueDurability.Persistent).type);
        Assert.Equal(KeyValueResponseType.DoesNotExist,
            Assert.Single(results, r => r.key == OutsideKey).type);
        Assert.Equal(KeyValueResponseType.DoesNotExist,
            Assert.Single(results, r => r.key == InsideKey && r.durability == KeyValueDurability.Ephemeral).type);
    }

    // ── gRPC wire fidelity ──────────────────────────────────────────────────────

    /// <summary>
    /// The per-key checks selection survives a real proto serialize/parse round trip and reaches the manager
    /// unchanged. The in-process transport hands the probe list straight through and would never notice a
    /// field that was not put on the wire, so this is the only place the encoding is actually pinned.
    /// </summary>
    [Fact]
    public async Task GrpcWire_PerItemChecks_ReachTheManagerUnchanged()
    {
        GrpcTryCheckManyWriteIntentsRequest request = new();
        request.Items.Add(new GrpcTryCheckManyWriteIntentsRequestItem
        {
            Key = InsideKey,
            Durability = GrpcKeyValueDurability.Persistent,
            Checks = (uint)KeyValueConflictChecks.ForeignRangeLock
        });
        request.Items.Add(new GrpcTryCheckManyWriteIntentsRequestItem
        {
            Key = OutsideKey,
            Durability = GrpcKeyValueDurability.Persistent,
            Checks = (uint)(KeyValueConflictChecks.WriteIntent | KeyValueConflictChecks.ForeignRangeLock)
        });

        GrpcTryCheckManyWriteIntentsRequest parsed =
            GrpcTryCheckManyWriteIntentsRequest.Parser.ParseFrom(request.ToByteArray());

        CapturingProbeKahuna fake = new();
        await new KeyValuesService(fake, NullLogger<IKahuna>.Instance)
            .TryCheckManyWriteIntentsInternal(parsed, null!);

        Assert.NotNull(fake.Probes);
        Assert.Equal(2, fake.Probes!.Count);
        Assert.Equal(KeyValueConflictChecks.ForeignRangeLock, fake.Probes[0].Checks);
        Assert.Equal(KeyValueConflictChecks.WriteIntent | KeyValueConflictChecks.ForeignRangeLock, fake.Probes[1].Checks);
    }

    /// <summary>
    /// An item with no checks field set decodes to <see cref="KeyValueConflictChecks.None"/> rather than
    /// silently defaulting to a check the sender did not ask for.
    /// </summary>
    [Fact]
    public async Task GrpcWire_UnsetChecks_DecodeToNone()
    {
        GrpcTryCheckManyWriteIntentsRequest request = new();
        request.Items.Add(new GrpcTryCheckManyWriteIntentsRequestItem
        {
            Key = InsideKey,
            Durability = GrpcKeyValueDurability.Persistent
        });

        GrpcTryCheckManyWriteIntentsRequest parsed =
            GrpcTryCheckManyWriteIntentsRequest.Parser.ParseFrom(request.ToByteArray());

        CapturingProbeKahuna fake = new();
        await new KeyValuesService(fake, NullLogger<IKahuna>.Instance)
            .TryCheckManyWriteIntentsInternal(parsed, null!);

        Assert.NotNull(fake.Probes);
        Assert.Equal(KeyValueConflictChecks.None, Assert.Single(fake.Probes!).Checks);
    }

    /// <summary>Records the probe list the gRPC handler decoded and answers every key cleanly.</summary>
    private sealed class CapturingProbeKahuna : FakeKahunaBase
    {
        public List<KeyValueConflictProbe>? Probes { get; private set; }

        public override Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentValues(
            HLCTimestamp transactionId, List<KeyValueConflictProbe> keys)
        {
            Probes = keys;

            return Task.FromResult(keys.ConvertAll(probe =>
                (KeyValueResponseType.DoesNotExist, probe.Key, probe.Durability)));
        }
    }
}
