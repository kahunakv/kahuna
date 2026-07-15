
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Discovery;
using Kommander.Time;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that EmbeddedKahunaOptions.MaxTransactionTimeout is wired through both embedded
/// constructors and that ConfigurationValidator enforces the documented constraints.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestEmbeddedTransactionLifetime
{
    private static EmbeddedKahunaOptions BaseOptions(int maxTimeoutMs = 300_000) => new()
    {
        Storage          = "memory",
        WalStorage       = "memory",
        InitialPartitions = 1,
        MaxTransactionTimeout = maxTimeoutMs,
    };

    private static int? StoredTimeout(EmbeddedKahunaNode node, HLCTimestamp txId)
        => ((KahunaManager)node.Kahuna).GetRecordedSessionTimeout(txId);

    // ── Raised max is honored end-to-end ─────────────────────────────────────

    [Fact]
    public async Task RaisedMaxTransactionTimeout_AdmitsLongSession_Unclamped()
    {
        const int oneHourMs = 3_600_000;
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(maxTimeoutMs: oneHourMs));
        await node.StartAsync(ct);

        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.StartTransaction(
            new KeyValueTransactionOptions { Timeout = oneHourMs });

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.Equal(oneHourMs, StoredTimeout(node, handle.TransactionId));
    }

    // ── Default max still clamps an oversized request ─────────────────────────

    [Fact]
    public async Task DefaultMaxTransactionTimeout_ClampsLongSession()
    {
        const int defaultMaxMs = 300_000;
        const int oneHourMs    = 3_600_000;
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(BaseOptions(maxTimeoutMs: defaultMaxMs));
        await node.StartAsync(ct);

        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.StartTransaction(
            new KeyValueTransactionOptions { Timeout = oneHourMs });

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.Equal(defaultMaxMs, StoredTimeout(node, handle.TransactionId));
    }

    // ── Second constructor threads MaxTransactionTimeout through ──────────────
    // The first (in-process) constructor is already covered by
    // RaisedMaxTransactionTimeout_AdmitsLongSession_Unclamped above; this exercises the
    // externally-supplied-communication overload so both mapping sites are guarded.

    [Fact]
    public async Task SecondConstructor_RaisedMax_AdmitsLongSession()
    {
        const int oneHourMs = 3_600_000;
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = BaseOptions(maxTimeoutMs: oneHourMs);

        // Use the externally-supplied-communication overload with in-process fakes so the same
        // ConfigurationValidator.Validate block as the production cluster path runs.
        EmbeddedRaftCommunication raftComm = new();
        MemoryInterNodeCommmunication interNode = new();
        StaticDiscovery discovery = new(EmbeddedRaftCommunication.Witnesses);

        await using EmbeddedKahunaNode node = new(options, interNode, raftComm, discovery);
        await node.StartAsync(ct);

        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.StartTransaction(
            new KeyValueTransactionOptions { Timeout = oneHourMs });

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.Equal(oneHourMs, StoredTimeout(node, handle.TransactionId));
    }

    // ── Validation rejects inconsistent configuration ─────────────────────────

    [Fact]
    public void Validate_MaxBelowDefault_ThrowsWithValues()
    {
        KahunaConfiguration config = new()
        {
            DefaultTransactionTimeout = 10_000,
            MaxTransactionTimeout     = 5_000,
        };

        KahunaServerException ex = Assert.Throws<KahunaServerException>(
            () => ConfigurationValidator.Validate(config));

        Assert.Contains("5000", ex.Message);
        Assert.Contains("10000", ex.Message);
    }

    [Fact]
    public void Validate_MaxAtOrBelowZero_Throws()
    {
        KahunaConfiguration config = new()
        {
            DefaultTransactionTimeout = 5_000,
            MaxTransactionTimeout     = 0,
        };

        KahunaServerException ex = Assert.Throws<KahunaServerException>(
            () => ConfigurationValidator.Validate(config));

        Assert.Contains("MaxTransactionTimeout", ex.Message);
        Assert.Contains("must be greater than zero", ex.Message);
    }

    [Fact]
    public void Validate_DefaultAtOrBelowZero_Throws()
    {
        KahunaConfiguration config = new()
        {
            DefaultTransactionTimeout = 0,
            MaxTransactionTimeout     = 300_000,
        };

        KahunaServerException ex = Assert.Throws<KahunaServerException>(
            () => ConfigurationValidator.Validate(config));

        Assert.Contains("DefaultTransactionTimeout", ex.Message);
        Assert.Contains("must be greater than zero", ex.Message);
    }

    // ── Non-positive session request resolves to DefaultTransactionTimeout ────

    [Fact]
    public async Task NonPositiveSessionTimeout_ResolvesToDefault()
    {
        const int defaultMs = 5_000;
        CancellationToken ct = TestContext.Current.CancellationToken;

        EmbeddedKahunaOptions options = BaseOptions();
        options.DefaultTransactionTimeout = defaultMs;

        await using EmbeddedKahunaNode node = new(options);
        await node.StartAsync(ct);

        // Timeout = 0 must resolve to DefaultTransactionTimeout, not disable the deadline.
        (KeyValueResponseType type, TransactionHandle handle) = await node.Kahuna.StartTransaction(
            new KeyValueTransactionOptions { Timeout = 0 });

        Assert.Equal(KeyValueResponseType.Set, type);
        Assert.Equal(defaultMs, StoredTimeout(node, handle.TransactionId));
    }
}
