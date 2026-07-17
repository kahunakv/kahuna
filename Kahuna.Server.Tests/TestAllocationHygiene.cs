
using System.IO.Hashing;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards allocation-hygiene invariants that are easy to regress silently:
///   • returning a compare-and-set request to the pool releases its (potentially large) compare
///     buffer instead of pinning it until the object is next reused;
///   • the span-based lock-resource hash yields the exact same value as encoding the resource into
///     a fresh UTF-8 array and hashing that — so routing/partitioning is unchanged.
/// </summary>
public sealed class TestAllocationHygiene
{
    [Fact]
    public void KeyValueRequestReturn_ReleasesCompareValueBuffer()
    {
        byte[] compareBuffer = new byte[1 << 20]; // 1 MiB — the buffer we must not retain in the pool.

        KeyValueRequest request = KeyValueRequestPool.Rent(
            KeyValueRequestType.TrySet, HLCTimestamp.Zero, HLCTimestamp.Zero,
            key: "acc/1", value: [1, 2, 3], compareValue: compareBuffer, compareRevision: 0,
            KeyValueFlags.None, expiresMs: 0, HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            proposalId: 0, partitionId: 0, promise: null);

        Assert.Same(compareBuffer, request.CompareValue);

        // Returning the request to the pool must drop every heap reference it holds, including the
        // compare buffer — otherwise a pooled object pins a large array indefinitely between reuses.
        KeyValueRequestPool.Return(request);

        Assert.Null(request.Value);
        Assert.Null(request.CompareValue);
        Assert.Null(request.Promise);
    }

    [Theory]
    [InlineData("")]
    [InlineData("lock")]
    [InlineData("app/orders/eu-west-1/shard-000042")]
    [InlineData("ключ-ресурса/セマフォ/🔒")] // multi-byte UTF-8
    [InlineData("very-long-resource-name-that-exceeds-the-inline-stack-buffer-threshold-used-by-the-" +
                "bounded-encoder-so-the-arraypool-fallback-path-is-exercised-instead-of-the-stack-path-" +
                "and-still-produces-the-same-hash-value-as-a-freshly-allocated-array-would-xxxxxxxxxxxxx")]
    public void LockRequestHash_MatchesFreshUtf8Array(string resource)
    {
        LockRequest request = new(
            LockRequestType.TryLock, resource, owner: null, expiresMs: 1000,
            LockDurability.Ephemeral, proposalId: 0, partitionId: 0, promise: null);

        int expected = (int)XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(resource));

        Assert.Equal(expected, request.GetHash());
    }
}
