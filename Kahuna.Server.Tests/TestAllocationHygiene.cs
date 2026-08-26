
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
            proposalId: 0, partitionId: 0, promise: default);

        Assert.Same(compareBuffer, request.CompareValue);

        // Returning the request to the pool must drop every heap reference it holds, including the
        // compare buffer — otherwise a pooled object pins a large array indefinitely between reuses.
        KeyValueRequestPool.Return(request);

        Assert.Null(request.Value);
        Assert.Null(request.CompareValue);
        Assert.True(request.Promise.IsDefault);
    }

    [Fact]
    public void LockRequestReturn_ReleasesOwnerAndPromise()
    {
        byte[] owner = new byte[1 << 20]; // 1 MiB — the buffer we must not retain in the pool.
        TaskCompletionSource<LockResponse?> promise = new();

        LockRequest request = LockRequestPool.RentInvalidateOrApply(
            "lock/1", owner, partitionId: 0, fencingToken: 7,
            HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, LockState.Locked);

        Assert.Same(owner, request.Owner);
        Assert.NotNull(request.InvalidateOrApplyData);
        Assert.True(request.ReturnToPoolOnReceive);

        // Returning the request to the pool must drop every heap reference it holds — the owner
        // buffer, the apply record, and any promise — otherwise a pooled object pins them
        // indefinitely between reuses. The ownership marker must also reset so a later Ask-style
        // rent of the same instance is not recycled twice.
        LockRequestPool.Return(request);

        Assert.Null(request.Owner);
        Assert.Null(request.InvalidateOrApplyData);
        Assert.Null(request.Promise);
        Assert.False(request.ReturnToPoolOnReceive);

        LockRequest rented = LockRequestPool.Rent(
            LockRequestType.TryLock, "lock/2", owner, expiresMs: 1000,
            LockDurability.Persistent, proposalId: 0, partitionId: 0, promise);

        Assert.False(rented.ReturnToPoolOnReceive);
        Assert.Same(promise, rented.Promise);

        LockRequestPool.Return(rented);
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
            LockDurability.Ephemeral, proposalId: 0, partitionId: 0, promise: default);

        int expected = (int)XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(resource));

        Assert.Equal(expected, request.GetHash());
    }
}
