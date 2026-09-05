
using System.Linq;
using System.Reflection;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Guards the payload-pool recycle contract: a recycled shell must be indistinguishable from a
/// fresh one. A field that survives <see cref="OperationCompletionPayload.Clear"/> would leak one
/// operation's effect into another transaction's fold.
/// </summary>
public class TestOperationCompletionPayloadPool
{
    /// <summary>
    /// The number of settable public instance properties <see cref="OperationCompletionPayload"/>
    /// currently declares. A read-only computed property (like <see cref="OperationCompletionPayload.HasWorkingSetEffect"/>)
    /// derives from the settable ones and needs no slot in <see cref="OperationCompletionPayload.Clear"/>, so it
    /// is excluded from this count. When a new settable property is added, it must join
    /// <see cref="OperationCompletionPayload.Clear"/> and the population + assertions below, then this count.
    /// </summary>
    private const int ExpectedPropertyCount = 16;

    private static OperationCompletionPayload FullyPopulated()
    {
        RangeLockKey range = new("orders", "a", true, "z", false, KeyValueDurability.Persistent);

        return new()
        {
            ModifiedKey = "k",
            ModifiedKeys = [("k", KeyValueDurability.Persistent)],
            AcquiredPointLock = "k",
            AcquiredPointLocks = [("k", KeyValueDurability.Persistent)],
            ReleasedPointLock = "k",
            AcquiredPrefixLock = "users:",
            ReleasedPrefixLock = "users:",
            AcquiredRangeLock = (range, RangeLockMode.Exclusive),
            ReleasedRangeLock = range,
            Read = new KeyValueTransactionReadKey { Key = "k", Durability = KeyValueDurability.Persistent, Exists = true, Revision = 1 },
            ReadObservations = [new KeyValueTransactionReadKey { Key = "k", Durability = KeyValueDurability.Persistent, Exists = true, Revision = 1 }],
            StagedMutations = [new StagedMutationEffect("k", [1], KeyValueState.Set, 1, 1, true)],
            Durability = KeyValueDurability.Ephemeral,
            CachedType = KeyValueResponseType.Locked,
            CachedRevision = 7,
            CachedTimestamp = new HLCTimestamp(1, 2, 3)
        };
    }

    [Fact]
    public void Clear_ResetsEveryProperty()
    {
        OperationCompletionPayload payload = FullyPopulated();

        payload.Clear();

        Assert.Null(payload.ModifiedKey);
        Assert.Null(payload.ModifiedKeys);
        Assert.Null(payload.AcquiredPointLock);
        Assert.Null(payload.AcquiredPointLocks);
        Assert.Null(payload.ReleasedPointLock);
        Assert.Null(payload.AcquiredPrefixLock);
        Assert.Null(payload.ReleasedPrefixLock);
        Assert.Null(payload.AcquiredRangeLock);
        Assert.Null(payload.ReleasedRangeLock);
        Assert.Null(payload.Read);
        Assert.Null(payload.ReadObservations);
        Assert.Null(payload.StagedMutations);
        Assert.Equal(default, payload.Durability);
        Assert.Equal(default, payload.CachedType);
        Assert.Equal(0, payload.CachedRevision);
        Assert.Equal(default, payload.CachedTimestamp);
    }

    [Fact]
    public void PropertyCount_MatchesClearCoverage()
    {
        PropertyInfo[] settableProperties = typeof(OperationCompletionPayload)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(property => property.CanWrite)
            .ToArray();

        Assert.True(
            settableProperties.Length == ExpectedPropertyCount,
            $"OperationCompletionPayload declares {settableProperties.Length} settable public properties but " +
            $"the recycle contract covers {ExpectedPropertyCount}. Add the new property to Clear() and to this " +
            "test, then update ExpectedPropertyCount.");
    }

    [Fact]
    public void RentAfterReturn_YieldsClearedShell()
    {
        OperationCompletionPayload payload = FullyPopulated();

        OperationCompletionPayloadPool.Return(payload);

        // The pool may hand back this shell or another; drain until the populated one reappears or
        // prove any rented shell is clean. One Rent suffices: Return clears before pooling.
        OperationCompletionPayload rented = OperationCompletionPayloadPool.Rent();

        Assert.Null(rented.ModifiedKey);
        Assert.Null(rented.Read);
        Assert.Null(rented.StagedMutations);
        Assert.Equal(default, rented.CachedType);
        Assert.Equal(default, rented.CachedTimestamp);

        OperationCompletionPayloadPool.Return(rented);
    }
}
