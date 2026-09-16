using Kahuna.Server.KeyValues;
using Kahuna.Shared.Routing;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A prefix-shaped request (bucket scan, prefix lock, range lock) must hash to the actor that owns the key
/// space the prefix names, whatever spelling the caller uses. Every point request on a key of that key space
/// hashes to the same actor, so the two must agree: a scan routed elsewhere caches copies that never receive
/// the owner's commit notifications, and a prefix lock recorded elsewhere never blocks a write.
/// </summary>
public sealed class TestKeyValueRequestRouting
{
    private static KeyValueRequest Request(KeyValueRequestType type, string key) => new(
        type,
        HLCTimestamp.Zero,
        HLCTimestamp.Zero,
        key,
        null,
        null,
        -1,
        KeyValueFlags.None,
        0,
        HLCTimestamp.Zero,
        KeyValueDurability.Persistent,
        0,
        0,
        default);

    [Theory]
    [InlineData("doctors", "doctors/1")]
    [InlineData("doctors/", "doctors/1")]
    [InlineData("svc1|cb.probe", "svc1|cb.probe/5ae318")]
    [InlineData("svc1|cb.probe/", "svc1|cb.probe/5ae318")]
    [InlineData("a/b", "a/b/1")]
    [InlineData("a/b/", "a/b/1")]
    public void PrefixRequests_HashToTheActorOfTheKeySpaceTheyName(string prefix, string key)
    {
        int owner = Request(KeyValueRequestType.TryGet, key).GetHash();

        Assert.Equal(owner, Request(KeyValueRequestType.TrySet, key).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.TryDelete, key).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.InvalidateOrApply, key).GetHash());

        Assert.Equal(owner, Request(KeyValueRequestType.GetByBucket, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.TryAcquireExclusivePrefixLock, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.TryReleaseExclusivePrefixLock, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.TryAcquireExclusiveRangeLock, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.TryReleaseExclusiveRangeLock, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.GetRangeLocks, prefix).GetHash());
        Assert.Equal(owner, Request(KeyValueRequestType.ImportRangeLocks, prefix).GetHash());
    }

    [Fact]
    public void BothSpellingsOfAKeySpace_HashTheSame()
    {
        Assert.Equal(
            Request(KeyValueRequestType.GetByBucket, "doctors").GetHash(),
            Request(KeyValueRequestType.GetByBucket, "doctors/").GetHash());

        Assert.Equal(
            Request(KeyValueRequestType.TryAcquireExclusivePrefixLock, "doctors").GetHash(),
            Request(KeyValueRequestType.TryAcquireExclusivePrefixLock, "doctors/").GetHash());
    }

    /// <summary>
    /// The partition router derives a prefix's key space the same way once the prefix is reduced: the
    /// slashed spelling must land on the partition of the keys it covers, not on the partition of a
    /// key space that happens to be spelled with a slash.
    /// </summary>
    [Theory]
    [InlineData("doctors", "doctors/1")]
    [InlineData("doctors/", "doctors/1")]
    [InlineData("svc1|cb.probe/", "svc1|cb.probe/5ae318")]
    [InlineData("a/b/", "a/b/1")]
    public void PrefixRequests_LandOnThePartitionOfTheKeySpaceTheyName(string prefix, string key)
    {
        for (int poolSize = 1; poolSize <= 16; poolSize++)
        {
            Assert.Equal(
                HashPlacement.BucketOfKey(key, poolSize),
                HashPlacement.BucketOfKey(KeyValueKeySpace.OfPrefix(prefix) + "/", poolSize));
        }
    }

    [Theory]
    [InlineData("doctors", "doctors")]
    [InlineData("doctors/", "doctors")]
    [InlineData("a/b/", "a/b")]
    [InlineData("a/b", "a/b")]
    [InlineData("/", "")]
    [InlineData("", "")]
    public void KeySpaceOfAPrefix_DropsOneTrailingSlash(string prefix, string expected)
    {
        Assert.Equal(expected, KeyValueKeySpace.OfPrefix(prefix));
    }

    [Theory]
    [InlineData("doctors/1", "doctors")]
    [InlineData("a/b/1", "a/b")]
    [InlineData("doctors", null)]
    public void KeySpaceOfAKey_IsTheTextBeforeTheLastSlash(string key, string? expected)
    {
        Assert.Equal(expected, KeyValueKeySpace.OfKey(key));
    }
}
