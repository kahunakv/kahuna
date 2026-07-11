
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Builds a stable SHA-256 digest of a transaction operation's structured inputs. The digest lets the
/// operation registry reject a reused operation id whose declaration differs. Fields are appended
/// length-prefixed (never concatenated as text) so that distinct field boundaries cannot collide —
/// e.g. key "ab"+value "c" hashes differently from key "a"+value "bc".
/// </summary>
internal static class OperationDigest
{
    internal static byte[] ForSet(string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.Set);
        AppendString(hash, key);
        AppendBytes(hash, value);
        AppendBytes(hash, compareValue);
        AppendLong(hash, compareRevision);
        AppendInt(hash, (int)flags);
        AppendInt(hash, expiresMs);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForDelete(string key, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.Delete);
        AppendString(hash, key);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForExtend(string key, int expiresMs, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.Extend);
        AppendString(hash, key);
        AppendInt(hash, expiresMs);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForPointLockAcquire(string key, int expiresMs, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.PointLock);
        AppendString(hash, key);
        AppendInt(hash, expiresMs);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForPointLockRelease(string key, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.PointLock);
        // A distinct constant separates a release declaration from an acquire of the same key.
        AppendInt(hash, -1);
        AppendString(hash, key);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForPrefixLockAcquire(string prefixKey, int expiresMs, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.PrefixLock);
        AppendString(hash, prefixKey);
        AppendInt(hash, expiresMs);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForPrefixLockRelease(string prefixKey, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.PrefixLock);
        // A distinct constant separates a release declaration from an acquire of the same prefix.
        AppendInt(hash, -1);
        AppendString(hash, prefixKey);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForRangeLockAcquire(string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, RangeLockMode mode, int expiresMs, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.RangeLock);
        AppendBounds(hash, prefix, startKey, startInclusive, endKey, endInclusive);
        AppendInt(hash, (int)mode);
        AppendInt(hash, expiresMs);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForRangeLockRelease(string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.RangeLock);
        // A distinct constant separates a release declaration from an acquire of the same bounds.
        AppendInt(hash, -1);
        AppendBounds(hash, prefix, startKey, startInclusive, endKey, endInclusive);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForScan(string prefixedKey, long readTimestampL, long readTimestampC, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, OperationKind.Scan);
        AppendString(hash, prefixedKey);
        AppendLong(hash, readTimestampL);
        AppendLong(hash, readTimestampC);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    internal static byte[] ForRead(OperationKind kind, string key, long revision, KeyValueDurability durability)
    {
        using IncrementalHash hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendTag(hash, kind);
        AppendString(hash, key);
        AppendLong(hash, revision);
        AppendInt(hash, (int)durability);
        return hash.GetHashAndReset();
    }

    private static void AppendBounds(IncrementalHash hash, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        AppendString(hash, prefix);
        AppendBytes(hash, startKey is null ? null : Encoding.UTF8.GetBytes(startKey));
        AppendInt(hash, startInclusive ? 1 : 0);
        AppendBytes(hash, endKey is null ? null : Encoding.UTF8.GetBytes(endKey));
        AppendInt(hash, endInclusive ? 1 : 0);
    }

    private static void AppendTag(IncrementalHash hash, OperationKind kind) => AppendInt(hash, (int)kind);

    private static void AppendString(IncrementalHash hash, string value) =>
        AppendBytes(hash, Encoding.UTF8.GetBytes(value));

    private static void AppendBytes(IncrementalHash hash, byte[]? value)
    {
        Span<byte> len = stackalloc byte[4];
        // -1 marks null so it is distinguishable from an empty array.
        BinaryPrimitives.WriteInt32LittleEndian(len, value?.Length ?? -1);
        hash.AppendData(len);
        if (value is not null)
            hash.AppendData(value);
    }

    private static void AppendInt(IncrementalHash hash, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
        hash.AppendData(buffer);
    }

    private static void AppendLong(IncrementalHash hash, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(buffer, value);
        hash.AppendData(buffer);
    }
}
