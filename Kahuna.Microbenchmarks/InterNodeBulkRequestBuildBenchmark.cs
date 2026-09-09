using BenchmarkDotNet.Attributes;
using Google.Protobuf;

using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the build of an inter-node bulk request in
/// <c>GrpcInterNodeCommunication</c>, measured on the set-many message because it is the widest item.
///
/// <para><b>Old</b> = feed a <c>yield return</c> iterator to <c>request.Items.Add(IEnumerable)</c>.
/// The repeated field pre-sizes only for an <c>ICollection</c> source; an iterator hides the count,
/// so the field starts at 8 slots and doubles as it fills, and the iterator state machine is one
/// more allocation per batch. <b>New</b> = set <c>Items.Capacity</c> from the input count, then add
/// each item directly, which is what the production encoders now do.</para>
///
/// <para>Both arms convert the same input list with the same per-item mapping, so the per-item
/// message objects cancel out. The difference is the backing-array growth, the discarded
/// intermediate arrays, and the iterator object: one iterator per batch at every size, an
/// oversized first array below 8 items, and one discarded array per doubling step above that.
/// The two arms produce byte-identical messages; the equivalence is pinned by
/// <c>TestInterNodeBulkRequestEncoding</c> in the server test suite, not re-checked here.</para>
///
/// <para><b>Result</b> (net10.0 Release, .NET 10.0.10, Apple M4 Arm64 RyuJIT, concurrent server GC;
/// BenchmarkDotNet 0.15.8, 10 cases in 3m46s). Old → new, per built request:
/// <list type="bullet">
///   <item>0 items: 136 B → 64 B, 14.1 ns → 5.9 ns (0.42× time). The iterator is the only thing
///     the empty batch ever allocated beyond the message itself.</item>
///   <item>1 item: 352 B → 224 B, 33.3 ns → 22.7 ns (0.68× time). The 8-slot first array shrinks
///     to an exact 1-slot one and the iterator goes away.</item>
///   <item>8 items: 1,248 B → 1,176 B, 129.9 ns → 101.6 ns (0.78× time). The first array fits the
///     batch exactly, so the 72 B saved is the iterator alone; the time still drops because the
///     direct loop skips the enumerator dispatch per item.</item>
///   <item>64 items: 9,384 B → 8,792 B, 912.8 ns → 714.6 ns (0.78× time). Three doubling steps
///     are avoided.</item>
///   <item>512 items: 73,968 B → 69,720 B, 7.02 µs → 5.80 µs (0.83× time). Six doubling steps are
///     avoided; the discarded arrays held 8+16+…+256 references plus the copies to move them.</item>
/// </list>
/// The allocation saving is bounded — the per-item messages dominate the batch — but the build runs
/// 17–58% faster at every size, and the same shape applies to the seven narrower bulk encoders.
/// These are build-leg numbers from one machine; they are not a cluster throughput measurement.</para>
/// </summary>
[MemoryDiagnoser]
public class InterNodeBulkRequestBuildBenchmark
{
    // An empty batch, a single routed key, a small fan-out group, a page, and a bulk write.
    [Params(0, 1, 8, 64, 512)]
    public int Items;

    private List<KahunaSetKeyValueRequestItem> items = null!;

    [GlobalSetup]
    public void Setup()
    {
        items = new(Items);

        for (int i = 0; i < Items; i++)
        {
            byte[] value = new byte[128];
            value[0] = (byte)i;

            items.Add(new KahunaSetKeyValueRequestItem
            {
                TransactionId = new(1, 100 + i, (uint)i),
                Key = $"bench/key/{i}",
                Value = value,
                CompareValue = null,
                CompareRevision = i,
                ExpiresMs = 30_000,
                Flags = KeyValueFlags.Set,
                Durability = KeyValueDurability.Persistent,
                RoutedGeneration = 1,
            });
        }
    }

    private static GrpcTrySetManyKeyValueRequestItem Convert(KahunaSetKeyValueRequestItem item)
    {
        GrpcTrySetManyKeyValueRequestItem grpcItem = new()
        {
            TransactionIdNode = item.TransactionId.N,
            TransactionIdPhysical = item.TransactionId.L,
            TransactionIdCounter = item.TransactionId.C,
            Key = item.Key,
            CompareRevision = item.CompareRevision,
            Flags = (GrpcKeyValueFlags)item.Flags,
            ExpiresMs = item.ExpiresMs,
            Durability = (GrpcKeyValueDurability)item.Durability,
            RoutedGeneration = item.RoutedGeneration,
        };

        if (item.Value is not null)
            grpcItem.Value = UnsafeByteOperations.UnsafeWrap(item.Value);

        if (item.CompareValue is not null)
            grpcItem.CompareValue = UnsafeByteOperations.UnsafeWrap(item.CompareValue);

        return grpcItem;
    }

    private static IEnumerable<GrpcTrySetManyKeyValueRequestItem> IteratorItems(List<KahunaSetKeyValueRequestItem> items)
    {
        foreach (KahunaSetKeyValueRequestItem item in items)
            yield return Convert(item);
    }

    [Benchmark(Baseline = true)]
    public GrpcTrySetManyKeyValueRequest IteratorAdd()
    {
        GrpcTrySetManyKeyValueRequest request = new();
        request.Items.Add(IteratorItems(items));
        return request;
    }

    [Benchmark]
    public GrpcTrySetManyKeyValueRequest ReservedDirectAdd()
    {
        GrpcTrySetManyKeyValueRequest request = new();

        if (items.Count > 0)
            request.Items.Capacity = items.Count;

        foreach (KahunaSetKeyValueRequestItem item in items)
            request.Items.Add(Convert(item));

        return request;
    }
}
