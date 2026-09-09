using BenchmarkDotNet.Attributes;
using Google.Protobuf;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Microbenchmarks;

/// <summary>
/// Before/after for the decode of a routed batch read in
/// <c>GrpcInterNodeCommunication.GetReadOnlyKeyValueEntry</c>.
///
/// <para><b>Old</b> = <c>item.Value.IsEmpty ? null : item.Value.ToByteArray()</c>. That allocates a
/// second array for every non-empty payload, on top of the one the protobuf parser already filled,
/// and it reports a key that holds zero bytes as a key that holds no value. <b>New</b> =
/// <c>ByteStringPayload.GetArrayOrNull(item.HasValue, item.Value)</c>, which reads the field's
/// presence flag and hands over the parser's own array when that array is exactly the payload.</para>
///
/// <para>Both arms parse the same serialized response first, so the measurement is the whole decode
/// leg a receiving node runs — the protobuf parse included — not the extraction alone. Both build the
/// real <see cref="ReadOnlyKeyValueEntry"/>, so the per-item object allocation is present in both and
/// cancels out. Only the two extraction expressions differ, and they are transcribed from the
/// production decoder rather than called through it, because the old one no longer exists.</para>
///
/// <para>Read the allocation column as bytes per decoded response. The expected difference is the sum
/// of the non-empty payload lengths plus one array header each, so it is zero at a payload size of
/// 0 bytes and grows with both the item count and the payload size. The time column carries the copy
/// as well, but the parse dominates it at every size here.</para>
///
/// <para><b>Result</b> (net10.0 Release, .NET 10.0.10, Arm64 RyuJIT, concurrent server GC;
/// BenchmarkDotNet 0.15.8, 24 cases in 7m54s). Old → new, per decoded response:
/// <list type="bullet">
///   <item>0 B, any item count: 704 B → 704 B (1 item), 22,840 B → 22,840 B (64), 180,608 B →
///     180,608 B (512). No payload, so nothing to copy and nothing to save. Time is level at 1 and
///     64 items and ~2% worse at 512 items (40.8 µs → 41.7 µs): reading the presence flag and
///     handing back the parser's empty array costs a little more than answering null did, and
///     answering null there was the defect.</item>
///   <item>128 B: 984 B → 832 B (1 item, 0.94× time), 40,760 B → 31,032 B (64, 0.90×),
///     323,968 B → 246,144 B (512, 0.92×).</item>
///   <item>4096 B: 8,920 B → 4,800 B (1 item, 0.62× time), 548,664 B → 284,984 B (64, 0.59×),
///     4,387,200 B → 2,277,760 B (512, 0.59×).</item>
///   <item>65536 B: 131,800 B → 66,240 B (1 item, 0.37× time), 8,412,984 B → 4,217,144 B (64,
///     0.42×), 67,301,760 B → 33,735,040 B (512, 0.50×).</item>
/// </list>
/// Every measured allocation difference equals the predicted one exactly — the item count times the
/// payload size plus a 24-byte array header per item. At and above 4 KB payloads the decode also
/// takes about half the time, because the removed copy was the larger half of the work the decode
/// did after the parse. These are decode-leg numbers from one machine; they are not a cluster
/// throughput measurement.</para>
/// </summary>
[MemoryDiagnoser]
public class ManyValuesDecodeBenchmark
{
    // One key, a page of keys, and a batch large enough that the per-item cost is what is measured.
    [Params(1, 64, 512)]
    public int Items;

    // A key that holds zero bytes, a small record, a page-sized record, and a payload well into the
    // large object heap.
    [Params(0, 128, 4096, 65536)]
    public int ValueBytes;

    private byte[] wire = [];

    [GlobalSetup]
    public void SerializeResponse()
    {
        byte[] payload = new byte[ValueBytes];

        for (int i = 0; i < payload.Length; i++)
            payload[i] = (byte)i;

        GrpcTryGetManyValuesResponse response = new();

        for (int i = 0; i < Items; i++)
        {
            response.Items.Add(new GrpcTryGetManyValuesResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeGot,
                Key = "benchmark/key/" + i,
                Durability = GrpcKeyValueDurability.Persistent,
                Value = UnsafeByteOperations.UnsafeWrap(payload),
                Revision = i,
                ExpiresNode = 1,
                ExpiresPhysical = 1_700_000_000_000,
                ExpiresCounter = 0,
                LastUsedNode = 1,
                LastUsedPhysical = 1_700_000_000_001,
                LastUsedCounter = 0,
                LastModifiedNode = 1,
                LastModifiedPhysical = 1_700_000_000_002,
                LastModifiedCounter = (uint)i,
                State = GrpcKeyValueState.StateSet
            });
        }

        wire = response.ToByteArray();
    }

    [Benchmark(Baseline = true)]
    public long Copying()
    {
        GrpcTryGetManyValuesResponse response = GrpcTryGetManyValuesResponse.Parser.ParseFrom(wire);

        long consumed = 0;

        foreach (GrpcTryGetManyValuesResponseItem item in response.Items)
        {
            ReadOnlyKeyValueEntry? entry = BuildEntry(item, item.Value.IsEmpty ? null : item.Value.ToByteArray());

            consumed += entry?.Value?.Length ?? 0;
        }

        return consumed;
    }

    [Benchmark]
    public long Borrowing()
    {
        GrpcTryGetManyValuesResponse response = GrpcTryGetManyValuesResponse.Parser.ParseFrom(wire);

        long consumed = 0;

        foreach (GrpcTryGetManyValuesResponseItem item in response.Items)
        {
            ReadOnlyKeyValueEntry? entry = BuildEntry(item, ByteStringPayload.GetArrayOrNull(item.HasValue, item.Value));

            consumed += entry?.Value?.Length ?? 0;
        }

        return consumed;
    }

    /// <summary>
    /// The rest of the decoded entry, shared by both arms so the only difference between them is the
    /// payload extraction passed in.
    /// </summary>
    private static ReadOnlyKeyValueEntry? BuildEntry(GrpcTryGetManyValuesResponseItem item, byte[]? value)
    {
        if ((KeyValueResponseType)item.Type is not (KeyValueResponseType.Get or KeyValueResponseType.Exists))
            return null;

        return new(
            value,
            item.Revision,
            new HLCTimestamp(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
            new HLCTimestamp(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
            new HLCTimestamp(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
            (KeyValueState)item.State
        );
    }
}
