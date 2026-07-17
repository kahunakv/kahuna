using System.Buffers;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that a transaction script produces identical results regardless of how the
/// input memory is backed: a plain array, a non-zero-offset slice shorter than its backing
/// array, or a non-array-backed buffer. This guards the memory-typed script entry point
/// against reading the whole backing array or assuming array-backing anywhere downstream.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestScriptMemoryInput
{
    private readonly ILoggerFactory loggerFactory;

    public TestScriptMemoryInput(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// Wraps a byte[] so its <see cref="ReadOnlyMemory{T}"/> is not array-backed:
    /// <c>MemoryMarshal.TryGetArray</c> returns false, forcing any consumer onto the
    /// span-only path rather than an array shortcut.
    /// </summary>
    private sealed class NonArrayBackedMemory(byte[] data) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => data;

        public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException();

        public override void Unpin() { }

        protected override void Dispose(bool disposing) { }
    }

    /// <summary>
    /// Returns a view over only <paramref name="scriptBytes"/>, embedded in a larger backing
    /// array with bytes on both sides that would break parsing if the whole array were read.
    /// A correct consumer honours the offset and length and never sees the surrounding bytes.
    /// </summary>
    private static ReadOnlyMemory<byte> OffsetSlice(byte[] scriptBytes)
    {
        byte[] prefix = Encoding.UTF8.GetBytes("GARBAGE ");
        byte[] suffix = Encoding.UTF8.GetBytes(" GARBAGE");

        byte[] backing = new byte[prefix.Length + scriptBytes.Length + suffix.Length];
        prefix.CopyTo(backing, 0);
        scriptBytes.CopyTo(backing, prefix.Length);
        suffix.CopyTo(backing, prefix.Length + scriptBytes.Length);

        return new ReadOnlyMemory<byte>(backing, prefix.Length, scriptBytes.Length);
    }

    private async Task<EmbeddedKahunaNode> StartNodeAsync()
    {
        EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        await node.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);

        return node;
    }

    [Fact]
    public async Task IdenticalResultAcrossBackingShapes()
    {
        await using EmbeddedKahunaNode node = await StartNodeAsync();

        const string script = """
        LET x = 41
        RETURN x + 1
        """;
        byte[] scriptBytes = Encoding.UTF8.GetBytes(script);

        // (a) plain array — the reference outcome.
        KeyValueTransactionResult fromArray =
            await node.Kahuna.TryExecuteTransactionScript(scriptBytes, null, null);

        // (b) non-zero-offset slice shorter than its backing array.
        KeyValueTransactionResult fromSlice =
            await node.Kahuna.TryExecuteTransactionScript(OffsetSlice(scriptBytes), null, null);

        // (c) non-array-backed memory (MemoryManager).
        using NonArrayBackedMemory owner = new(scriptBytes);
        KeyValueTransactionResult fromNonArray =
            await node.Kahuna.TryExecuteTransactionScript(owner.Memory, null, null);

        Assert.Equal(KeyValueResponseType.Get, fromArray.Type);
        Assert.Equal("42", Encoding.UTF8.GetString(fromArray.Value ?? []));

        // The slice must yield exactly the logical script's result — proving offset and length
        // are honoured and none of the surrounding "GARBAGE" bytes reach the parser.
        Assert.Equal(fromArray.Type, fromSlice.Type);
        Assert.Equal(fromArray.Revision, fromSlice.Revision);
        Assert.Equal(fromArray.Value ?? [], fromSlice.Value ?? []);

        // Non-array-backed memory must behave identically — no path may assume array-backing.
        Assert.Equal(fromArray.Type, fromNonArray.Type);
        Assert.Equal(fromArray.Revision, fromNonArray.Revision);
        Assert.Equal(fromArray.Value ?? [], fromNonArray.Value ?? []);
    }

    [Fact]
    public async Task SlicedMutationScriptAffectsOnlyTheLogicalBytes()
    {
        await using EmbeddedKahunaNode node = await StartNodeAsync();

        string key = "tenant/table/" + Guid.NewGuid().ToString("N")[..10];
        string script = $"SET `{key}` 'sliced-value'";
        byte[] scriptBytes = Encoding.UTF8.GetBytes(script);

        // Drive a real mutation through a non-zero-offset slice.
        KeyValueTransactionResult setResult =
            await node.Kahuna.TryExecuteTransactionScript(OffsetSlice(scriptBytes), null, null);
        Assert.Equal(KeyValueResponseType.Set, setResult.Type);

        // The write landed under the exact logical key, with the exact logical value.
        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) =
            await node.Kahuna.LocateAndTryGetValue(
                Kommander.Time.HLCTimestamp.Zero,
                key,
                -1,
                Kommander.Time.HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, getType);
        Assert.Equal("sliced-value", Encoding.UTF8.GetString(entry?.Value ?? []));
    }

    [Fact]
    public async Task EmptyAndMalformedScriptsBehaveIdenticallyAcrossShapes()
    {
        await using EmbeddedKahunaNode node = await StartNodeAsync();

        // Empty script: the array form and the memory form must reach the same outcome
        // (and neither is a successful execution).
        KeyValueTransactionResult emptyArray =
            await node.Kahuna.TryExecuteTransactionScript(Array.Empty<byte>(), null, null);
        KeyValueTransactionResult emptyMemory =
            await node.Kahuna.TryExecuteTransactionScript(ReadOnlyMemory<byte>.Empty, null, null);

        Assert.Equal(emptyArray.Type, emptyMemory.Type);
        Assert.NotEqual(KeyValueResponseType.Get, emptyArray.Type);

        // Malformed script: same error outcome whether array-backed or a non-zero-offset slice.
        byte[] malformed = Encoding.UTF8.GetBytes("this is not a valid kahuna script");

        KeyValueTransactionResult malformedArray =
            await node.Kahuna.TryExecuteTransactionScript(malformed, null, null);
        KeyValueTransactionResult malformedSlice =
            await node.Kahuna.TryExecuteTransactionScript(OffsetSlice(malformed), null, null);

        Assert.Equal(KeyValueResponseType.Errored, malformedArray.Type);
        Assert.Equal(malformedArray.Type, malformedSlice.Type);
    }
}
