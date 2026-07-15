using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for <see cref="TransactionOperationId.Derive"/> — the deterministic per-sub-step id used to
/// register each page of a paged scan under its own operation id.
/// </summary>
public sealed class TestTransactionOperationId
{
    [Fact]
    public void Derive_IsStableForSameBaseAndIndex()
    {
        TransactionOperationId baseId = new(0x0123456789ABCDEFUL, 0xFEDCBA9876543210UL);

        for (int index = 0; index < 64; index++)
            Assert.Equal(baseId.Derive(index), baseId.Derive(index));
    }

    [Fact]
    public void Derive_YieldsDistinctIdsForDistinctIndices()
    {
        TransactionOperationId baseId = TransactionOperationId.NewRandom();

        HashSet<TransactionOperationId> seen = [];
        for (int index = 0; index < 4096; index++)
            Assert.True(seen.Add(baseId.Derive(index)), $"collision at index {index}");
    }

    [Fact]
    public void Derive_DiffersAcrossDistinctBaseIds()
    {
        TransactionOperationId a = new(1, 2);
        TransactionOperationId b = new(2, 1);

        for (int index = 0; index < 64; index++)
            Assert.NotEqual(a.Derive(index), b.Derive(index));
    }

    [Fact]
    public void Derive_ProducesNonEmptyId()
    {
        TransactionOperationId baseId = new(1, 1);

        for (int index = 0; index < 64; index++)
            Assert.False(baseId.Derive(index).IsEmpty);
    }
}
