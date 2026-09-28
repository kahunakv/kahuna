using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The coordinator holds every revision a transaction staged, and it is the only party that can tell a
/// restaging that continues the transaction's own pin from one that started over from the committed head
/// after a leader change dropped the pin. These tests drive completed operations through the session
/// registry and assert when <see cref="TransactionContext.StagedChainBreak"/> is set: a restaging must sit
/// exactly one revision above the previous one (or on it, after an extend), a point read of a staged key
/// must answer the staged revision, and a lock grant's base observation is not held to that rule.
/// </summary>
public sealed class TestStagedChainContinuity
{
    private const string Key = "chain/key";

    private static TransactionContext NewContext() => new()
    {
        TransactionId = new HLCTimestamp(1, 100, 0),
        CoordinatorKey = "coord",
        Timeout = 5000
    };

    private static TransactionOperationId Op(int n) => new((ulong)n, 0);

    private static HLCTimestamp Stamp(int n) => new(1, 1_000 + n, 0);

    private static void CompleteWrite(TransactionContext ctx, int op, long revision, HLCTimestamp stampedAt, KeyValueState state = KeyValueState.Set, long expiresMs = 0)
    {
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(op), state == KeyValueState.Set ? OperationKind.Set : OperationKind.Delete, null).Outcome);
        OperationCompletionPayload payload = new()
        {
            ModifiedKey = Key,
            AcquiredPointLock = Key,
            Durability = KeyValueDurability.Persistent,
            StagedMutations = [new StagedMutationEffect(Key, state == KeyValueState.Set ? [1] : null, state, revision, expiresMs, false, stampedAt)]
        };
        ctx.CompleteOperation(Op(op), payload, null);
    }

    private static void CompleteObservation(TransactionContext ctx, int op, OperationKind kind, bool exists, long revision)
    {
        Assert.Equal(OperationRegistrationOutcome.New, ctx.BeginOperation(Op(op), kind, null).Outcome);
        OperationCompletionPayload payload = new()
        {
            Durability = KeyValueDurability.Persistent,
            Read = new KeyValueTransactionReadKey { Key = Key, Durability = KeyValueDurability.Persistent, Exists = exists, Revision = revision }
        };
        ctx.CompleteOperation(Op(op), payload, null);
    }

    [Fact]
    public void RestagingOneRevisionAbove_KeepsTheChain()
    {
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteWrite(ctx, 2, revision: 6, Stamp(2));
        CompleteWrite(ctx, 3, revision: 7, Stamp(3), KeyValueState.Deleted);
        CompleteWrite(ctx, 4, revision: 8, Stamp(4));

        Assert.Null(ctx.StagedChainBreak);
        Assert.Equal(8, ctx.StagedMutations![Key].Revision);
    }

    [Fact]
    public void RestagingAtTheSameRevision_UnderANewStamp_BreaksTheChain()
    {
        // The second staging was allocated from a fresh pin at the committed head: the first staging is gone.
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteWrite(ctx, 2, revision: 5, Stamp(2));

        Assert.NotNull(ctx.StagedChainBreak);
        Assert.Contains(Key, ctx.StagedChainBreak);
    }

    [Fact]
    public void RestagingAboveTheChain_BreaksIt()
    {
        // Two commits landed on the key between the stagings, which a held pin never allows.
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteWrite(ctx, 2, revision: 8, Stamp(2));

        Assert.NotNull(ctx.StagedChainBreak);
    }

    [Fact]
    public void OneAnswerFoldedTwiceUnderTheSameStamp_IsNotARestaging()
    {
        // A batch that names a key twice folds the participant's single answer once per item.
        TransactionContext ctx = NewContext();
        ctx.StageMutation(Key, [1], KeyValueState.Set, 5, 0, false, Stamp(1));
        ctx.StageMutation(Key, [2], KeyValueState.Set, 5, 0, false, Stamp(1));

        Assert.Null(ctx.StagedChainBreak);
    }

    [Fact]
    public void ExtendKeepsTheStagedRevision_AndAnyOtherRevisionBreaksTheChain()
    {
        TransactionContext intact = NewContext();
        intact.StageMutation(Key, [1], KeyValueState.Set, 5, 0, false, Stamp(1));
        intact.StageMutation(Key, [1], KeyValueState.Set, 5, 1000, false, Stamp(2), advancesRevision: false);
        Assert.Null(intact.StagedChainBreak);

        TransactionContext broken = NewContext();
        broken.StageMutation(Key, [1], KeyValueState.Set, 5, 0, false, Stamp(1));
        broken.StageMutation(Key, [1], KeyValueState.Set, 4, 1000, false, Stamp(2), advancesRevision: false);
        Assert.NotNull(broken.StagedChainBreak);
    }

    [Fact]
    public void ReadOfAStagedKey_AtTheStagedRevision_KeepsTheChain()
    {
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteObservation(ctx, 2, OperationKind.Get, exists: true, revision: 5);
        CompleteObservation(ctx, 3, OperationKind.Exists, exists: true, revision: 5);

        Assert.Null(ctx.StagedChainBreak);
        Assert.Null(ctx.ReadKeys);
    }

    [Theory]
    [InlineData(OperationKind.Get)]
    [InlineData(OperationKind.GetMany)]
    public void ReadOfAStagedKey_AtAnotherRevision_BreaksTheChain(OperationKind kind)
    {
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteObservation(ctx, 2, kind, exists: true, revision: 4);

        Assert.NotNull(ctx.StagedChainBreak);
        Assert.Contains("read of key", ctx.StagedChainBreak);
    }

    [Fact]
    public void ReadOfAStagedSet_ThatAnswersAbsent_BreaksTheChain_UnlessTheSetCarriesATtl()
    {
        TransactionContext plain = NewContext();
        CompleteWrite(plain, 1, revision: 5, Stamp(1));
        CompleteObservation(plain, 2, OperationKind.Get, exists: false, revision: -1);
        Assert.NotNull(plain.StagedChainBreak);

        // A set with a TTL may lapse inside the transaction; its absence proves nothing.
        TransactionContext ttl = NewContext();
        CompleteWrite(ttl, 1, revision: 5, Stamp(1), expiresMs: 10);
        CompleteObservation(ttl, 2, OperationKind.Get, exists: false, revision: -1);
        Assert.Null(ttl.StagedChainBreak);
    }

    [Fact]
    public void ReadOfAStagedDelete_ThatAnswersPresent_BreaksTheChain()
    {
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1), KeyValueState.Deleted);
        CompleteObservation(ctx, 2, OperationKind.Get, exists: true, revision: 4);

        Assert.NotNull(ctx.StagedChainBreak);
    }

    [Fact]
    public void LockGrantBaseOnAStagedKey_IsNotHeldToTheStagedRevision()
    {
        // A point lock re-taken on a staged key answers the committed base, which is below the staging.
        TransactionContext ctx = NewContext();
        CompleteWrite(ctx, 1, revision: 5, Stamp(1));
        CompleteObservation(ctx, 2, OperationKind.PointLock, exists: true, revision: 4);
        CompleteObservation(ctx, 3, OperationKind.ManyPointLock, exists: true, revision: 4);

        Assert.Null(ctx.StagedChainBreak);
    }

    [Fact]
    public void ReadOfAKeyTheTransactionNeverStaged_EntersTheReadSet()
    {
        TransactionContext ctx = NewContext();
        CompleteObservation(ctx, 1, OperationKind.Get, exists: true, revision: 9);

        Assert.Null(ctx.StagedChainBreak);
        Assert.Equal(9, ctx.ReadKeys![(Key, KeyValueDurability.Persistent)].Revision);
    }
}
