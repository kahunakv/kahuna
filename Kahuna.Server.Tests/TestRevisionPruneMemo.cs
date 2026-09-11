using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The targeted revision prune runs inside every flush cycle for every key the cycle flushed, and
/// used to walk each key's whole revision block every time even when nothing was old enough to
/// delete — a cost that grows with the key's write rate and starved the flush (the 1.7.7 bank-soak
/// regression). The RocksDB backend now memoizes what a walk learned and answers later visits in
/// O(1) until something could actually be deletable. These tests pin the memo's contract: a skip is
/// only ever taken when a walk would delete nothing, and every event that could make rows deletable
/// (age crossing the cutoff, new history under count retention, a moved snapshot floor, a deleted
/// key, a recovery reopen) puts the key back on the walk path.
/// </summary>
public sealed class TestRevisionPruneMemo : IDisposable
{
    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-prune-memo-" + Guid.NewGuid().ToString("N"));

    private readonly RocksDbPersistenceBackend backend;

    public TestRevisionPruneMemo()
    {
        Directory.CreateDirectory(path);
        backend = new RocksDbPersistenceBackend(path, "v1");
    }

    public void Dispose()
    {
        backend.Dispose();
        try { Directory.Delete(path, recursive: true); } catch { /* best effort */ }
    }

    private static long NowMs => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static PersistenceRequestItem Item(string key, long revision, long physical) =>
        new(key, [1, 2, 3], revision, 0, 0, 0, 0, 0, 0, 0, physical, 0, (int)KeyValueState.Set);

    private RevisionPruneResult Prune(string key, int count = 0, TimeSpan age = default, int batchSize = 1000, HLCTimestamp floor = default)
    {
        Assert.True(backend.PruneKeyValueRevisions([key], count, age, batchSize, floor, out RevisionPruneResult result));
        return result;
    }

    [Fact]
    public void AgeRetention_SecondVisitIsSkipped_UntilARowCrossesTheCutoff()
    {
        const string key = "memo/age";
        TimeSpan age = TimeSpan.FromMinutes(10);

        // Revisions 1-3 well inside the window, 4 is the head.
        long fresh = NowMs - 60_000;
        Assert.True(backend.StoreKeyValues([Item(key, 1, fresh), Item(key, 2, fresh + 1), Item(key, 3, fresh + 2), Item(key, 4, fresh + 3)]));

        RevisionPruneResult first = Prune(key, age: age);
        Assert.Equal(1, first.KeysVisited);
        Assert.Equal(0, first.KeysSkipped);
        Assert.Equal(0, first.RevisionsDeleted);

        RevisionPruneResult second = Prune(key, age: age);
        Assert.Equal(1, second.KeysVisited);
        Assert.Equal(1, second.KeysSkipped);
        Assert.Equal(0, second.RevisionsDeleted);

        // A narrower window makes the same rows deletable: the memo must not suppress that walk.
        RevisionPruneResult narrowed = Prune(key, age: TimeSpan.FromSeconds(30));
        Assert.Equal(0, narrowed.KeysSkipped);
        Assert.Equal(3, narrowed.RevisionsDeleted);
        Assert.NotNull(backend.GetKeyValueRevision(key, 4));
        Assert.Null(backend.GetKeyValueRevision(key, 1));

        // Nothing non-current survives: skipped again, under either window.
        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(30)).KeysSkipped);
        Assert.Equal(1, Prune(key, age: age).KeysSkipped);
    }

    [Fact]
    public void AgeRetention_HeadAdvanceReleasesThePreviousHeadRow()
    {
        const string key = "memo/head";
        TimeSpan age = TimeSpan.FromSeconds(30);

        long old = NowMs - 120_000;
        Assert.True(backend.StoreKeyValues([Item(key, 1, old)]));

        // The head's own row is never deletable, so the memo says "nothing" — and is right.
        Assert.Equal(0, Prune(key, age: age).RevisionsDeleted);
        Assert.Equal(1, Prune(key, age: age).KeysSkipped);

        // Advancing the head turns revision 1's row into an old non-current row: the store path
        // must move the memo so the next visit walks and deletes it.
        Assert.True(backend.StoreKeyValues([Item(key, 2, NowMs)]));

        RevisionPruneResult afterAdvance = Prune(key, age: age);
        Assert.Equal(0, afterAdvance.KeysSkipped);
        Assert.Equal(1, afterAdvance.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValue(key));
    }

    [Fact]
    public void CountRetention_SkipsUntilNewHistoryExceedsTheBudget()
    {
        const string key = "memo/count";
        long now = NowMs;

        Assert.True(backend.StoreKeyValues([Item(key, 1, now), Item(key, 2, now + 1), Item(key, 3, now + 2)]));

        Assert.Equal(0, Prune(key, count: 3).RevisionsDeleted);
        Assert.Equal(1, Prune(key, count: 3).KeysSkipped);

        // A fourth revision pushes the oldest past the count: the memo must walk again.
        Assert.True(backend.StoreKeyValues([Item(key, 4, now + 3)]));

        RevisionPruneResult grown = Prune(key, count: 3);
        Assert.Equal(0, grown.KeysSkipped);
        Assert.Equal(1, grown.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValueRevision(key, 2));

        Assert.Equal(1, Prune(key, count: 3).KeysSkipped);
    }

    [Fact]
    public void DeleteBudgetExhausted_DropsTheMemoSoTheKeyWalksAgain()
    {
        const string key = "memo/budget";
        long old = NowMs - 120_000;

        List<PersistenceRequestItem> items = [];
        for (long r = 1; r <= 6; r++)
            items.Add(Item(key, r, old + r));
        items.Add(Item(key, 7, NowMs));
        Assert.True(backend.StoreKeyValues(items));

        RevisionPruneResult partial = Prune(key, age: TimeSpan.FromSeconds(30), batchSize: 2);
        Assert.Equal(2, partial.RevisionsDeleted);
        Assert.True(partial.BatchLimitReached);

        RevisionPruneResult rest = Prune(key, age: TimeSpan.FromSeconds(30), batchSize: 100);
        Assert.Equal(0, rest.KeysSkipped);
        Assert.Equal(4, rest.RevisionsDeleted);

        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(30)).KeysSkipped);
    }

    [Fact]
    public void FloorProtection_SkipsWhileTheFloorStands_WalksOnceItMoves()
    {
        const string key = "memo/floor";
        long old = NowMs - 120_000;

        Assert.True(backend.StoreKeyValues([Item(key, 1, old), Item(key, 2, old + 1), Item(key, 3, old + 2), Item(key, 4, NowMs)]));

        // Floor at revision 1's commit time: revisions 1..3 are the boundary and newer — protected.
        HLCTimestamp floor = new(0, old, 0);
        RevisionPruneResult blocked = Prune(key, age: TimeSpan.FromSeconds(30), floor: floor);
        Assert.Equal(0, blocked.RevisionsDeleted);
        Assert.Equal(0, blocked.FloorViolations);

        // Same floor: nothing can have changed, so the memo skips.
        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(30), floor: floor).KeysSkipped);

        // The floor moves past revision 2: revision 1 becomes deletable, the memo must walk.
        HLCTimestamp moved = new(0, old + 1, 0);
        RevisionPruneResult released = Prune(key, age: TimeSpan.FromSeconds(30), floor: moved);
        Assert.Equal(0, released.KeysSkipped);
        Assert.Equal(1, released.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValueRevision(key, 2));

        // Floor lifted entirely: the remaining old rows go.
        RevisionPruneResult lifted = Prune(key, age: TimeSpan.FromSeconds(30));
        Assert.Equal(0, lifted.KeysSkipped);
        Assert.Equal(2, lifted.RevisionsDeleted);
    }

    [Fact]
    public void FloorBlockedKey_StillWalksWhenAnUnprotectedRowAgesPastTheCutoff()
    {
        const string key = "memo/floor-age";
        long now = NowMs;

        // rev1 is the youngest-but-lowest revision (90 s old); rev2 (100 s old) is the floor boundary,
        // so rev2 and everything above it is protected while rev1 is not.
        Assert.True(backend.StoreKeyValues([Item(key, 1, now - 90_000), Item(key, 2, now - 100_000), Item(key, 3, now - 50_000), Item(key, 4, now)]));
        HLCTimestamp floor = new(0, now - 95_000, 0);

        // Age 95 s: rev2 is deletable but protected → floor-blocked; rev1 (90 s) is not old enough.
        RevisionPruneResult blocked = Prune(key, age: TimeSpan.FromSeconds(95), floor: floor);
        Assert.Equal(0, blocked.RevisionsDeleted);
        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(95), floor: floor).KeysSkipped);

        // Same floor, but the cutoff now passes rev1: the memo must not hide it behind the block.
        RevisionPruneResult aged = Prune(key, age: TimeSpan.FromSeconds(80), floor: floor);
        Assert.Equal(0, aged.KeysSkipped);
        Assert.Equal(1, aged.RevisionsDeleted);
        Assert.Null(backend.GetKeyValueRevision(key, 1));
        Assert.NotNull(backend.GetKeyValueRevision(key, 2));

        // Blocked again on rev2 only; nothing unprotected remains, so the key skips.
        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(80), floor: floor).KeysSkipped);
    }

    [Fact]
    public void DeletedKeyAndClearedMemos_WalkAgain()
    {
        const string key = "memo/delete";
        Assert.True(backend.StoreKeyValues([Item(key, 1, NowMs)]));

        Prune(key, age: TimeSpan.FromMinutes(1));
        Assert.Equal(1, Prune(key, age: TimeSpan.FromMinutes(1)).KeysSkipped);
        Assert.Equal(1, backend.PruneMemoCount);

        Assert.True(backend.DeleteKeyValues([key]));
        Assert.Equal(0, backend.PruneMemoCount);

        // Recreated with old history: the fresh walk sees it.
        long old = NowMs - 120_000;
        Assert.True(backend.StoreKeyValues([Item(key, 1, old), Item(key, 2, NowMs)]));
        RevisionPruneResult recreated = Prune(key, age: TimeSpan.FromSeconds(30));
        Assert.Equal(0, recreated.KeysSkipped);
        Assert.Equal(1, recreated.RevisionsDeleted);

        Assert.Equal(1, Prune(key, age: TimeSpan.FromSeconds(30)).KeysSkipped);
        backend.ClearPruneMemos();
        Assert.Equal(0, Prune(key, age: TimeSpan.FromSeconds(30)).KeysSkipped);
    }

    [Fact]
    public void MemoNeverSkipsAWalkThatWouldDelete_RandomizedAgainstAFreshWalk()
    {
        // Property check: after any interleaving of stores and prunes, a visit the memo skips must
        // agree with a forced walk (memo cleared) that nothing is deletable.
        Random random = new(1234);
        string[] keys = Enumerable.Range(0, 12).Select(i => $"memo/rand/{i}").ToArray();
        long[] nextRevision = new long[keys.Length];
        TimeSpan age = TimeSpan.FromSeconds(45);
        const int count = 4;

        for (int step = 0; step < 400; step++)
        {
            int k = random.Next(keys.Length);

            switch (random.Next(4))
            {
                case 0:
                case 1:
                {
                    // Store 1..3 new revisions, some backdated past the cutoff.
                    List<PersistenceRequestItem> items = [];
                    int n = 1 + random.Next(3);
                    for (int i = 0; i < n; i++)
                    {
                        long physical = random.Next(3) == 0 ? NowMs - 120_000 - random.Next(1000) : NowMs - random.Next(1000);
                        items.Add(Item(keys[k], ++nextRevision[k], physical));
                    }
                    Assert.True(backend.StoreKeyValues(items));
                    break;
                }
                case 2:
                {
                    int policyCount = random.Next(2) == 0 ? count : 0;
                    TimeSpan policyAge = random.Next(2) == 0 ? age : default;
                    if (policyCount == 0 && policyAge == default)
                        policyAge = age;

                    RevisionPruneResult r = Prune(keys[k], count: policyCount, age: policyAge, batchSize: 1 + random.Next(4));
                    if (r.KeysSkipped == 1)
                    {
                        // A skip claims a walk under the same policy would delete nothing: force that walk.
                        backend.ClearPruneMemos();
                        RevisionPruneResult forced = Prune(keys[k], count: policyCount, age: policyAge, batchSize: 1000);
                        Assert.Equal(0, forced.KeysSkipped);
                        Assert.Equal(0, forced.RevisionsDeleted);
                    }
                    break;
                }
                default:
                {
                    if (random.Next(6) == 0)
                        Assert.True(backend.DeleteKeyValues([keys[k]]));
                    break;
                }
            }
        }
    }
}
