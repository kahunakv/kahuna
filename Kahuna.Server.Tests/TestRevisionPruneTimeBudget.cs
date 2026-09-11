using System.Diagnostics;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The revision prune shares the single background writer with the flush. Its targeted pass used to
/// check its time budget only between 64-key chunks, and the backend-wide sweep had no budget at all
/// and stepped every history row in the column family — on a hot store that was seconds of writer
/// time every cleanup interval, taken from the flush while committed writes kept arriving (the
/// five-minute backlog spikes on the 1.7.8-flusher.1 soak). The RocksDB backend now takes the budget
/// itself: it stops starting keys once the budget elapses, pauses a sweep on row progress as well,
/// resumes from where it stopped, and jumps a sweep over each key's revision block instead of
/// stepping it. These tests pin that contract and that nothing is skipped or double-counted by the
/// pauses and jumps.
/// </summary>
public sealed class TestRevisionPruneTimeBudget : IDisposable
{
    private readonly string path = Path.Combine(Path.GetTempPath(), "kahuna-prune-budget-" + Guid.NewGuid().ToString("N"));

    private readonly RocksDbPersistenceBackend backend;

    public TestRevisionPruneTimeBudget()
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

    /// <summary>Writes <paramref name="keyCount"/> keys with <paramref name="depth"/> revisions each, every
    /// non-head revision old enough to be deletable under a 30-second age policy.</summary>
    private List<string> Seed(string bucket, int keyCount, int depth)
    {
        long old = NowMs - 120_000;
        List<string> keys = new(keyCount);
        List<PersistenceRequestItem> items = [];

        for (int k = 0; k < keyCount; k++)
        {
            string key = $"{bucket}/k{k:D5}";
            keys.Add(key);
            for (long r = 1; r <= depth; r++)
                items.Add(Item(key, r, r == depth ? NowMs : old + r));

            if (items.Count >= 4096)
            {
                Assert.True(backend.StoreKeyValues(items));
                items.Clear();
            }
        }

        if (items.Count > 0)
            Assert.True(backend.StoreKeyValues(items));

        return keys;
    }

    private static readonly TimeSpan Age = TimeSpan.FromSeconds(30);

    [Fact]
    public void TargetedPass_StopsBeforeTheNextKeyOnceTheBudgetElapses_AndReportsTheRest()
    {
        List<string> keys = Seed("budget/targeted", 400, 40);

        // A budget that is already spent: exactly one key runs (progress is guaranteed), the rest
        // come back as remaining with the time flag set.
        Assert.True(backend.PruneKeyValueRevisions(keys, 0, Age, 1_000_000, HLCTimestamp.Zero, TimeSpan.Zero, out RevisionPruneResult spent));
        Assert.Equal(1, spent.KeysVisited);
        Assert.True(spent.BatchLimitReached);
        Assert.True(spent.TimeBudgetExhausted);
        Assert.NotNull(spent.RemainingKeys);
        Assert.Equal(keys.Count - 1, spent.RemainingKeys!.Count);
        Assert.Equal(39, spent.RevisionsDeleted);

        // Repeated budgeted calls over the remaining keys converge: every key visited exactly once,
        // every deletable row gone, and the final call reports no backlog.
        IReadOnlyCollection<string> pending = spent.RemainingKeys!;
        int visited = spent.KeysVisited;
        int deleted = spent.RevisionsDeleted;
        int passes = 0;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(pending, 0, Age, 1_000_000, HLCTimestamp.Zero, TimeSpan.FromMilliseconds(2), out RevisionPruneResult r));
            passes++;
            visited += r.KeysVisited;
            deleted += r.RevisionsDeleted;
            Assert.True(r.KeysVisited >= 1);

            if (!r.BatchLimitReached)
            {
                Assert.Null(r.RemainingKeys);
                Assert.False(r.TimeBudgetExhausted);
                break;
            }

            Assert.True(r.TimeBudgetExhausted, "a pass that stopped under a huge delete budget must attribute the stop to time");
            Assert.NotNull(r.RemainingKeys);
            Assert.Equal(pending.Count - r.KeysVisited, r.RemainingKeys!.Count);
            pending = r.RemainingKeys!;
            Assert.True(passes < 10_000, "the budgeted pass is not making progress");
        }

        Assert.Equal(keys.Count, visited);
        Assert.Equal(keys.Count * 39, deleted);
        Assert.Null(backend.GetKeyValueRevision(keys[^1], 1));
        Assert.NotNull(backend.GetKeyValueRevision(keys[^1], 40));

        // Unbounded and unbudgeted forms agree on "nothing left".
        Assert.True(backend.PruneKeyValueRevisions(keys, 0, Age, 1_000_000, HLCTimestamp.Zero, out RevisionPruneResult again));
        Assert.Equal(0, again.RevisionsDeleted);
        Assert.False(again.BatchLimitReached);
    }

    [Fact]
    public void Sweep_PausesOnItsBudget_ResumesFromItsCursor_AndVisitsEveryKeyExactlyOnce()
    {
        // Deep blocks (well past the 8-row seek threshold) so the sweep must jump over history rows,
        // and enough keys that a 1 ms budget cannot finish a pass.
        const int keyCount = 3_000;
        const int depth = 24;
        List<string> keys = Seed("budget/sweep", keyCount, depth);

        int visited = 0;
        int deleted = 0;
        int passes = 0;
        int budgetPauses = 0;

        while (true)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, TimeSpan.FromMilliseconds(1), out RevisionPruneResult r));
            passes++;
            visited += r.KeysVisited;
            deleted += r.RevisionsDeleted;

            if (r.TimeBudgetExhausted)
            {
                budgetPauses++;
                Assert.True(r.BatchLimitReached, "a time pause must also report backlog so the writer resumes the sweep");
            }

            if (!r.BatchLimitReached)
                break;

            Assert.True(passes < 100_000, "the sweep is not making progress");
        }

        Assert.True(budgetPauses > 0, $"a 1 ms budget over {keyCount} keys should have paused at least once ({passes} passes)");
        Assert.True(passes > 1);

        // Every key seen exactly once across the passes, every deletable row gone.
        Assert.Equal(keyCount, visited);
        Assert.Equal(keyCount * (depth - 1), deleted);
        Assert.Null(backend.GetKeyValueRevision(keys[0], 1));
        Assert.Null(backend.GetKeyValueRevision(keys[^1], depth - 1));
        Assert.NotNull(backend.GetKeyValueRevision(keys[^1], depth));

        // The cursor wrapped: a fresh unbudgeted sweep starts over, sees every key, deletes nothing.
        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, out RevisionPruneResult wrap));
        Assert.Equal(keyCount, wrap.KeysVisited);
        Assert.Equal(0, wrap.RevisionsDeleted);
        Assert.False(wrap.BatchLimitReached);
    }

    [Fact]
    public void Sweep_JumpsOverRevisionBlocks_SoItsCostTracksKeysNotRows()
    {
        // Two stores would be cleaner, but the seek is a pure iterator move: measure one store before
        // and after deepening every block ten-fold. Rows grow 10x; a row-stepping sweep would too.
        const int keyCount = 500;
        List<string> keys = Seed("budget/jump", keyCount, 10);

        // First sweep walks every key once (priming the memo) and deletes the nine old rows, leaving
        // the head row only. History is then re-seeded INSIDE the window, so the blocks are deep but
        // nothing is deletable and a timed pass measures pure step/seek cost.
        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, out _));

        long fresh = NowMs - 1_000;
        void Deepen(int fromRev, int toRev)
        {
            List<PersistenceRequestItem> items = [];
            foreach (string key in keys)
            {
                for (long r = fromRev; r <= toRev; r++)
                    items.Add(Item(key, r, fresh + r));
                if (items.Count >= 4096) { Assert.True(backend.StoreKeyValues(items)); items.Clear(); }
            }
            if (items.Count > 0) Assert.True(backend.StoreKeyValues(items));
        }

        Deepen(11, 40);
        // Memo warm-up for the new history, then time the steady-state pass.
        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, out _));
        TimeSpan shallow = TimeSweep(keys.Count);

        Deepen(41, 340);
        Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, out _));
        TimeSpan deep = TimeSweep(keys.Count);

        // Rows per key went 30 -> 330 (11x). With row stepping the pass would scale with it; with the
        // jump it stays within a small factor. Generous bound: timing on a shared CI box.
        Assert.True(deep < shallow * 4 + TimeSpan.FromMilliseconds(20),
            $"sweep did not jump over revision blocks: {shallow.TotalMilliseconds:F1} ms at 30 rows/key vs {deep.TotalMilliseconds:F1} ms at 330 rows/key");
    }

    private TimeSpan TimeSweep(int expectedKeys)
    {
        TimeSpan best = TimeSpan.MaxValue;
        for (int i = 0; i < 3; i++)
        {
            Stopwatch sw = Stopwatch.StartNew();
            Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, out RevisionPruneResult r));
            sw.Stop();
            Assert.Equal(expectedKeys, r.KeysVisited);
            Assert.Equal(0, r.RevisionsDeleted);
            Assert.False(r.BatchLimitReached);
            if (sw.Elapsed < best) best = sw.Elapsed;
        }
        return best;
    }

    [Fact]
    public void Sweep_NeverSkipsATildeSiblingKey_WhenJumping()
    {
        // "a/x~1".."a/x~30" are revision rows of key "a/x"; "a/x~1sib" is a different logical key whose
        // rows ("a/x~1sib~1".., "a/x~1sib~CURRENT") sort between "a/x~19" and "a/x~2", i.e. inside the
        // run a jump would skip. The registry marks bucket "a/" as tilde-bearing, so the sweep must
        // step there, not jump, and must still visit the sibling; bucket "b/" is free to jump.
        long old = NowMs - 120_000;
        List<PersistenceRequestItem> items = [];
        for (long r = 1; r <= 30; r++)
            items.Add(Item("a/x", r, r == 30 ? NowMs : old + r));
        for (long r = 1; r <= 12; r++)
            items.Add(Item("a/x~1sib", r, r == 12 ? NowMs : old + r));
        for (long r = 1; r <= 12; r++)
            items.Add(Item("b/plain", r, r == 12 ? NowMs : old + r));
        Assert.True(backend.StoreKeyValues(items));

        int visited = 0;
        int deleted = 0;
        for (int i = 0; i < 1000; i++)
        {
            Assert.True(backend.PruneKeyValueRevisions(null, 0, Age, 1_000_000, HLCTimestamp.Zero, TimeSpan.FromTicks(1), out RevisionPruneResult r));
            visited += r.KeysVisited;
            deleted += r.RevisionsDeleted;
            if (!r.BatchLimitReached)
                break;
        }

        Assert.Equal(3, visited);
        Assert.Equal(29 + 11 + 11, deleted);
        Assert.Null(backend.GetKeyValueRevision("a/x~1sib", 1));
        Assert.NotNull(backend.GetKeyValueRevision("a/x~1sib", 12));
        Assert.NotNull(backend.GetKeyValue("a/x~1sib"));
    }
}
