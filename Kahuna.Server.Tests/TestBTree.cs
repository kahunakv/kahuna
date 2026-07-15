
using Kahuna.Utils;

namespace Kahuna.Server.Tests;

public class TestBTree
{
    // ── Memory-retention regression tests ───────────────────────────────────────

    // Forces a full GC collection cycle and asserts the weak reference is dead.
    private static void AssertCollected(WeakReference weakRef)
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);
        Assert.False(weakRef.IsAlive, "Removed value must not remain reachable through a stale array slot");
    }

    // Returns a WeakReference to a large byte-array payload inserted under key.
    private static WeakReference InsertPayload(BTree<int, object> btree, int key)
    {
        // 1 MB payload — large enough that GC.Collect can't miss it if it's still rooted.
        object payload = new byte[1024 * 1024];
        btree.Insert(key, payload);
        return new WeakReference(payload);
    }

    // Structural invariant: no node may hold a populated slot beyond its logical extent. Merge and
    // borrow leave a *duplicate* of a still-live sibling/subtree in the vacated slot, so a WeakReference
    // on a value cannot observe those clears — but this whole-tree walk asserts every slot >= KeyCount is
    // cleared, which fails if any shrink path forgot to null a vacated slot. Keys inserted by these tests
    // are always >= 1, so a cleared int key slot (default 0) is unambiguous.
    private static void AssertNoStaleSlots(BTree<int, object> btree)
    {
        if (btree.Root is not null)
            AssertNodeNoStaleSlots(btree.Root);
    }

    private static void AssertNodeNoStaleSlots(Node<int, object> node)
    {
        for (int i = node.KeyCount; i < node.Keys.Length; i++)
            Assert.Equal(0, node.Keys[i]);

        if (node.IsLeaf)
        {
            for (int i = node.KeyCount; i < node.Values.Length; i++)
                Assert.Null(node.Values[i]);
            return;
        }

        // An internal node with KeyCount keys has KeyCount+1 live children [0, KeyCount]; every slot
        // above that must be null.
        for (int i = node.KeyCount + 1; i < node.Children.Length; i++)
            Assert.Null(node.Children[i]);

        for (int i = 0; i <= node.KeyCount; i++)
            AssertNodeNoStaleSlots(node.Children[i]!);
    }

    [Fact]
    public void RemovedValueIsCollectable_LastKeyOfLeaf()
    {
        // Removing the LAST key of a leaf is the case that exercises the trailing-slot clear: removing an
        // interior key already overwrites its slot via the shift-left, so it would pass without the fix.
        BTree<int, object> btree = new(4);
        btree.Insert(1, "other");
        WeakReference weakRef = InsertPayload(btree, 2);   // payload under the last key

        btree.Remove(2);

        AssertCollected(weakRef);
    }

    [Fact]
    public void RemovedValueIsCollectable_LeafRootDrainedToEmpty()
    {
        // This is the reported count=0, alive=True case: after draining the root leaf to zero
        // the Values array must not retain any reference to the last removed entry.
        BTree<int, object> btree = new(4);
        WeakReference weakRef = InsertPayload(btree, 1);

        btree.Remove(1);

        Assert.Equal(0, btree.Count);
        AssertCollected(weakRef);
    }

    // Borrow and merge leave a duplicate of a still-live sibling/subtree in the vacated slot, so they are
    // verified structurally (the value stays live either way — only the stale *slot* is the defect).

    [Fact]
    public void NoStaleSlots_AfterBorrowFromLeft()
    {
        // Order-4 tree, minKeys = 1. Drive an underflow whose left sibling can spare a key so the child
        // borrows from the left, then assert the donor left no populated slot behind.
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 6; i++)
            btree.Insert(i, $"v{i}");

        btree.Remove(4);
        btree.Remove(3);

        AssertNoStaleSlots(btree);
    }

    [Fact]
    public void NoStaleSlots_AfterBorrowFromRight()
    {
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 6; i++)
            btree.Insert(i, $"v{i}");

        btree.Remove(1);
        btree.Remove(2);

        AssertNoStaleSlots(btree);
    }

    [Fact]
    public void RemovedValueIsCollectable_BorrowFromRight()
    {
        // Leaves: [[1,2],[3,4],[5,6]]. Remove key 2 (frees payload), then 1 (underflow on left
        // leaf → borrow-from-right from [3,4]).
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 6; i++)
            btree.Insert(i, $"v{i}");

        WeakReference weakRef = InsertPayload(btree, 2);

        btree.Remove(2);
        btree.Remove(1);

        AssertCollected(weakRef);
    }

    [Fact]
    public void NoStaleSlots_AfterMergeWithLeft()
    {
        // Drain the right side so an underflowing node merges into its left sibling; the parent must not
        // keep a dangling separator key or child pointer in the vacated trailing slot.
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 9; i++)
            btree.Insert(i, $"v{i}");

        btree.Remove(9);
        btree.Remove(8);
        btree.Remove(7);

        AssertNoStaleSlots(btree);
    }

    [Fact]
    public void NoStaleSlots_AfterMergeWithRight()
    {
        // Drain the left side so an underflowing node merges its right sibling into itself.
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 9; i++)
            btree.Insert(i, $"v{i}");

        btree.Remove(1);
        btree.Remove(2);
        btree.Remove(3);

        AssertNoStaleSlots(btree);
    }

    [Fact]
    public void RemovedValueIsCollectable_LastKeyOfPostSplitSourceLeaf()
    {
        // After a split, the source (left) leaf is no longer the root. Put the payload under its last
        // key and remove it, exercising the trailing-slot clear on a non-root leaf reached via recursion.
        BTree<int, object> btree = new(4);
        btree.Insert(1, "v1");
        btree.Insert(2, "v2");
        btree.Insert(3, "v3");

        WeakReference weakRef = InsertPayload(btree, 2);   // updates key 2's value in leaf [1,2,3]
        btree.Insert(4, "v4");                             // splits → source [1,2(payload)], sibling [3,4]

        btree.Remove(2);                                   // remove the last key of the source leaf

        AssertCollected(weakRef);
    }

    [Fact]
    public void SplitMovedValueIsCollectable()
    {
        // Values moved into the sibling during SplitLeaf must not be double-rooted by stale
        // source-array slots — removing from the sibling must make them collectible.
        BTree<int, object> btree = new(4);
        btree.Insert(1, "v1");
        btree.Insert(2, "v2");
        btree.Insert(3, "v3");

        // Key 4 triggers SplitLeaf; the payload lands in the new sibling.
        WeakReference weakRef = InsertPayload(btree, 4);

        btree.Remove(4);

        AssertCollected(weakRef);
    }

    [Fact]
    public void NoStaleSlots_AfterHeavyInsertRemoveWorkload()
    {
        // Shape-independent catch-all: a large churn drives many splits, borrows, and merges across a
        // multi-level tree; the surviving nodes must carry no populated slot beyond their logical extent.
        BTree<int, object> btree = new(4);
        for (int i = 1; i <= 200; i++)
            btree.Insert(i, $"v{i}");

        // Remove most keys (interleaved) to churn structure while leaving a populated multi-level tree.
        for (int i = 1; i <= 200; i++)
            if (i % 4 != 0)
                btree.Remove(i);

        Assert.True(btree.Count > 0);
        AssertNoStaleSlots(btree);

        // Surviving keys still resolve correctly after all the structural churn.
        for (int i = 1; i <= 200; i++)
            if (i % 4 == 0)
                Assert.NotNull(btree.Get(i));
    }

    [Fact]
    public void LeafNodesHaveZeroLengthChildrenArray()
    {
        // F7: leaf nodes must not allocate a Children array; they share a static empty array.
        Node<int, string> leaf = new(4, true, Comparer<int>.Default);
        Assert.Empty(leaf.Children);

        // Internal nodes must still allocate order+1 slots.
        Node<int, string> inner = new(4, false, Comparer<int>.Default);
        Assert.Equal(5, inner.Children.Length);
    }

    [Fact]
    public void InternalNodesStillFunctionAfterSplitsAndMerges()
    {
        // Regression guard: F7 must not break correctness of internal-node Children.
        BTree<int, int> btree = new(4);
        for (int i = 0; i < 50; i++)
            btree.Insert(i, i * 10);

        for (int i = 0; i < 50; i++)
            Assert.Equal(i * 10, btree.Get(i));

        for (int i = 0; i < 50; i++)
            btree.Remove(i);

        Assert.Equal(0, btree.Count);
    }


    [Fact]
    public void TestBTreeBasic()
    {
        BTree<int, string> btree = new(4);
        
        btree.Insert(1, "one");
        btree.Insert(2, "two");
        btree.Insert(3, "three");
        
        Assert.True(btree.ContainsKey(1));        
        Assert.True(btree.TryGetValue(1, out string? value));        
        Assert.Equal("one", value);
        Assert.Equal("one", btree.Get(1));
        
        Assert.True(btree.ContainsKey(2));
        Assert.True(btree.TryGetValue(2, out value));
        Assert.Equal("two", value);
        Assert.Equal("two", btree.Get(2));
        
        Assert.True(btree.ContainsKey(3));
        Assert.True(btree.TryGetValue(3, out value));
        Assert.Equal("three", value);
        Assert.Equal("three", btree.Get(3));
        
        Assert.False(btree.ContainsKey(4));
        Assert.False(btree.TryGetValue(4, out value));
        Assert.Null(value);
        Assert.Null(btree.Get(4));
        
        Assert.Equal(3, btree.Count);
    }
    
    [Fact]
    public void TestBTreeInsertUpdatesValueOnDuplicateKey()
    {
        BTree<int, string> btree = new(4);

        btree.Insert(1, "one");
        btree.Insert(2, "two");
        btree.Insert(1, "uno");   // duplicate key ⇒ upsert, not throw

        Assert.Equal("uno", btree.Get(1));
        Assert.True(btree.TryGetValue(1, out string? value));
        Assert.Equal("uno", value);
        Assert.Equal(2, btree.Count);   // count unchanged by the update
    }

    [Fact]
    public void TestBTreeUpsertAcrossSplitsKeepsLatestValue()
    {
        BTree<int, int> btree = new(4);

        for (int i = 0; i < 200; i++)
            btree.Insert(i, i);

        // Re-insert every key with a new value; tree is multi-level by now.
        for (int i = 0; i < 200; i++)
            btree.Insert(i, i + 1000);

        Assert.Equal(200, btree.Count);
        for (int i = 0; i < 200; i++)
            Assert.Equal(i + 1000, btree.Get(i));
    }

    [Fact]
    public void TestBTreeTryGetValueReportsPresentNullValue()
    {
        // A key mapped to a null value is still present: TryGetValue must report it via the return
        // flag, not by null-checking the payload.
        BTree<string, string?> btree = new(4);

        btree.Insert("k", null);

        Assert.True(btree.ContainsKey("k"));
        Assert.True(btree.TryGetValue("k", out string? value));
        Assert.Null(value);

        Assert.False(btree.TryGetValue("missing", out value));
    }

    [Fact]
    public void TestBTreeBasicSplit()
    {
        BTree<int, string> btree = new(4);
        
        btree.Insert(1, "one");
        btree.Insert(2, "two");
        btree.Insert(3, "three");
        btree.Insert(4, "four");
        btree.Insert(5, "five");
        
        Assert.True(btree.ContainsKey(1));
        Assert.True(btree.TryGetValue(1, out string? value));
        Assert.Equal("one", value);
        
        Assert.True(btree.ContainsKey(2));
        Assert.True(btree.TryGetValue(2, out value));
        Assert.Equal("two", value);
        
        Assert.True(btree.ContainsKey(3));
        Assert.True(btree.TryGetValue(3, out value));
        Assert.Equal("three", value);
        
        Assert.True(btree.ContainsKey(4));
        Assert.True(btree.TryGetValue(4, out value));
        Assert.Equal("four", value);
        
        Assert.True(btree.ContainsKey(5));
        Assert.True(btree.TryGetValue(5, out value));
        Assert.Equal("five", value);
        
        Assert.Equal(5, btree.Count);
    }
    
    [Fact]
    public void TestBTreeBasicRemove()
    {
        BTree<int, string> btree = new(4);
        
        btree.Insert(1, "one");
        btree.Insert(2, "two");
        btree.Insert(3, "three");
        
        Assert.True(btree.ContainsKey(1));
        
        Assert.True(btree.TryGetValue(1, out string? value));
        Assert.Equal("one", value);
        
        Assert.True(btree.ContainsKey(2));
        Assert.True(btree.TryGetValue(2, out value));
        Assert.Equal("two", value);
        
        Assert.True(btree.ContainsKey(3));
        Assert.True(btree.TryGetValue(3, out value));
        Assert.Equal("three", value);
        
        Assert.False(btree.ContainsKey(4));
        Assert.False(btree.TryGetValue(4, out value));
        Assert.Null(value);
        
        btree.Remove(3);
        
        Assert.False(btree.ContainsKey(3));
        Assert.False(btree.TryGetValue(3, out value));
        Assert.Null(value);
    }

    [Fact]
    public void TestBTreeMany()
    {
        BTree<int, int> btree = new(16);
        
        for (int i = 0; i < 1000; i++)
            btree.Insert(i, i);

        for (int i = 0; i < 1000; i++)
            Assert.True(btree.ContainsKey(i));
        
        for (int i = 0; i < 1000; i++)
            btree.Remove(i);
        
        for (int i = 0; i < 1000; i++)
            Assert.False(btree.ContainsKey(i));

        Assert.Equal(0, btree.Count);
    }
    
    [Fact]
    public void TestBTreeMany2()
    {
        BTree<int, int> btree = new(16);

        for (int i = 0; i < 1000; i++)
        {
            if (i % 2 == 0)
                btree.Insert(i, i);
        }

        for (int i = 0; i < 1000; i++)
        {
            if (i % 2 == 0)
                Assert.True(btree.ContainsKey(i));
            else
                Assert.False(btree.ContainsKey(i));
        }

        for (int i = 0; i < 1000; i++)
            btree.Remove(i);
        
        for (int i = 0; i < 1000; i++)
            Assert.False(btree.ContainsKey(i));

        Assert.Equal(0, btree.Count);
    }
    
    [Fact]
    public void TestBTreeMany3()
    {
        BTree<string, int> btree = new(32);
        
        HashSet<string> keys = new(1000);

        for (int i = 0; i < 1000; i++)
        {
            string key = Guid.NewGuid().ToString();
            keys.Add(key);
            btree.Insert(key, i);
        }

        foreach (string key in keys)        
            Assert.True(btree.ContainsKey(key));
        
        HashSet<string> keys2 = new(1000);

        foreach (KeyValuePair<string, int> item in btree.GetItems())
        {
            Assert.Contains(item.Key, keys);
            keys2.Add(item.Key);
        }

        foreach (string key in keys2)        
            Assert.True(btree.ContainsKey(key));        

        int j = 0;

        foreach (string key in keys2)
        {
            if (j++ % 2 == 0)
                Assert.True(btree.Remove(key));   
        }
        
        Assert.Equal(500, btree.Count);
    }
    
    [Fact]
    public void TestBTreeMany4()
    {
        BTree<string, string> btree = new(32);
        
        Dictionary<string, string> keys = new(1000);

        for (int i = 0; i < 1000; i++)
        {
            string key = Guid.NewGuid().ToString();
            string value = Guid.NewGuid().ToString();
            
            keys.Add(key, value);
            btree.Insert(key, value);
        }

        foreach (KeyValuePair<string, string> kv in keys)        
            Assert.True(btree.ContainsKey(kv.Key));
        
        Dictionary<string, string> keys2 = new(1000);

        foreach (KeyValuePair<string, string> item in btree.GetItems())
        {
            Assert.Contains(item.Key, keys);
            keys2.Add(item.Key, item.Value);
        }

        foreach (KeyValuePair<string, string> kv in keys2)
        {
            Assert.True(btree.ContainsKey(kv.Key));
            Assert.NotNull(btree.Get(kv.Key));
            Assert.Equal(kv.Value, btree.Get(kv.Key));
        }

        int j = 0;

        foreach (KeyValuePair<string, string> kv in keys2)
        {
            if (j++ % 2 == 0)
                Assert.True(btree.Remove(kv.Key));   
        }
        
        Assert.Equal(500, btree.Count);
    }
    
    [Fact]
    public void TestBTreeBasicRange()
    {
        BTree<int, string> btree = new(4);
        
        btree.Insert(1, "one");
        btree.Insert(2, "two");
        btree.Insert(3, "three");
        btree.Insert(4, "four");
        btree.Insert(5, "five");
        btree.Insert(6, "six");
        btree.Insert(7, "seven");
        btree.Insert(8, "eight");
        btree.Insert(9, "nine");        
        
        Assert.True(btree.ContainsKey(1));
        
        Assert.True(btree.TryGetValue(1, out string? value));
        Assert.Equal("one", value);
        
        Assert.True(btree.ContainsKey(2));
        Assert.True(btree.TryGetValue(2, out value));
        Assert.Equal("two", value);
        
        Assert.True(btree.ContainsKey(3));
        Assert.True(btree.TryGetValue(3, out value));
        Assert.Equal("three", value);
        
        IEnumerable<KeyValuePair<int, string>> items = btree.GetItems();
        Assert.Equal(9, items.Count());
        
        items = btree.GetItems(3, 6);
        Assert.Equal(4, items.Count());
        
        items = btree.GetItems(0, 3);
        Assert.Equal(3, items.Count());
        
        items = btree.GetItems(8, 10);
        Assert.Equal(2, items.Count());
        
        Assert.Equal(9, btree.Count);
    }
    
    [Fact]
    public void TestBTreeBasicRange2()
    {
        BTree<string, string> btree = new(4);
        
        btree.Insert("services/001", "one");
        btree.Insert("services/002", "two");
        btree.Insert("services/003", "three");
        btree.Insert("services/004", "four");
        btree.Insert("services/005", "five");
        btree.Insert("services/006", "six");
        btree.Insert("services/007", "seven");
        btree.Insert("services/008", "eight");
        btree.Insert("services/009", "nine");        
        
        Assert.True(btree.ContainsKey("services/001"));
        
        Assert.True(btree.TryGetValue("services/001", out string? value));
        Assert.Equal("one", value);
        
        Assert.True(btree.ContainsKey("services/002"));
        Assert.True(btree.TryGetValue("services/002", out value));
        Assert.Equal("two", value);
        
        Assert.True(btree.ContainsKey("services/003"));
        Assert.True(btree.TryGetValue("services/003", out value));
        Assert.Equal("three", value);
        
        IEnumerable<KeyValuePair<string, string>> items = btree.GetItems();
        Assert.Equal(9, items.Count());
        
        items = btree.GetItems("services/003", "services/006");
        Assert.Equal(4, items.Count());
        
        items = btree.GetItems("services/000", "services/003");
        Assert.Equal(3, items.Count());
        
        items = btree.GetItems("services/008", "services/010");
        Assert.Equal(2, items.Count());
        
        items = btree.GetByBucket("services");
        Assert.Equal(9, items.Count());
        
        Assert.Equal(9, btree.Count);
    }
    
    [Fact]
    public void TestBTreeBasicRange3()
    {
        BTree<string, string> btree = new(32);

        for (int i = 0; i < 100; i++)
        {
            btree.Insert("services/" + i.ToString("D4"), i.ToString());    
            btree.Insert("config/" + i.ToString("D4"), i.ToString());
        }
        
        Assert.True(btree.ContainsKey("services/0001"));        
        Assert.True(btree.TryGetValue("services/0001", out string? value));
        Assert.Equal("1", value);
        
        Assert.True(btree.ContainsKey("services/0002"));
        Assert.True(btree.TryGetValue("services/0002", out value));
        Assert.Equal("2", value);
        
        Assert.True(btree.ContainsKey("services/0003"));
        Assert.True(btree.TryGetValue("services/0003", out value));
        Assert.Equal("3", value);
        
        IEnumerable<KeyValuePair<string, string>> items = btree.GetItems();
        Assert.Equal(200, items.Count());
        
        /*items = btree.GetItems("services/003", "services/006");
        Assert.Equal(4, items.Count());
        
        items = btree.GetItems("services/000", "services/003");
        Assert.Equal(3, items.Count());
        
        items = btree.GetItems("services/008", "services/010");
        Assert.Equal(2, items.Count());*/
        
        items = btree.GetByBucket("services");
        Assert.Equal(100, items.Count());
        
        items = btree.GetByBucket("config");
        Assert.Equal(100, items.Count());

        Assert.Equal(200, btree.Count);
    }

    /// <summary>
    /// Regression: GetByBucket must return every key under the bucket even when a *sibling*
    /// bucket name sorts differently under ordinal vs current-culture string comparison.
    ///
    /// The tree is ordered with String.CompareTo (current culture), where '~' (0x7E) sorts
    /// BEFORE 'r' (0x72); but GetByBucket's early-exit uses StringComparison.Ordinal, where
    /// '~' sorts AFTER 'r'. With a sibling bucket "{table}:i:~pk" present, its keys land first
    /// in the leaf chain and the ordinal early-`break` aborts the scan before any
    /// "{table}:i:robots_id_idx/..." key is reached — returning 0 instead of all matches.
    ///
    /// This is the exact key layout CamusDB produces ("~pk" primary-key index next to a
    /// secondary index), and it made read-your-own-writes index scans intermittently return
    /// no rows. GetByBucket must use one consistent (ordinal) comparison end to end.
    /// </summary>
    [Fact]
    public void TestGetByBucketWithCultureOrdinalDivergentSibling()
    {
        BTree<string, string> btree = new(32);

        const string table = "6a1b1bf86a4dc10c156599f8";
        string scanBucket = $"{table}:i:robots_id_idx";

        // Insert in a shuffled-ish order; final result must not depend on insertion order.
        btree.Insert($"{table}:i:~pk/k0", "u0");          // sibling unique index "~pk"
        btree.Insert($"{scanBucket}/k0", "m0");           // matching secondary-index entries
        btree.Insert($"{table}:r/k0", "r0");              // row entry
        btree.Insert($"{table}:i:~pk/k1", "u1");
        btree.Insert($"{scanBucket}/k1", "m1");
        btree.Insert($"{scanBucket}/k2", "m2");
        btree.Insert($"{table}:r/k1", "r1");

        // Sanity: every key is retrievable individually.
        Assert.True(btree.ContainsKey($"{scanBucket}/k0"));
        Assert.True(btree.ContainsKey($"{scanBucket}/k1"));
        Assert.True(btree.ContainsKey($"{scanBucket}/k2"));

        List<KeyValuePair<string, string>> matches = btree
            .GetByBucket(scanBucket)
            .Where(kv => kv.Key.StartsWith($"{scanBucket}/", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(3, matches.Count);
    }

    /// <summary>
    /// Same defect at scale: many "~pk" sibling keys interleaved with the scanned bucket and
    /// row keys across a multi-level tree. GetByBucket must still return all bucket members.
    /// </summary>
    [Fact]
    public void TestGetByBucketIgnoresTildeSiblingBucketAtScale()
    {
        BTree<string, string> btree = new(32);

        const string table = "6a1b1bf86a4dc10c156599f8";
        string scanBucket = $"{table}:i:robots_id_idx";

        for (int i = 0; i < 50; i++)
        {
            string suffix = i.ToString("D4");
            btree.Insert($"{scanBucket}/{suffix}", "m" + i);
            btree.Insert($"{table}:i:~pk/{suffix}", "u" + i);
            btree.Insert($"{table}:r/{suffix}", "r" + i);
        }

        int matches = btree
            .GetByBucket(scanBucket)
            .Count(kv => kv.Key.StartsWith($"{scanBucket}/", StringComparison.Ordinal));

        Assert.Equal(50, matches);
    }

    [Fact]
    public void TestGetByRangeInclusiveBothEnds()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/002", "b");
        btree.Insert("svc/003", "c");
        btree.Insert("svc/004", "d");
        btree.Insert("svc/005", "e");

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/002", true, "svc/004", true, 100)
            .ToList();

        Assert.Equal(3, items.Count);
        Assert.Equal("svc/002", items[0].Key);
        Assert.Equal("svc/003", items[1].Key);
        Assert.Equal("svc/004", items[2].Key);
    }

    [Fact]
    public void TestGetByRangeExclusiveBothEnds()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/002", "b");
        btree.Insert("svc/003", "c");
        btree.Insert("svc/004", "d");
        btree.Insert("svc/005", "e");

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/002", false, "svc/004", false, 100)
            .ToList();

        Assert.Single(items);
        Assert.Equal("svc/003", items[0].Key);
    }

    [Fact]
    public void TestGetByRangeInclusiveStartExclusiveEnd()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/002", "b");
        btree.Insert("svc/003", "c");
        btree.Insert("svc/004", "d");
        btree.Insert("svc/005", "e");

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/002", true, "svc/004", false, 100)
            .ToList();

        Assert.Equal(2, items.Count);
        Assert.Equal("svc/002", items[0].Key);
        Assert.Equal("svc/003", items[1].Key);
    }

    [Fact]
    public void TestGetByRangeExclusiveStartInclusiveEnd()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/002", "b");
        btree.Insert("svc/003", "c");
        btree.Insert("svc/004", "d");
        btree.Insert("svc/005", "e");

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/002", false, "svc/004", true, 100)
            .ToList();

        Assert.Equal(2, items.Count);
        Assert.Equal("svc/003", items[0].Key);
        Assert.Equal("svc/004", items[1].Key);
    }

    [Fact]
    public void TestGetByRangeLimitCap()
    {
        BTree<string, string> btree = new(4);

        for (int i = 1; i <= 10; i++)
            btree.Insert($"svc/{i:D3}", i.ToString());

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/001", true, null, false, 3)
            .ToList();

        Assert.Equal(3, items.Count);
        Assert.Equal("svc/001", items[0].Key);
        Assert.Equal("svc/002", items[1].Key);
        Assert.Equal("svc/003", items[2].Key);
    }

    [Fact]
    public void TestGetByRangeStartBetweenExistingKeys()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/003", "c");
        btree.Insert("svc/005", "e");
        btree.Insert("svc/007", "g");
        btree.Insert("svc/009", "i");

        // "svc/004" falls between existing keys 003 and 005
        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/004", true, "svc/008", true, 100)
            .ToList();

        Assert.Equal(2, items.Count);
        Assert.Equal("svc/005", items[0].Key);
        Assert.Equal("svc/007", items[1].Key);
    }

    [Fact]
    public void TestGetByRangeEmptyRange()
    {
        BTree<string, string> btree = new(4);

        btree.Insert("svc/001", "a");
        btree.Insert("svc/002", "b");
        btree.Insert("svc/003", "c");

        // start > end → empty
        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/003", true, "svc/001", true, 100)
            .ToList();

        Assert.Empty(items);
    }

    [Fact]
    public void TestGetByRangeNullEndScansToEndOfTree()
    {
        BTree<string, string> btree = new(4);

        for (int i = 1; i <= 5; i++)
            btree.Insert($"svc/{i:D3}", i.ToString());

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/003", true, null, false, 100)
            .ToList();

        Assert.Equal(3, items.Count);
        Assert.Equal("svc/003", items[0].Key);
        Assert.Equal("svc/004", items[1].Key);
        Assert.Equal("svc/005", items[2].Key);
    }

    [Fact]
    public void TestGetByRangeOrdinalOrder()
    {
        BTree<string, string> btree = new(16);

        // Insert keys that would sort differently under culture vs ordinal
        btree.Insert("svc/a", "1");
        btree.Insert("svc/b", "2");
        btree.Insert("svc/z", "3");
        btree.Insert("svc/~extra", "4");  // '~' (0x7E) > 'z' (0x7A) ordinal

        List<KeyValuePair<string, string>> items = btree
            .GetByRange("svc/a", true, "svc/z", true, 100)
            .ToList();

        Assert.Equal(3, items.Count);
        Assert.Equal("svc/a", items[0].Key);
        Assert.Equal("svc/b", items[1].Key);
        Assert.Equal("svc/z", items[2].Key);
        // "svc/~extra" is beyond "svc/z" in ordinal order and must not appear
    }

    /// <summary>
    /// Port of TestGetByBucketWithCultureOrdinalDivergentSibling to GetByRange.
    /// </summary>
    [Fact]
    public void TestGetByRangeWithCultureOrdinalDivergentSibling()
    {
        BTree<string, string> btree = new(32);

        const string table = "6a1b1bf86a4dc10c156599f8";
        string scanBucket = $"{table}:i:robots_id_idx";
        string rangeStart = $"{scanBucket}/";
        string rangeEnd = $"{scanBucket}/\xff\xff\xff\xff";

        btree.Insert($"{table}:i:~pk/k0", "u0");
        btree.Insert($"{scanBucket}/k0", "m0");
        btree.Insert($"{table}:r/k0", "r0");
        btree.Insert($"{table}:i:~pk/k1", "u1");
        btree.Insert($"{scanBucket}/k1", "m1");
        btree.Insert($"{scanBucket}/k2", "m2");
        btree.Insert($"{table}:r/k1", "r1");

        Assert.True(btree.ContainsKey($"{scanBucket}/k0"));
        Assert.True(btree.ContainsKey($"{scanBucket}/k1"));
        Assert.True(btree.ContainsKey($"{scanBucket}/k2"));

        List<KeyValuePair<string, string>> matches = btree
            .GetByRange(rangeStart, true, rangeEnd, true, 100)
            .Where(kv => kv.Key.StartsWith($"{scanBucket}/", StringComparison.Ordinal))
            .ToList();

        Assert.Equal(3, matches.Count);
    }

    /// <summary>
    /// Port of TestGetByBucketIgnoresTildeSiblingBucketAtScale to GetByRange.
    /// </summary>
    [Fact]
    public void TestGetByRangeIgnoresTildeSiblingBucketAtScale()
    {
        BTree<string, string> btree = new(32);

        const string table = "6a1b1bf86a4dc10c156599f8";
        string scanBucket = $"{table}:i:robots_id_idx";
        string rangeStart = $"{scanBucket}/";
        string rangeEnd = $"{scanBucket}/\xff\xff\xff\xff";

        for (int i = 0; i < 50; i++)
        {
            string suffix = i.ToString("D4");
            btree.Insert($"{scanBucket}/{suffix}", "m" + i);
            btree.Insert($"{table}:i:~pk/{suffix}", "u" + i);
            btree.Insert($"{table}:r/{suffix}", "r" + i);
        }

        int matches = btree
            .GetByRange(rangeStart, true, rangeEnd, true, 200)
            .Count(kv => kv.Key.StartsWith($"{scanBucket}/", StringComparison.Ordinal));

        Assert.Equal(50, matches);
    }
}