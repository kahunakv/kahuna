
using Kahuna.Utils;

namespace Kahuna.Tests.Server;

public class TestBTree
{
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
}