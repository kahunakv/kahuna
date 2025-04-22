
using Kahuna.Utils;

namespace Kahuna.Tests.Server;

public class TestBTree
{
    [Fact]
    public void TestBTreeBasic()
    {
        BTree<int, string> btree = new(2);
        
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
        
        Assert.Equal(3, btree.Count);
    }
    
    [Fact]
    public void TestBTreeBasicSplit()
    {
        BTree<int, string> btree = new(2);
        
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
        BTree<int, string> btree = new(2);
        
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
}