
using System.Diagnostics.CodeAnalysis;

namespace Kahuna.Utils;

/// <summary>
/// Represents a generic B-Tree data structure for storing key-value pairs.
/// </summary>
/// <typeparam name="TKey">The type of the key. Must implement IComparable interface.</typeparam>
/// <typeparam name="TValue">The type of the value.</typeparam>
public sealed class BTree<TKey, TValue> where TKey : IComparable<TKey>
{
    private readonly int _order;

    private Node<TKey, TValue>? Root { get; set; }
    
    public int Count { get; internal set; }

    /// <summary>
    /// Represents a self-balancing B-Tree structure for storing key-value pairs, where keys are always sorted in ascending order.
    /// Provides efficient insertion, deletion, and search operations.
    /// </summary>
    /// <typeparam name="TKey">The type of the keys, which must implement IComparable to ensure proper ordering.</typeparam>
    /// <typeparam name="TValue">The type of the values associated with the keys.</typeparam>
    public BTree(int order)
    {
        if (order < 3)
            throw new ArgumentException("Order must be at least 3", nameof(order));
        
        _order = order;
        Root = new(_order, true);
        Count = 0;
    }

    /// <summary>
    /// Determines whether the B-Tree contains a specified key.
    /// </summary>
    /// <param name="key">The key to locate in the B-Tree.</param>
    /// <returns>
    /// True if the key exists in the B-Tree; otherwise, false.
    /// </returns>
    public bool ContainsKey(TKey key)
    {
        Node<TKey, TValue>? node = Root;

        while (node is not null)
        {
            int idx = node.FindIndex(key);
            
            if (node.IsLeaf)
                return idx < node.KeyCount && node.Keys[idx].CompareTo(key) == 0;
            
            int childIdx = node.FindChildIndex(key);
            node = node.Children[childIdx];
        }
        
        return false;
    }

    /// <summary>
    /// Attempts to retrieve the value associated with the specified key in the B-Tree.
    /// </summary>
    /// <param name="key">The key whose associated value is to be retrieved.</param>
    /// <param name="value">When this method returns, contains the value associated with the specified key, if the key is found; otherwise, contains the default value for the type of the value parameter. This parameter is passed uninitialized.</param>
    /// <returns>True if the key exists in the B-Tree and its value is retrieved successfully; otherwise, false.</returns>
    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        value = Get(key);
        return value is not null;
    }

    /// <summary>
    /// Retrieves the value associated with the specified key from the B-Tree.
    /// If the key does not exist in the tree, the default value for the type of the value is returned.
    /// </summary>
    /// <param name="key">The key to locate in the B-Tree.</param>
    /// <returns>The value associated with the specified key, or the default value for the type of the value if the key is not found.</returns>
    public TValue? Get(TKey key)
    {
        Node<TKey, TValue>? node = Root;

        while (node is not null)
        {
            // binary‐search the sorted key array
            int idx = Array.BinarySearch(node.Keys, 0, node.KeyCount, key);

            if (node.IsLeaf)
            {
                if (idx >= 0)
                    return node.Values[idx]!;           // found in leaf
                break;                                  // not in tree
            }

            // if found in interior, go right of that key; otherwise ~idx is the child to descend
            int childIdx = idx >= 0 ? idx + 1 : ~idx;
            node = node.Children[childIdx];
        }

        return default;
    }

    /// <summary>
    /// Inserts a key-value pair into the B-Tree. If the key already exists, its value is updated.
    /// </summary>
    /// <param name="key">The key to be inserted or updated. Must implement IComparable for proper ordering.</param>
    /// <param name="value">The value associated with the key.</param>
    public void Insert(TKey key, TValue value)
    {
        Root ??= new(_order, true);

        (Node<TKey, TValue>? sibling, TKey promote) = Root.InsertRec(key, value, this);
        if (sibling is not null)
        {
            Node<TKey, TValue> oldRoot = Root;
            Node<TKey, TValue> newRoot = new(_order, false)
            {
                Keys = {
                    [0] = promote
                },
                KeyCount = 1,
                Children =
                {
                    [0] = oldRoot,
                    [1] = sibling
                }
            };
            Root = newRoot;
        }
    }

    /// <summary>
    /// Removes the specified key from the B-Tree. If the key exists, it is deleted, and the structure is updated accordingly.
    /// </summary>
    /// <param name="key">The key to be removed from the B-Tree.</param>
    /// <returns>True if the key was successfully removed; otherwise, false if the key was not found.</returns>
    public bool Remove(TKey key)
    {
        if (Root is null)
            return false;

        bool removed = Root.RemoveRec(key, this);
        if (removed)
            Count--;

        // if root is empty and has children, collapse
        if (Root.KeyCount == 0 && Root.IsLeaf == false)
            Root = Root.Children[0];

        return removed;
    }

    /// <summary>
    /// Retrieves all key-value pairs contained within the B-Tree in ascending key order,
    /// starting from the smallest key and traversing through the tree using linked nodes.
    /// </summary>
    /// <returns>An enumerable collection of key-value pairs stored in the B-Tree.</returns>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetItems()
    {
        Node<TKey, TValue>? temp = Root;
        if (temp is null)
            yield break;

        Node<TKey, TValue> node = temp;
        while (!node.IsLeaf)
            node = node.Children[0];

        Node<TKey, TValue>? cursor = node;
        
        while (cursor is not null)
        {
            for (int i = 0; i < cursor.KeyCount; i++)
                yield return new(cursor.Keys[i], cursor.Values[i]!);
            
            cursor = cursor.Next;
        }
    }

    /// <summary>
    /// Retrieves all key-value pairs in the B-Tree within a specified range of keys,
    /// inclusive of the minimum and maximum keys.
    /// </summary>
    /// <param name="minKey">The minimum key of the range. All returned keys will be greater than or equal to this key.</param>
    /// <param name="maxKey">The maximum key of the range. All returned keys will be less than or equal to this key.</param>
    /// <returns>A collection of key-value pairs that fall within the specified key range, ordered by ascending key.</returns>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetItems(TKey minKey, TKey maxKey)
    {
        Node<TKey, TValue>? temp = Root;
        if (temp is null)
            yield break;

        // 1) Descend to the leaf that should contain minKey
        Node<TKey, TValue> node = temp;
        
        while (!node.IsLeaf)
            node = node.Children[node.FindChildIndex(minKey)];

        // 2) Walk forward through the leaf chain
        Node<TKey, TValue>? cursor = node;
        
        while (cursor is not null)
        {
            for (int i = 0; i < cursor.KeyCount; i++)
            {
                TKey key = cursor.Keys[i];
                if (key.CompareTo(minKey) < 0)
                    continue;
                
                if (key.CompareTo(maxKey) > 0)
                    yield break;
                
                yield return new(key, cursor.Values[i]!);
            }
            cursor = cursor.Next;
        }
    }

    /// <summary>
    /// Retrieves all key-value pairs from the B-Tree where the keys start with the specified prefix.
    /// This method is only supported for BTree instances where TKey is of type string.
    /// </summary>
    /// <param name="prefix">The prefix to match against the keys in the B-Tree. Only keys that begin with this prefix will be included in the results.</param>
    /// <returns>A collection of key-value pairs where the keys start with the specified prefix, retrieved in lexicographical order.</returns>
    /// <exception cref="InvalidOperationException">Thrown when TKey is not of type string, as the operation is only valid for string-type keys.</exception>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetByBucket(string prefix)
    {
        // only valid when TKey is string
        if (typeof(TKey) != typeof(string))
            throw new InvalidOperationException("GetByBucket only supported for BTree<string, TValue>");

        Node<TKey, TValue>? temp = Root;
        if (temp is null)
            yield break;

        // 1) Descend to the leaf that would contain `prefix`
        Node<TKey, TValue> node = temp;
        
        while (!node.IsLeaf)
            node = node.Children[node.FindChildIndex((TKey)(object)prefix)]!;

        // 2) Walk forward through the linked leaves
        Node<TKey, TValue>? cursor = node;
        
        while (cursor is not null)
        {
            for (int i = 0; i < cursor.KeyCount; i++)
            {
                // safe cast because we checked TKey==string
                string key = (string)(object)cursor.Keys[i];
                
                if (key.StartsWith(prefix))
                    yield return new((TKey)(object)key, cursor.Values[i]!);
                else if (string.Compare(key, prefix, StringComparison.Ordinal) > 0)
                    // once we pass the prefix range, we can stop
                    yield break;
            }
            
            cursor = cursor.Next;
        }
    }
}

/// <summary>
/// Represents a node within a B-Tree data structure. A node contains keys, values, and pointers to child nodes, enabling efficient organization and retrieval of data.
/// </summary>
/// <typeparam name="TKey">The type of the keys stored in the node, which must implement the IComparable interface for comparison.</typeparam>
/// <typeparam name="TValue">The type of the values stored in the node.</typeparam>
public class Node<TKey, TValue> where TKey : IComparable<TKey>
{
    internal readonly TKey[] Keys;

    internal readonly TValue?[] Values;
    
    internal readonly Node<TKey, TValue>[] Children;
    
    internal int KeyCount;
    
    internal readonly bool IsLeaf;
    
    internal Node<TKey, TValue>? Next;

    private readonly int _order;

    public Node(int order, bool isLeaf)
    {
        _order = order;
        Keys = new TKey[order];
        Values = isLeaf ? new TValue?[order] : [];
        Children = new Node<TKey, TValue>[order + 1];
        IsLeaf = isLeaf;
        KeyCount = 0;
        Next = null;
    }

    // returns (newSibling, keyToPromote) if split happened
    internal (Node<TKey, TValue>?, TKey) InsertRec(TKey key, TValue value, BTree<TKey, TValue> tree)
    {
        if (IsLeaf)
        {
            int idx = FindIndex(key);
            if (idx < KeyCount && Keys[idx].CompareTo(key) == 0)
                throw new ArgumentException("Duplicate key", nameof(key));

            for (int i = KeyCount; i > idx; i--)
            {
                Keys[i] = Keys[i - 1];
                Values[i] = Values[i - 1];
            }
            
            Keys[idx] = key;
            Values[idx] = value;
            KeyCount++;
            tree.Count++;

            if (KeyCount >= _order)
                return SplitLeaf();

            return (null!, default!);
        }
        
        // interior
        int childIdx = FindChildIndex(key);
        
        (Node<TKey, TValue>? childSib, TKey childPromote) = Children[childIdx].InsertRec(key, value, tree);
        
        if (childSib is not null)
        {
            for (int i = KeyCount; i > childIdx; i--)
            {
                Keys[i] = Keys[i - 1];
                Children[i + 1] = Children[i];
            }
            
            Keys[childIdx] = childPromote;
            Children[childIdx + 1] = childSib;
            KeyCount++;

            if (KeyCount >= _order)
                return SplitInternal();
        }
        
        return (null!, default!);
    }

    private (Node<TKey, TValue> sibling, TKey promoteKey) SplitLeaf()
    {
        int mid = KeyCount / 2;
        Node<TKey, TValue> sibling = new(_order, true);
        
        int j = 0;
        for (int i = mid; i < KeyCount; i++, j++)
        {
            sibling.Keys[j] = Keys[i];
            sibling.Values[j] = Values[i];
        }
        
        sibling.KeyCount = KeyCount - mid;
        KeyCount = mid;
        sibling.Next = Next;
        Next = sibling;
        
        return (sibling, sibling.Keys[0]);
    }

    private (Node<TKey, TValue> sibling, TKey promoteKey) SplitInternal()
    {
        int mid = KeyCount / 2;
        Node<TKey, TValue> sibling = new(_order, false);
        
        int j = 0;
        
        for (int i = mid + 1; i < KeyCount; i++, j++)
            sibling.Keys[j] = Keys[i];
        
        for (int i = mid + 1, k = 0; i <= KeyCount; i++, k++)
            sibling.Children[k] = Children[i];
        
        sibling.KeyCount = KeyCount - mid - 1;
        TKey promoteKey = Keys[mid];
        KeyCount = mid;
        
        return (sibling, promoteKey);
    }

    internal int FindIndex(TKey key)
    {
        int i = 0;
        
        while (i < KeyCount && Keys[i].CompareTo(key) < 0)
            i++;
        
        return i;
    }

    internal int FindChildIndex(TKey key)
    {
        int i = 0;
        
        while (i < KeyCount && Keys[i].CompareTo(key) <= 0)
            i++;
        
        return i;
    }
    
    internal bool RemoveRec(TKey key, BTree<TKey, TValue> tree)
    {
        if (IsLeaf)
        {
            // try delete in this leaf
            int idx = FindIndex(key);
            if (idx >= KeyCount || Keys[idx].CompareTo(key) != 0)
                return false;

            // shift left
            for (int i = idx; i < KeyCount - 1; i++)
            {
                Keys[i] = Keys[i + 1];
                Values[i] = Values[i + 1];
            }

            KeyCount--;
            return true;
        }

        // internal node: recurse to child
        int childIdx = FindChildIndex(key);
        bool removed = Children[childIdx].RemoveRec(key, tree);
        if (removed == false)
            return false;

        // if child underflows, rebalance
        int minKeys = (_order - 1) / 2;
        Node<TKey, TValue> child = Children[childIdx];
        if (child.KeyCount < minKeys)
            Rebalance(childIdx);

        return true;
    }

    private void Rebalance(int idxChild)
    {
        //Node<TKey, TValue> child = Children[idxChild]!;
        Node<TKey, TValue>? left  = idxChild > 0           ? Children[idxChild - 1] : null;
        Node<TKey, TValue>? right = idxChild < KeyCount    ? Children[idxChild + 1] : null;
        int minKeys = (_order - 1) / 2;

        if (left is not null && left.KeyCount > minKeys)
            BorrowFromLeft(idxChild);
        else if (right is not null && right.KeyCount > minKeys)
            BorrowFromRight(idxChild);
        else if (left is not null)
            MergeWithLeft(idxChild);
        else if (right is not null)
            MergeWithRight(idxChild);
    }

    private void BorrowFromLeft(int idxChild)
    {
        Node<TKey, TValue> child = Children[idxChild];
        Node<TKey, TValue> left  = Children[idxChild - 1];

        if (child.IsLeaf)
        {
            // pull last of left into front of child
            for (int i = child.KeyCount; i > 0; i--)
            {
                child.Keys[i]   = child.Keys[i - 1];
                child.Values[i] = child.Values[i - 1];
            }
            child.Keys[0]   = left.Keys[left.KeyCount - 1];
            child.Values[0] = left.Values[left.KeyCount - 1];
            child.KeyCount++;
            left.KeyCount--;
            // update separator
            Keys[idxChild - 1] = child.Keys[0];
        }
        else
        {
            // pull separator down, last child key up
            for (int i = child.KeyCount; i > 0; i--)
            {
                child.Keys[i]      = child.Keys[i - 1];
                child.Children[i + 1] = child.Children[i];
            }
            child.Children[1] = child.Children[0];
            child.Keys[0]     = Keys[idxChild - 1];
            child.Children[0] = left.Children[left.KeyCount];
            Keys[idxChild - 1] = left.Keys[left.KeyCount - 1];
            child.KeyCount++;
            left.KeyCount--;
        }
    }

    private void BorrowFromRight(int idxChild)
    {
        Node<TKey, TValue> child = Children[idxChild];
        Node<TKey, TValue> right = Children[idxChild + 1];

        if (child.IsLeaf)
        {
            // pull first of right into end of child
            child.Keys[child.KeyCount]   = right.Keys[0];
            child.Values[child.KeyCount] = right.Values[0];
            child.KeyCount++;

            for (int i = 0; i < right.KeyCount - 1; i++)
            {
                right.Keys[i]   = right.Keys[i + 1];
                right.Values[i] = right.Values[i + 1];
            }
            right.KeyCount--;
            // update separator
            Keys[idxChild] = right.Keys[0];
        }
        else
        {
            // pull separator down, first right child up
            child.Keys[child.KeyCount]        = Keys[idxChild];
            child.Children[child.KeyCount + 1] = right.Children[0];
            child.KeyCount++;
            Keys[idxChild] = right.Keys[0];

            for (int i = 0; i < right.KeyCount - 1; i++)
                right.Keys[i] = right.Keys[i + 1];
            
            for (int i = 0; i < right.KeyCount; i++)
                right.Children[i] = right.Children[i + 1];
            
            right.KeyCount--;
        }
    }

    private void MergeWithLeft(int idxChild)
    {
        Node<TKey, TValue> child = Children[idxChild];
        Node<TKey, TValue> left  = Children[idxChild - 1];

        if (child.IsLeaf)
        {
            // append child keys/values to left leaf
            for (int i = 0; i < child.KeyCount; i++)
            {
                left.Keys[left.KeyCount + i]   = child.Keys[i];
                left.Values[left.KeyCount + i] = child.Values[i];
            }
            left.KeyCount += child.KeyCount;
            left.Next      = child.Next;
        }
        else
        {
            // bring down separator, then child keys/children
            left.Keys[left.KeyCount] = Keys[idxChild - 1];
            left.KeyCount++;
            for (int i = 0; i < child.KeyCount; i++)
            {
                left.Keys[left.KeyCount + i]      = child.Keys[i];
                left.Children[left.KeyCount + i]  = child.Children[i];
            }
            left.Children[left.KeyCount + child.KeyCount] = child.Children[child.KeyCount];
            left.KeyCount += child.KeyCount;
        }

        // remove slot from parent
        for (int i = idxChild - 1; i < KeyCount - 1; i++)
        {
            Keys[i]      = Keys[i + 1];
            Children[i+1] = Children[i+2];
        }
        
        KeyCount--;
    }

    private void MergeWithRight(int idxChild)
    {
        Node<TKey, TValue> child = Children[idxChild];
        Node<TKey, TValue> right = Children[idxChild + 1];

        if (child.IsLeaf)
        {
            // append right into child
            for (int i = 0; i < right.KeyCount; i++)
            {
                child.Keys[child.KeyCount + i]   = right.Keys[i];
                child.Values[child.KeyCount + i] = right.Values[i];
            }
            child.KeyCount += right.KeyCount;
            child.Next      = right.Next;
        }
        else
        {
            // bring down separator, then right keys/children
            child.Keys[child.KeyCount] = Keys[idxChild];
            child.KeyCount++;
            for (int i = 0; i < right.KeyCount; i++)
            {
                child.Keys[child.KeyCount + i]      = right.Keys[i];
                child.Children[child.KeyCount + i]  = right.Children[i];
            }
            child.Children[child.KeyCount + right.KeyCount] = right.Children[right.KeyCount];
            child.KeyCount += right.KeyCount;
        }

        // remove slot from parent
        for (int i = idxChild; i < KeyCount - 1; i++)
        {
            Keys[i]      = Keys[i + 1];
            Children[i+1] = Children[i+2];
        }
        
        KeyCount--;
    }
}
