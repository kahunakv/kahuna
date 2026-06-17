
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
    private readonly IComparer<TKey> _comparer;

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
        // Use ordinal comparison for string keys so ordering is byte-exact and culture-invariant.
        _comparer = typeof(TKey) == typeof(string)
            ? (IComparer<TKey>)(object)StringComparer.Ordinal
            : Comparer<TKey>.Default;
        Root = new(_order, true, _comparer);
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
            // One binary search per node: keys are unique, so a non-negative result is an exact hit.
            int idx = Array.BinarySearch(node.Keys, 0, node.KeyCount, key, _comparer);

            if (node.IsLeaf)
                return idx >= 0;

            // Found-in-interior ⇒ descend right of the key; otherwise ~idx is the child to descend.
            node = node.Children[idx >= 0 ? idx + 1 : ~idx];
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
        Node<TKey, TValue>? node = Root;

        while (node is not null)
        {
            int idx = Array.BinarySearch(node.Keys, 0, node.KeyCount, key, _comparer);

            if (node.IsLeaf)
            {
                if (idx >= 0)
                {
                    // Report presence via the return value, not a null-check on the payload: a key
                    // legitimately mapped to a null/default value is still present in the tree.
                    value = node.Values[idx]!;
                    return true;
                }
                break;
            }

            node = node.Children[idx >= 0 ? idx + 1 : ~idx];
        }

        value = default;
        return false;
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
            // binary‐search the sorted key array using the tree's comparer
            int idx = Array.BinarySearch(node.Keys, 0, node.KeyCount, key, _comparer);

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
        Root ??= new(_order, true, _comparer);

        (Node<TKey, TValue>? sibling, TKey promote) = Root.InsertRec(key, value, this);
        if (sibling is not null)
        {
            Node<TKey, TValue> oldRoot = Root;
            Node<TKey, TValue> newRoot = new(_order, false, _comparer)
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
                if (_comparer.Compare(key, minKey) < 0)
                    continue;

                if (_comparer.Compare(key, maxKey) > 0)
                    yield break;

                yield return new(key, cursor.Values[i]!);
            }
            cursor = cursor.Next;
        }
    }

    /// <summary>
    /// Retrieves key-value pairs from the B-Tree within an ordinal string range, with inclusive/exclusive
    /// bounds and a maximum item count. Only supported for BTree instances where TKey is of type string.
    /// </summary>
    /// <param name="start">The lower bound key.</param>
    /// <param name="startInclusive">Whether the lower bound is inclusive.</param>
    /// <param name="end">The upper bound key, or null to scan to the end of the tree.</param>
    /// <param name="endInclusive">Whether the upper bound is inclusive.</param>
    /// <param name="limit">Maximum number of items to return.</param>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetByRange(
        string start, bool startInclusive,
        string? end, bool endInclusive,
        int limit)
    {
        if (typeof(TKey) != typeof(string))
            throw new InvalidOperationException("GetByRange only supported for BTree<string, TValue>");

        Node<TKey, TValue>? temp = Root;
        if (temp is null || limit <= 0)
            yield break;

        Node<TKey, TValue> node = temp;
        while (!node.IsLeaf)
            node = node.Children[node.FindChildIndex((TKey)(object)start)]!;

        int yielded = 0;
        Node<TKey, TValue>? cursor = node;

        while (cursor is not null && yielded < limit)
        {
            for (int i = 0; i < cursor.KeyCount && yielded < limit; i++)
            {
                string key = (string)(object)cursor.Keys[i];

                int cmpStart = string.CompareOrdinal(key, start);
                if (cmpStart < 0 || (!startInclusive && cmpStart == 0))
                    continue;

                if (end is not null)
                {
                    int cmpEnd = string.CompareOrdinal(key, end);
                    if (cmpEnd > 0 || (!endInclusive && cmpEnd == 0))
                        yield break;
                }

                yield return new((TKey)(object)key, cursor.Values[i]!);
                yielded++;
            }

            cursor = cursor.Next;
        }
    }

    /// <summary>
    /// Retrieves all key-value pairs from the B-Tree where the keys start with the specified prefix.
    /// This method is only supported for BTree instances where TKey is of type string.
    /// </summary>
    /// <param name="prefix">The prefix to match against the keys in the B-Tree. Only keys that begin with this prefix will be included in the results.</param>
    /// <returns>A collection of key-value pairs where the keys start with the specified prefix, retrieved in ordinal order.</returns>
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

                if (key.StartsWith(prefix, StringComparison.Ordinal))
                    yield return new((TKey)(object)key, cursor.Values[i]!);
                else if (string.CompareOrdinal(key, prefix) > 0)
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
    private readonly IComparer<TKey> _comparer;

    public Node(int order, bool isLeaf, IComparer<TKey> comparer)
    {
        _order = order;
        _comparer = comparer;
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
            if (idx < KeyCount && _comparer.Compare(Keys[idx], key) == 0)
            {
                // Key already present: update the value in place (no structural change, no count bump).
                Values[idx] = value;
                return (null!, default!);
            }

            // Open a gap at idx by sliding the tail one slot right (Array.Copy handles the overlap).
            Array.Copy(Keys, idx, Keys, idx + 1, KeyCount - idx);
            Array.Copy(Values, idx, Values, idx + 1, KeyCount - idx);

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
            Array.Copy(Keys, childIdx, Keys, childIdx + 1, KeyCount - childIdx);
            Array.Copy(Children, childIdx + 1, Children, childIdx + 2, KeyCount - childIdx);

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
        Node<TKey, TValue> sibling = new(_order, true, _comparer);

        int count = KeyCount - mid;
        Array.Copy(Keys, mid, sibling.Keys, 0, count);
        Array.Copy(Values, mid, sibling.Values, 0, count);

        sibling.KeyCount = count;
        KeyCount = mid;
        sibling.Next = Next;
        Next = sibling;

        return (sibling, sibling.Keys[0]);
    }

    private (Node<TKey, TValue> sibling, TKey promoteKey) SplitInternal()
    {
        int mid = KeyCount / 2;
        Node<TKey, TValue> sibling = new(_order, false, _comparer);

        int count = KeyCount - mid - 1;
        Array.Copy(Keys, mid + 1, sibling.Keys, 0, count);
        Array.Copy(Children, mid + 1, sibling.Children, 0, count + 1);

        sibling.KeyCount = count;
        TKey promoteKey = Keys[mid];
        KeyCount = mid;

        return (sibling, promoteKey);
    }

    // Lower bound: first index whose key is >= the search key (the insertion point). Keys are unique,
    // so Array.BinarySearch returns either the exact hit or the bitwise-complement of that bound.
    internal int FindIndex(TKey key)
    {
        int idx = Array.BinarySearch(Keys, 0, KeyCount, key, _comparer);
        return idx >= 0 ? idx : ~idx;
    }

    // Upper bound: first index whose key is > the search key (the child to descend into). On an exact
    // hit we step one past it, matching the original "<= 0 ⇒ keep scanning" linear semantics.
    internal int FindChildIndex(TKey key)
    {
        int idx = Array.BinarySearch(Keys, 0, KeyCount, key, _comparer);
        return idx >= 0 ? idx + 1 : ~idx;
    }

    internal bool RemoveRec(TKey key, BTree<TKey, TValue> tree)
    {
        if (IsLeaf)
        {
            // try delete in this leaf
            int idx = FindIndex(key);
            if (idx >= KeyCount || _comparer.Compare(Keys[idx], key) != 0)
                return false;

            // shift left to close the gap
            Array.Copy(Keys, idx + 1, Keys, idx, KeyCount - idx - 1);
            Array.Copy(Values, idx + 1, Values, idx, KeyCount - idx - 1);

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
            Array.Copy(child.Keys, 0, child.Keys, 1, child.KeyCount);
            Array.Copy(child.Values, 0, child.Values, 1, child.KeyCount);
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
            Array.Copy(child.Keys, 0, child.Keys, 1, child.KeyCount);
            Array.Copy(child.Children, 0, child.Children, 1, child.KeyCount + 1);
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

            Array.Copy(right.Keys, 1, right.Keys, 0, right.KeyCount - 1);
            Array.Copy(right.Values, 1, right.Values, 0, right.KeyCount - 1);
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

            Array.Copy(right.Keys, 1, right.Keys, 0, right.KeyCount - 1);
            Array.Copy(right.Children, 1, right.Children, 0, right.KeyCount);

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
            Array.Copy(child.Keys, 0, left.Keys, left.KeyCount, child.KeyCount);
            Array.Copy(child.Values, 0, left.Values, left.KeyCount, child.KeyCount);
            left.KeyCount += child.KeyCount;
            left.Next      = child.Next;
        }
        else
        {
            // bring down separator, then child keys/children
            left.Keys[left.KeyCount] = Keys[idxChild - 1];
            left.KeyCount++;
            Array.Copy(child.Keys, 0, left.Keys, left.KeyCount, child.KeyCount);
            Array.Copy(child.Children, 0, left.Children, left.KeyCount, child.KeyCount + 1);
            left.KeyCount += child.KeyCount;
        }

        // remove slot from parent (drop separator Keys[idxChild-1] and Children[idxChild])
        Array.Copy(Keys, idxChild, Keys, idxChild - 1, KeyCount - idxChild);
        Array.Copy(Children, idxChild + 1, Children, idxChild, KeyCount - idxChild);

        KeyCount--;
    }

    private void MergeWithRight(int idxChild)
    {
        Node<TKey, TValue> child = Children[idxChild];
        Node<TKey, TValue> right = Children[idxChild + 1];

        if (child.IsLeaf)
        {
            // append right into child
            Array.Copy(right.Keys, 0, child.Keys, child.KeyCount, right.KeyCount);
            Array.Copy(right.Values, 0, child.Values, child.KeyCount, right.KeyCount);
            child.KeyCount += right.KeyCount;
            child.Next      = right.Next;
        }
        else
        {
            // bring down separator, then right keys/children
            child.Keys[child.KeyCount] = Keys[idxChild];
            child.KeyCount++;
            Array.Copy(right.Keys, 0, child.Keys, child.KeyCount, right.KeyCount);
            Array.Copy(right.Children, 0, child.Children, child.KeyCount, right.KeyCount + 1);
            child.KeyCount += right.KeyCount;
        }

        // remove slot from parent (drop separator Keys[idxChild] and Children[idxChild+1])
        Array.Copy(Keys, idxChild + 1, Keys, idxChild, KeyCount - idxChild - 1);
        Array.Copy(Children, idxChild + 2, Children, idxChild + 1, KeyCount - idxChild - 1);

        KeyCount--;
    }
}
