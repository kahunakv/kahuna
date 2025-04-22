using System.Diagnostics.CodeAnalysis;

namespace Kahuna.Utils;

/// <summary>
/// Represents a node in the B-Tree.
/// </summary>
public sealed class Node<TKey, TValue>
{
    // The maximum number of keys a node can hold
    private readonly int _maxKeys;
    
    // Current number of keys in this node
    public int KeyCount { get; set; }
    
    // Array of keys
    public KeyValuePair<TKey, TValue>[] Keys { get; }
    
    // Array of child nodes
    public Node<TKey, TValue>[] Children { get; }
    
    // Whether this node is a leaf
    public bool IsLeaf { get; set; }

    /// <summary>
    /// Initializes a new instance of the B-Tree node.
    /// </summary>
    /// <param name="minDegree">The minimum degree of the B-Tree.</param>
    /// <param name="isLeaf">Whether this node is a leaf.</param>
    public Node(int minDegree, bool isLeaf)
    {
        _maxKeys = 2 * minDegree - 1;
        KeyCount = 0;
        Keys = new KeyValuePair<TKey, TValue>[_maxKeys];
        Children = new Node<TKey, TValue>[_maxKeys + 1];
        IsLeaf = isLeaf;
    }

    /// <summary>
    /// Inserts a key-value pair into this node, assuming the node is not full.
    /// </summary>
    /// <param name="index">The index at which to insert.</param>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    public void InsertNonFull(int index, TKey key, TValue value)
    {
        // Move all keys after the insertion point to the right
        for (int i = KeyCount - 1; i >= index; i--)
            Keys[i + 1] = Keys[i];

        // Insert the new key
        Keys[index] = new(key, value);
        KeyCount++;
    }

    /// <summary>
    /// Splits a child node of this node.
    /// </summary>
    /// <param name="childIndex">The index of the child to split.</param>
    public void SplitChild(int childIndex)
    {
        Node<TKey, TValue> child = Children[childIndex];
        Node<TKey, TValue> newChild = new(_maxKeys / 2 + 1, child.IsLeaf);
        
        // Copy the upper half of child's keys to the new child
        for (int i = 0; i < _maxKeys / 2; i++)
            newChild.Keys[i] = child.Keys[i + _maxKeys / 2 + 1];
        
        newChild.KeyCount = _maxKeys / 2;

        // If child is not a leaf, also move the appropriate children
        if (!child.IsLeaf)
        {
            for (int i = 0; i <= _maxKeys / 2; i++)
                newChild.Children[i] = child.Children[i + _maxKeys / 2 + 1];
        }

        // Adjust the key count of the child
        child.KeyCount = _maxKeys / 2;

        // Move the child pointers in this node to make room for the new child
        for (int i = KeyCount; i > childIndex; i--)
            Children[i + 1] = Children[i];
        
        Children[childIndex + 1] = newChild;

        // Move the keys in this node to make room for the promoted key
        for (int i = KeyCount - 1; i >= childIndex; i--)
            Keys[i + 1] = Keys[i];

        // Promote the middle key from the child
        Keys[childIndex] = child.Keys[_maxKeys / 2];
        KeyCount++;
    }

    /// <summary>
    /// Removes a key at the specified index from this node.
    /// </summary>
    /// <param name="index">The index of the key to remove.</param>
    public void RemoveKey(int index)
    {
        // Shift all keys after the index left by one position
        for (int i = index + 1; i < KeyCount; i++)
            Keys[i - 1] = Keys[i];
        
        KeyCount--;
    }

    /// <summary>
    /// Merges the child at childIndex with the child at childIndex+1.
    /// </summary>
    /// <param name="childIndex">The index of the first child to merge.</param>
    public void MergeChildren(int childIndex)
    {
        Node<TKey, TValue> child1 = Children[childIndex];
        Node<TKey, TValue> child2 = Children[childIndex + 1];

        // Move the separating key from this node to child1
        child1.Keys[child1.KeyCount] = Keys[childIndex];
        child1.KeyCount++;

        // Copy all keys from child2 to child1
        for (int i = 0; i < child2.KeyCount; i++)
        {
            child1.Keys[child1.KeyCount + i] = child2.Keys[i];
        }

        // If not leaves, also copy the children
        if (!child1.IsLeaf)
        {
            for (int i = 0; i <= child2.KeyCount; i++) 
                child1.Children[child1.KeyCount + i] = child2.Children[i];
        }

        // Update the key count of child1
        child1.KeyCount += child2.KeyCount;

        // Remove the separating key from this node
        RemoveKey(childIndex);

        // Move all child pointers after childIndex+1 to the left
        for (int i = childIndex + 1; i < KeyCount + 1; i++)
            Children[i] = Children[i + 1];
    }
}

/// <summary>
/// A B-Tree implementation in C#.
/// </summary>
/// <typeparam name="TKey">The type of keys stored in the B-Tree.</typeparam>
/// <typeparam name="TValue">The type of values associated with keys.</typeparam>
public sealed class BTree<TKey, TValue> where TKey : IComparable<TKey>
{
    private readonly int _minDegree;
    
    private Node<TKey, TValue> _root;
    
    private int _count;

    /// <summary>
    /// Initializes a new instance of the B-Tree with a specified minimum degree.
    /// The minimum degree determines the minimum and maximum number of keys per node.
    /// </summary>
    /// <param name="minDegree">The minimum degree of the B-Tree. Must be at least 2.</param>
    public BTree(int minDegree)
    {
        if (minDegree < 2)
            throw new ArgumentException("Minimum degree of a B-Tree must be at least 2.", nameof(minDegree));

        _minDegree = minDegree;
        _root = new(_minDegree, true);
        _count = 0;
    }

    /// <summary>
    /// Gets the number of key-value pairs in the B-Tree.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Searches for a key in the B-Tree.
    /// </summary>
    /// <param name="key">The key to search for.</param>
    /// <param name="value">When this method returns, contains the value associated with the specified key, 
    /// if the key is found; otherwise, the default value for the type of the value parameter.</param>
    /// <returns>true if the B-Tree contains an element with the specified key; otherwise, false.</returns>
    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        if (Search(_root, key, out TValue? tempValue))
        {
            value = tempValue!;
            return true;
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Recursively searches for a key in a node and its subtree.
    /// </summary>
    /// <param name="node">The node to search in.</param>
    /// <param name="key">The key to search for.</param>
    /// <param name="value">When this method returns, contains the value associated with the specified key, 
    /// if the key is found; otherwise, the default value for the type of the value parameter.</param>
    /// <returns>true if the key is found; otherwise, false.</returns>
    private bool Search(Node<TKey, TValue> node, TKey key, [MaybeNullWhen(false)] out TValue? value)
    {
        // Find the first key greater than or equal to the search key
        int i = 0;
        while (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) > 0)
            i++;

        // If a matching key is found, return its value
        if (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) == 0)
        {
            value = node.Keys[i].Value;
            return true;
        }

        // If this is a leaf node and no matching key was found, the key is not in the tree
        if (node.IsLeaf)
        {
            value = default;
            return false;
        }

        // Recursively search in the appropriate child node
        return Search(node.Children[i], key, out value);
    }

    /// <summary>
    /// Inserts a key-value pair into the B-Tree.
    /// </summary>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    public void Insert(TKey key, TValue value)
    {
        // If the root is full, split it
        if (_root.KeyCount == 2 * _minDegree - 1)
        {
            Node<TKey, TValue> newRoot = new(_minDegree, false)
            {
                Children = { [0] = _root }
            };
            newRoot.SplitChild(0);
            _root = newRoot;
            
            // Continue with insertion at the new root
        }

        InsertNonFull(_root, key, value);

        _count++;
    }

    /// <summary>
    /// Recursively inserts a key-value pair into a non-full node.
    /// </summary>
    /// <param name="node">The node to insert into.</param>
    /// <param name="key">The key to insert.</param>
    /// <param name="value">The value to associate with the key.</param>
    private void InsertNonFull(Node<TKey, TValue> node, TKey key, TValue value)
    {
        // Find the position to insert the key
        int i = node.KeyCount - 1;
        while (i >= 0 && key.CompareTo(node.Keys[i].Key) < 0)
            i--;
        
        i++;

        // If the node is a leaf, insert the key here
        if (node.IsLeaf)
            node.InsertNonFull(i, key, value);
        else
        {
            // If the child node is full, split it first
            if (node.Children[i].KeyCount == 2 * _minDegree - 1)
            {
                node.SplitChild(i);
                
                // After splitting, the new key might go into the new child
                if (key.CompareTo(node.Keys[i].Key) > 0)
                    i++;
            }
            
            // Continue insertion in the appropriate child
            InsertNonFull(node.Children[i], key, value);
        }
    }

    /// <summary>
    /// Removes a key from the B-Tree.
    /// </summary>
    /// <param name="key">The key to remove.</param>
    /// <returns>true if the key was found and removed; otherwise, false.</returns>
    public bool Remove(TKey key)
    {
        bool removed = Remove(_root, key);
        
        // If the root has no keys and is not a leaf, make its first child the new root
        if (_root is { KeyCount: 0, IsLeaf: false })
            _root = _root.Children[0];
        
        if (removed)
            _count--;
        
        return removed;
    }

    /// <summary>
    /// Recursively removes a key from a node and its subtree.
    /// </summary>
    /// <param name="node">The node to remove from.</param>
    /// <param name="key">The key to remove.</param>
    /// <returns>true if the key was found and removed; otherwise, false.</returns>
    private bool Remove(Node<TKey, TValue> node, TKey key)
    {
        // Find the position of the key or the child that might contain it
        int i = 0;
        
        while (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) > 0)
            i++;

        // If the key is in this node
        if (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) == 0)
        {
            // If this is a leaf node, simply remove the key
            if (node.IsLeaf)
            {
                node.RemoveKey(i);
                return true;
            }

            // If this is not a leaf node, replace the key with its predecessor or successor
            // and recursively delete that key from the appropriate child
            
            // Case 1: The child that precedes the key has at least t keys
            if (node.Children[i].KeyCount >= _minDegree)
            {
                // Find the predecessor of the key
                KeyValuePair<TKey, TValue> predecessor = FindPredecessor(node, i);
                node.Keys[i] = predecessor;
                return Remove(node.Children[i], predecessor.Key);
            }
            
            // Case 2: The child that follows the key has at least t keys
            if (node.Children[i + 1].KeyCount >= _minDegree)
            {
                // Find the successor of the key
                KeyValuePair<TKey, TValue> successor = FindSuccessor(node, i);
                node.Keys[i] = successor;
                return Remove(node.Children[i + 1], successor.Key);
            }
            
            // Case 3: Both the preceding and following children have t-1 keys
            
            // Merge the key and the two children
            node.MergeChildren(i);
            return Remove(node.Children[i], key);
        }
        
        // If the key is not in this node but might be in a child
        
        // If this is a leaf node, the key is not in the tree
        if (node.IsLeaf)
            return false;

        // Ensure that the child has at least t keys
        EnsureChildHasMinDegreeKeys(node, i);

        // Recursively remove from the child
        return Remove(node.Children[i], key);
    }

    /// <summary>
    /// Finds the predecessor of a key in a node.
    /// </summary>
    /// <param name="node">The node containing the key.</param>
    /// <param name="keyIndex">The index of the key.</param>
    /// <returns>The predecessor key-value pair.</returns>
    private static KeyValuePair<TKey, TValue> FindPredecessor(Node<TKey, TValue> node, int keyIndex)
    {
        // Start with the child that precedes the key
        Node<TKey, TValue> current = node.Children[keyIndex];
        
        // Keep moving to the rightmost child until a leaf is reached
        while (!current.IsLeaf)
            current = current.Children[current.KeyCount];
        
        // The last key in the leaf is the predecessor
        return current.Keys[current.KeyCount - 1];
    }

    /// <summary>
    /// Finds the successor of a key in a node.
    /// </summary>
    /// <param name="node">The node containing the key.</param>
    /// <param name="keyIndex">The index of the key.</param>
    /// <returns>The successor key-value pair.</returns>
    private static KeyValuePair<TKey, TValue> FindSuccessor(Node<TKey, TValue> node, int keyIndex)
    {
        // Start with the child that follows the key
        Node<TKey, TValue> current = node.Children[keyIndex + 1];
        
        // Keep moving to the leftmost child until a leaf is reached
        while (!current.IsLeaf)
            current = current.Children[0];
        
        // The first key in the leaf is the successor
        return current.Keys[0];
    }

    /// <summary>
    /// Ensures that a child node has at least _minDegree keys.
    /// </summary>
    /// <param name="node">The parent node.</param>
    /// <param name="childIndex">The index of the child to ensure has enough keys.</param>
    private void EnsureChildHasMinDegreeKeys(Node<TKey, TValue> node, int childIndex)
    {
        Node<TKey, TValue> child = node.Children[childIndex];
        
        // If the child already has enough keys, nothing to do
        if (child.KeyCount >= _minDegree)
            return;

        // Try to borrow a key from the left sibling
        if (childIndex > 0 && node.Children[childIndex - 1].KeyCount >= _minDegree)
            BorrowFromLeftSibling(node, childIndex);
        
        // Try to borrow a key from the right sibling
        else if (childIndex < node.KeyCount && node.Children[childIndex + 1].KeyCount >= _minDegree)
            BorrowFromRightSibling(node, childIndex);
        
        // If borrowing is not possible, merge with a sibling
        else
        {
            // If child is the last child, merge with the child to its left
            if (childIndex == node.KeyCount)
                node.MergeChildren(childIndex - 1);
            // Otherwise, merge with the child to its right
            else
                node.MergeChildren(childIndex);
        }
    }

    /// <summary>
    /// Borrows a key from the left sibling of a child.
    /// </summary>
    /// <param name="node">The parent node.</param>
    /// <param name="childIndex">The index of the child.</param>
    private static void BorrowFromLeftSibling(Node<TKey, TValue> node, int childIndex)
    {
        Node<TKey, TValue> child = node.Children[childIndex];
        Node<TKey, TValue> leftSibling = node.Children[childIndex - 1];

        // Make room for the borrowed key in the child
        for (int i = child.KeyCount - 1; i >= 0; i--)
            child.Keys[i + 1] = child.Keys[i];

        // If not a leaf, also make room for the borrowed child
        if (!child.IsLeaf)
        {
            for (int i = child.KeyCount; i >= 0; i--)
                child.Children[i + 1] = child.Children[i];
        }

        // The parent's key goes down to the child
        child.Keys[0] = node.Keys[childIndex - 1];

        // If not a leaf, the rightmost child of the left sibling becomes the leftmost child of the child
        if (!child.IsLeaf)
            child.Children[0] = leftSibling.Children[leftSibling.KeyCount];

        // The rightmost key of the left sibling goes up to the parent
        node.Keys[childIndex - 1] = leftSibling.Keys[leftSibling.KeyCount - 1];

        // Update key counts
        child.KeyCount++;
        leftSibling.KeyCount--;
    }

    /// <summary>
    /// Borrows a key from the right sibling of a child.
    /// </summary>
    /// <param name="node">The parent node.</param>
    /// <param name="childIndex">The index of the child.</param>
    private static void BorrowFromRightSibling(Node<TKey, TValue> node, int childIndex)
    {
        Node<TKey, TValue> child = node.Children[childIndex];
        Node<TKey, TValue> rightSibling = node.Children[childIndex + 1];

        // The parent's key goes down to the child
        child.Keys[child.KeyCount] = node.Keys[childIndex];

        // If not a leaf, the leftmost child of the right sibling becomes the rightmost child of the child
        if (!child.IsLeaf)
            child.Children[child.KeyCount + 1] = rightSibling.Children[0];

        // The leftmost key of the right sibling goes up to the parent
        node.Keys[childIndex] = rightSibling.Keys[0];

        // Move all keys in the right sibling one position to the left
        for (int i = 1; i < rightSibling.KeyCount; i++)
            rightSibling.Keys[i - 1] = rightSibling.Keys[i];

        // If not a leaf, also move all children in the right sibling one position to the left
        if (!rightSibling.IsLeaf)
        {
            for (int i = 1; i <= rightSibling.KeyCount; i++)
                rightSibling.Children[i - 1] = rightSibling.Children[i];
        }

        // Update key counts
        child.KeyCount++;
        rightSibling.KeyCount--;
    }

    /// <summary>
    /// Checks if the B-Tree contains a key.
    /// </summary>
    /// <param name="key">The key to check for.</param>
    /// <returns>true if the B-Tree contains the key; otherwise, false.</returns>
    public bool ContainsKey(TKey key)
    {
        return TryGetValue(key, out _);
    }

    /// <summary>
    /// Clears all keys from the B-Tree.
    /// </summary>
    public void Clear()
    {
        _root = new(_minDegree, true);
        _count = 0;
    }

    /// <summary>
    /// Gets all key-value pairs in the B-Tree in ascending order.
    /// </summary>
    /// <returns>An enumerable collection of key-value pairs.</returns>
    public IEnumerable<KeyValuePair<TKey, TValue>> GetItems()
    {
        List<KeyValuePair<TKey, TValue>> items = new List<KeyValuePair<TKey, TValue>>(_count);
        CollectItems(_root, items);
        return items;
    }

    /// <summary>
    /// Recursively collects all key-value pairs from a node and its subtree.
    /// </summary>
    /// <param name="node">The node to collect from.</param>
    /// <param name="items">The collection to add the items to.</param>
    private void CollectItems(Node<TKey, TValue> node, List<KeyValuePair<TKey, TValue>> items)
    {
        if (node.IsLeaf)
        {
            // If this is a leaf, simply add all its keys
            for (int i = 0; i < node.KeyCount; i++)
                items.Add(node.Keys[i]);
        }
        else
        {
            // If this is not a leaf, traverse its children in order
            for (int i = 0; i < node.KeyCount; i++)
            {
                CollectItems(node.Children[i], items);
                items.Add(node.Keys[i]);
            }
            
            CollectItems(node.Children[node.KeyCount], items);
        }
    }

    /// <summary>
    /// Searches for all key-value pairs within a specified range of keys.
    /// </summary>
    /// <param name="startKey">The inclusive lower bound of the range.</param>
    /// <param name="endKey">The inclusive upper bound of the range.</param>
    /// <returns>An enumerable collection of key-value pairs within the specified range.</returns>
    /// <exception cref="ArgumentException">Thrown when startKey is greater than endKey.</exception>
    public IEnumerable<KeyValuePair<TKey, TValue>> RangeSearch(TKey startKey, TKey endKey)
    {
        if (startKey.CompareTo(endKey) > 0)
            throw new ArgumentException("The start key must be less than or equal to the end key.", nameof(startKey));

        List<KeyValuePair<TKey, TValue>> results = [];
        RangeSearch(_root, startKey, endKey, results);
        return results;
    }

    /// <summary>
    /// Recursively searches for all key-value pairs within a specified range of keys in a node and its subtree.
    /// </summary>
    /// <param name="node">The node to search in.</param>
    /// <param name="startKey">The inclusive lower bound of the range.</param>
    /// <param name="endKey">The inclusive upper bound of the range.</param>
    /// <param name="results">The collection to add the results to.</param>
    private void RangeSearch(Node<TKey, TValue> node, TKey startKey, TKey endKey, List<KeyValuePair<TKey, TValue>> results)
    {
        if (node == null)
            return;

        // Find the first key that is greater than or equal to startKey
        int i = 0;
        while (i < node.KeyCount && startKey.CompareTo(node.Keys[i].Key) > 0)
            i++;

        // Start collecting keys
        while (i < node.KeyCount && endKey.CompareTo(node.Keys[i].Key) >= 0)
        {
            // If not a leaf, first traverse the child before this key
            if (!node.IsLeaf)
                RangeSearch(node.Children[i], startKey, endKey, results);

            // Add the current key if it's within the range
            if (startKey.CompareTo(node.Keys[i].Key) <= 0 && endKey.CompareTo(node.Keys[i].Key) >= 0)
                results.Add(node.Keys[i]);

            i++;
        }

        // If not a leaf, traverse the last child
        if (!node.IsLeaf)
            RangeSearch(node.Children[i], startKey, endKey, results);
    }

    /// <summary>
    /// Searches for all key-value pairs less than the specified key.
    /// </summary>
    /// <param name="key">The key to compare with.</param>
    /// <returns>An enumerable collection of key-value pairs with keys less than the specified key.</returns>
    public IEnumerable<KeyValuePair<TKey, TValue>> LessThan(TKey key)
    {
        List<KeyValuePair<TKey, TValue>> results = [];
        LessThan(_root, key, results);
        return results;
    }

    /// <summary>
    /// Recursively searches for all key-value pairs less than the specified key in a node and its subtree.
    /// </summary>
    /// <param name="node">The node to search in.</param>
    /// <param name="key">The key to compare with.</param>
    /// <param name="results">The collection to add the results to.</param>
    private void LessThan(Node<TKey, TValue> node, TKey key, List<KeyValuePair<TKey, TValue>> results)
    {
        if (node == null)
            return;

        int i = 0;
        // For each key in this node
        while (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) > 0)
        {
            // If not a leaf, first traverse the child before this key
            if (!node.IsLeaf)
                LessThan(node.Children[i], key, results);

            // Add the current key
            results.Add(node.Keys[i]);
            i++;
        }

        // If not a leaf, traverse the child at position i
        if (!node.IsLeaf)
            LessThan(node.Children[i], key, results);
    }

    /// <summary>
    /// Searches for all key-value pairs greater than the specified key.
    /// </summary>
    /// <param name="key">The key to compare with.</param>
    /// <returns>An enumerable collection of key-value pairs with keys greater than the specified key.</returns>
    public IEnumerable<KeyValuePair<TKey, TValue>> GreaterThan(TKey key)
    {
        List<KeyValuePair<TKey, TValue>> results = new List<KeyValuePair<TKey, TValue>>();
        GreaterThan(_root, key, results);
        return results;
    }

    /// <summary>
    /// Recursively searches for all key-value pairs greater than the specified key in a node and its subtree.
    /// </summary>
    /// <param name="node">The node to search in.</param>
    /// <param name="key">The key to compare with.</param>
    /// <param name="results">The collection to add the results to.</param>
    private static void GreaterThan(Node<TKey, TValue> node, TKey key, List<KeyValuePair<TKey, TValue>> results)
    {
        if (node == null)
            return;

        int i = 0;
        // Find the first key that is greater than the specified key
        while (i < node.KeyCount && key.CompareTo(node.Keys[i].Key) >= 0)
        {
            // If not a leaf, first traverse the child before this key
            if (!node.IsLeaf)
                GreaterThan(node.Children[i], key, results);
            
            i++;
        }

        // Add all remaining keys
        while (i < node.KeyCount)
        {
            // If not a leaf, first traverse the child before this key
            if (!node.IsLeaf)
                GreaterThan(node.Children[i], key, results);

            // Add the current key
            results.Add(node.Keys[i]);
            i++;
        }

        // If not a leaf, traverse the last child
        if (!node.IsLeaf)
            GreaterThan(node.Children[i], key, results);
    }
}

