package storage

import (
	"bytes"
	ConcurrencyPackage "db-engine-v2/internal/concurrency"
	BufferPoolPackage "db-engine-v2/internal/storage/BufferPool"
	PagePackage "db-engine-v2/internal/storage/Page"
	"db-engine-v2/types"
	"errors"
	"log"
	"sync"
)

type BTree struct {
	RootPageID  types.PageID
	MaxChilds   uint16
	BufferPool  *BufferPoolPackage.BufferPoolManager
	LockManager *ConcurrencyPackage.LockManager
	mu          sync.RWMutex
}

func NewBTree(bufferPool *BufferPoolPackage.BufferPoolManager, lockManager *ConcurrencyPackage.LockManager, maxChilds uint16) (*BTree, error) {
	if maxChilds < 3 {
		return nil, errors.New("b+ tree should have at least 3 children")
	}
	tree := &BTree{
		MaxChilds:   maxChilds,
		BufferPool:  bufferPool,
		LockManager: lockManager,
	}

	rootPage, err := bufferPool.NewPage()
	if err != nil {
		return nil, err
	}

	rootNode := &BNode{
		isLeaf: true,
		PageID: rootPage.GetID(),
		Parent: 0,
		Keys:   make([][]byte, 0),
		Values: make([][]byte, 0),
	}

	rootPage.Data = rootNode.SerializeBNode()
	rootPage.SetDirty(true)
	tree.RootPageID = rootPage.GetID()

	bufferPool.UnPinPage(rootPage.GetID(), true)
	return tree, nil
}
func (tree *BTree) SearchValue(key []byte) ([]byte, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	rootPage, err := tree.BufferPool.ReadPage(tree.RootPageID)
	if err != nil {
		return nil, err
	}
	defer tree.BufferPool.UnPinPage(rootPage.GetID(), false)

	rootNode, err := DeSerializeBNode(rootPage.GetData())
	if err != nil {
		return nil, err
	}

	leafNode, leafPage, err := tree.findLeafNode(rootNode, key)
	if err != nil {
		return nil, err
	}
	defer tree.BufferPool.UnPinPage(leafPage.GetID(), false)
	for i, nodeKey := range leafNode.Keys {
		if bytes.Equal(nodeKey, key) {
			return leafNode.Values[i], nil
		}
	}
	return nil, errors.New("key doesn't exist")
}
func (tree *BTree) lowerBound(key []byte) (*BNode, int, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	rootPage, err := tree.BufferPool.ReadPage(tree.RootPageID)
	if err != nil {
		return nil, 0, err
	}
	defer tree.BufferPool.UnPinPage(rootPage.GetID(), false)

	rootNode, err := DeSerializeBNode(rootPage.GetData())
	if err != nil {
		return nil, 0, err
	}

	leafNode, leafPage, err := tree.findLeafNode(rootNode, key)
	if err != nil {
		return nil, 0, err
	}
	defer tree.BufferPool.UnPinPage(leafPage.GetID(), false)
	// First node key which is greater than or equal to key
	for i, nodeKey := range leafNode.Keys {
		if bytes.Compare(key, nodeKey) <= 0 {
			return leafNode, i, nil
		}
	}
	return nil, -1, nil
}

func (tree *BTree) upperBound(key []byte) (*BNode, int, error) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	rootPage, err := tree.BufferPool.ReadPage(tree.RootPageID)
	if err != nil {
		return nil, 0, err
	}
	defer tree.BufferPool.UnPinPage(rootPage.GetID(), false)

	rootNode, err := DeSerializeBNode(rootPage.GetData())
	if err != nil {
		return nil, 0, err
	}

	leafNode, leafPage, err := tree.findLeafNode(rootNode, key)
	if err != nil {
		return nil, 0, err
	}
	defer tree.BufferPool.UnPinPage(leafPage.GetID(), false)
	// First node key which is greater than to key
	for i, nodeKey := range leafNode.Keys {
		if bytes.Compare(key, nodeKey) < 0 {
			return leafNode, i, nil
		}
	}
	return leafNode, len(leafNode.Keys), nil
}

func (tree *BTree) findLeafNode(node *BNode, key []byte) (*BNode, *PagePackage.Page, error) {
	if node.isLeaf {
		leafPage, err := tree.BufferPool.ReadPage(node.PageID)
		if err != nil {
			return nil, nil, err
		}
		return node, leafPage, nil
	}
	// Find which child to go next
	// We have total of k+1 childs (slots between the current node keys)
	// The required slot is the one which has its next key > key parameter
	childIndex := 0
	for i, nodeKey := range node.Keys {
		if bytes.Compare(key, nodeKey) < 0 {
			break
		}
		childIndex = i + 1
	}
	childPage, err := tree.BufferPool.ReadPage(node.PageIDs[childIndex])
	if err != nil {
		return nil, nil, err
	}
	childNode, err := DeSerializeBNode(childPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(childPage.GetID(), false)
		return nil, nil, err
	}
	leafNode, leafPage, err := tree.findLeafNode(childNode, key)
	tree.BufferPool.UnPinPage(childPage.GetID(), false)
	return leafNode, leafPage, err
}

// Insert a key-value pair into the B+ tree
func (tree *BTree) Insert(key []byte, value []byte, TransID types.TransactionID) error {

	tree.mu.Lock()
	defer tree.mu.Unlock()

	// Acquire exclusive lock (write) on the tree
	root := tree.RootPageID
	if err := tree.LockManager.AcquireLock(TransID, types.ResourceID(root), ConcurrencyPackage.LockModeExclusive); err != nil {
		return err
	}
	defer tree.LockManager.ReleaseLock(TransID, types.ResourceID(root))
	defer tree.LockManager.ReleaseLock(TransID, types.ResourceID(tree.RootPageID))

	rootPage, err := tree.BufferPool.ReadPage(tree.RootPageID)
	if err != nil {
		return err
	}
	rootNode, err := DeSerializeBNode(rootPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(rootPage.GetID(), false)
		return err
	}

	// Pass it to a function to insert it, check if split is happened after insertion to change the root if there's a new one
	newRoot, splitOccured, err := tree.insertIntoNode(rootNode, rootPage, key, value, nil, TransID)
	if err != nil {
		tree.BufferPool.UnPinPage(rootPage.GetID(), false)
		return err
	}

	if splitOccured {
		tree.RootPageID = newRoot.PageID
	}
	tree.BufferPool.UnPinPage(rootPage.GetID(), false)

	return nil
}

// Recursively insert a key-value pair into a node
// Returns the (possibly new) root node, a flag indicating if a split occurred, and any error
func (tree *BTree) insertIntoNode(node *BNode, page *PagePackage.Page, key []byte, value []byte, newChildPageID *types.PageID, TransID types.TransactionID) (*BNode, bool, error) {
	// If we are at the leaf level, insert directly
	if node.isLeaf {
		return tree.insertIntoLeaf(node, page, key, value, TransID)
	}

	// For internal nodes, find the next child to insert into
	childIndex := 0
	for i, nodeKey := range node.Keys {
		if bytes.Compare(key, nodeKey) < 0 {
			break
		}
		childIndex = i + 1
	}

	childPage, err := tree.BufferPool.ReadPage(node.PageIDs[childIndex])
	if err != nil {
		return nil, false, err
	}

	childNode, err := DeSerializeBNode(childPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(childPage.GetID(), false)
		return nil, false, err
	}

	// Here's the issue - we're not capturing the new child page ID
	newChildNode, splitOccured, err := tree.insertIntoNode(childNode, childPage, key, value, nil, TransID)
	if err != nil {
		tree.BufferPool.UnPinPage(childPage.GetID(), false)
		return nil, false, err
	}

	// We don't care about the child for now, so we can unpin it
	// It's dirty because it might be modified during insertion
	tree.BufferPool.UnPinPage(childPage.GetID(), true)

	if !splitOccured {
		// No new root
		return node, false, nil
	}

	// If a split occurred, we get the separator key from the newChildNode
	// and insert it into this nod

	// Get the separator key and insert into this node
	separatorKey := newChildNode.Keys[0]
	return tree.insertIntoInternal(node, page, separatorKey, newChildNode.PageID, TransID)
}
func (tree *BTree) insertIntoLeaf(node *BNode, page *PagePackage.Page, key []byte, value []byte, TransID types.TransactionID) (*BNode, bool, error) {
	pos := 0
	for i, nodeKey := range node.Keys {
		if bytes.Compare(key, nodeKey) > 0 { // key > nodeKey
			pos = i + 1
		} else if bytes.Equal(key, nodeKey) {
			// Key already exists, we can just update the value

			node.Values[i] = value
			page.Data = node.SerializeBNode()
			page.SetDirty(true)
			return node, false, nil
		}
	}
	// Insert the key-value pair at the correct position
	node.Keys = append(node.Keys, nil)
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = key

	node.Values = append(node.Values, nil)
	copy(node.Values[pos+1:], node.Values[pos:])
	node.Values[pos] = value

	// Now after insertion, check if we need to split or not
	if len(node.Keys) <= int(tree.MaxChilds)-1 {
		// No need to split
		page.Data = node.SerializeBNode()
		page.SetDirty(true)
		return node, false, nil
	}
	return tree.splitLeafNode(node, page, TransID)
}
func (tree *BTree) insertIntoInternal(node *BNode, page *PagePackage.Page, key []byte, RightChildPageID types.PageID, TransID types.TransactionID) (*BNode, bool, error) {
	pos := 0
	for i, nodeKey := range node.Keys {
		if bytes.Compare(key, nodeKey) > 0 {
			pos = i + 1
		} else if bytes.Equal(key, nodeKey) {
			node.PageIDs[i+1] = RightChildPageID
			page.Data = node.SerializeBNode()
			page.SetDirty(true)
			return node, false, nil
		}
	}
	node.Keys = append(node.Keys, nil)
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = key

	// Remember, we have keys+1 childs
	node.PageIDs = append(node.PageIDs, 0)
	copy(node.PageIDs[pos+2:], node.PageIDs[pos+1:])
	node.PageIDs[pos+1] = RightChildPageID

	// Now keys[pos] points to the right child (parent), so we need to update the right child
	rightChildPage, err := tree.BufferPool.ReadPage(RightChildPageID)
	if err != nil {
		return nil, false, err
	}
	rightChildNode, err := DeSerializeBNode(rightChildPage.GetData())
	if err != nil {
		return nil, false, err
	}
	rightChildNode.Parent = node.PageID
	rightChildPage.Data = rightChildNode.SerializeBNode()
	rightChildPage.SetDirty(true)

	tree.BufferPool.UnPinPage(rightChildPage.GetID(), true)

	// we might need to split the internal node also
	if len(node.Keys) <= int(tree.MaxChilds)-1 {
		// No need to split
		page.Data = node.SerializeBNode()
		page.SetDirty(true)
		return node, false, nil
	}
	return tree.SplitInternalNode(node, page, TransID)
}
func (tree *BTree) SplitInternalNode(node *BNode, page *PagePackage.Page, TransID types.TransactionID) (*BNode, bool, error) {
	// Create a new internal node
	newPage, err := tree.BufferPool.NewPage()
	if err != nil {
		return nil, false, err
	}
	midIndex := len(node.Keys) / 2
	moveUpKey := node.Keys[midIndex]

	newNode := &BNode{
		isLeaf:  false,
		Keys:    make([][]byte, len(node.Keys)-midIndex-1),
		PageIDs: make([]types.PageID, len(node.PageIDs)-midIndex-1),
		PageID:  newPage.GetID(),
		Parent:  node.Parent,
	}
	copy(newNode.Keys, node.Keys[midIndex+1:])
	copy(newNode.PageIDs, node.PageIDs[midIndex+1:])

	// Now, we should change the parent of the newNode as the parent of the node

	for _, newChildPageID := range newNode.PageIDs {
		newChildPage, err := tree.BufferPool.ReadPage(newChildPageID)
		if err != nil {
			return nil, false, err
		}
		newChildNode, err := DeSerializeBNode(newChildPage.GetData())
		if err != nil {
			return nil, false, err
		}
		newChildNode.Parent = newNode.PageID
		newChildPage.Data = newChildNode.SerializeBNode()
		newChildPage.SetDirty(true)
		tree.BufferPool.UnPinPage(newChildPage.GetID(), true)
	}
	node.Keys = node.Keys[:midIndex]
	node.PageIDs = node.PageIDs[:midIndex+1]
	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	newPage.Data = newNode.SerializeBNode()
	newPage.SetDirty(true)
	tree.BufferPool.UnPinPage(newPage.GetID(), true)

	// If the node is the root node (splitted), we need to create a new root

	if node.Parent == 0 {
		return tree.createNewRoot(node, newNode, moveUpKey, TransID)
	}

	// The parent might change so we need to update
	parentPage, err := tree.BufferPool.ReadPage(node.Parent)
	if err != nil {
		return nil, false, err
	}
	parentNode, err := DeSerializeBNode(parentPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(parentPage.GetID(), false)
		return nil, false, err
	}
	_, splitOccured, err := tree.insertIntoInternal(parentNode, parentPage, newNode.Keys[0], newNode.PageID, TransID)
	tree.BufferPool.UnPinPage(parentPage.GetID(), true)

	if err != nil {
		return nil, false, err
	}
	return node, splitOccured, nil

}
func (tree *BTree) splitLeafNode(node *BNode, page *PagePackage.Page, TransID types.TransactionID) (*BNode, bool, error) {
	// Create a new leaf node
	newPage, err := tree.BufferPool.NewPage()
	if err != nil {
		return nil, false, err
	}
	midIndex := len(node.Keys) / 2

	newNode := &BNode{
		isLeaf:   true,
		Keys:     make([][]byte, len(node.Keys)-midIndex),
		Values:   make([][]byte, len(node.Values)-midIndex),
		PageID:   newPage.GetID(),
		Parent:   node.Parent,
		NextLeaf: node.NextLeaf,
	}
	/*
		      [5]
		    /     \
		[2,3,5] -> [9,10]

		insert(4)

			  [5]
		    /     \
		[2,3,4,5] -> [9,10]

			    [4 ,  5]
			 /     |    \
		    /      |     \
		[2,3] -> [4,5] -> [9,10]

		node = [2,3]
		newNode = [4,5]
		newNode.NextLeaf = node.NextLeaf
		node.NextLeaf = newNode.PageID
	*/

	copy(newNode.Keys, node.Keys[midIndex:])
	copy(newNode.Values, node.Values[midIndex:])

	node.NextLeaf = newNode.PageID

	node.Keys = node.Keys[:midIndex]
	node.Values = node.Values[:midIndex]

	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	newPage.Data = newNode.SerializeBNode()
	newPage.SetDirty(true)

	tree.BufferPool.UnPinPage(newNode.PageID, true)

	// When splitting the root node, the newNode.Keys[0] will form a new root node
	if node.Parent == 0 { // root node
		return tree.createNewRoot(node, newNode, newNode.Keys[0], TransID)
	}

	// Push up newNode.Keys[0] to the parent and split if needed ans so on
	parentPage, err := tree.BufferPool.ReadPage(node.Parent)
	if err != nil {
		return nil, false, err
	}
	parentNode, err := DeSerializeBNode(parentPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(parentPage.GetID(), false)
		return nil, false, err
	}
	_, splitOccured, err := tree.insertIntoInternal(parentNode, parentPage, newNode.Keys[0], newNode.PageID, TransID)
	if err != nil {
		return nil, false, err
	}
	return node, splitOccured, nil

}

func (tree *BTree) createNewRoot(leftNode *BNode, rightNode *BNode, key []byte, TransID types.TransactionID) (*BNode, bool, error) {
	rootPage, err := tree.BufferPool.NewPage()
	tree.LockManager.AcquireLock(TransID, types.ResourceID(rootPage.ID), ConcurrencyPackage.LockModeExclusive)
	if err != nil {
		return nil, false, err
	}

	rootNode := &BNode{
		isLeaf:  false,
		Keys:    [][]byte{key},
		PageID:  rootPage.GetID(),
		PageIDs: []types.PageID{leftNode.PageID, rightNode.PageID},
		Parent:  0,
	}
	for _, childID := range rootNode.PageIDs {
		childPage, err := tree.BufferPool.ReadPage(childID)
		if err != nil {
			return nil, false, err
		}
		childNode, err := DeSerializeBNode(childPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(childPage.GetID(), false)
			return nil, false, err
		}
		childNode.Parent = rootNode.PageID
		childPage.Data = childNode.SerializeBNode()
		childPage.SetDirty(true)

		tree.BufferPool.UnPinPage(childPage.GetID(), true)
	}

	rootPage.Data = rootNode.SerializeBNode()
	rootPage.SetDirty(true)
	tree.BufferPool.UnPinPage(rootPage.GetID(), true)

	return rootNode, true, nil
}

func (tree *BTree) Delete(key []byte, TransID types.TransactionID) error {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	if err := tree.LockManager.AcquireLock(TransID, types.ResourceID(tree.RootPageID), ConcurrencyPackage.LockModeExclusive); err != nil {
		return err
	}
	rootPage, err := tree.BufferPool.ReadPage(tree.RootPageID)
	if err != nil {
		return err
	}
	rootNode, err := DeSerializeBNode(rootPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(rootPage.GetID(), false)
		return err
	}

	keyFound, err := tree.deleteFromNode(rootNode, rootPage, key)

	if err != nil {
		tree.BufferPool.UnPinPage(rootPage.GetID(), false)
		return err
	}

	if !keyFound {
		tree.BufferPool.UnPinPage(rootPage.GetID(), false)
		return errors.New("key not found")
	}

	if !rootNode.isLeaf && len(rootNode.Keys) == 0 {
		// We need to elect a new root
		newRootID := rootNode.PageIDs[0]
		newRootPage, err := tree.BufferPool.ReadPage(newRootID)
		if err != nil {
			tree.BufferPool.UnPinPage(rootPage.GetID(), true)
			return err
		}
		newRootNode, err := DeSerializeBNode(newRootPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(rootPage.GetID(), true)
			tree.BufferPool.UnPinPage(newRootPage.GetID(), false)
			return err
		}
		newRootNode.Parent = 0
		newRootPage.Data = newRootNode.SerializeBNode()
		newRootPage.SetDirty(true)

		tree.RootPageID = newRootID

		tree.BufferPool.UnPinPage(newRootPage.GetID(), true)
	}
	tree.BufferPool.UnPinPage(rootPage.GetID(), true)
	return nil

}
func (tree *BTree) deleteFromNode(node *BNode, page *PagePackage.Page, key []byte) (bool, error) {
	if node.isLeaf {
		return tree.deleteFromLeafNode(node, page, key)
	}
	childIndex := 0
	for i, nodeKey := range node.Keys {
		if bytes.Compare(key, nodeKey) < 0 {
			break
		}
		childIndex = i + 1
	}
	childPage, err := tree.BufferPool.ReadPage(node.PageIDs[childIndex])
	if err != nil {
		return false, err
	}

	childNode, err := DeSerializeBNode(childPage.GetData())
	if err != nil {
		tree.BufferPool.UnPinPage(childPage.GetID(), false)
		return false, err
	}

	keyFound, err := tree.deleteFromLeafNode(childNode, childPage, key)
	if err != nil {
		tree.BufferPool.UnPinPage(childPage.GetID(), false)
		return false, err
	}
	// After removing, if the childNode is underfull (keys are less than the half (maxChild-1)/2)
	// Only the leaf node is allowed to be underfull
	if len(childNode.Keys) < (int(tree.MaxChilds-1)/2) && (childNode.PageID != tree.RootPageID) {
		// Need to handle an underflow of the childNode (either by merging or borrowing)
		err = tree.handleUnderflow(node, page, childNode, childPage, childIndex)
		if err != nil {
			tree.BufferPool.UnPinPage(childPage.GetID(), true)
			return false, err
		}
	}
	tree.BufferPool.UnPinPage(childPage.GetID(), true)
	return keyFound, nil

}
func (tree *BTree) deleteFromLeafNode(node *BNode, page *PagePackage.Page, key []byte) (bool, error) {
	keyPos := -1
	for i, nodeKey := range node.Keys {
		if bytes.Equal(key, nodeKey) {
			keyPos = i
			break
		}
	}
	if keyPos == -1 {
		return false, nil
	}
	node.Keys = append(node.Keys[:keyPos], node.Keys[keyPos+1:]...)
	node.Values = append(node.Values[:keyPos], node.Values[keyPos+1:]...)
	page.Data = node.SerializeBNode()
	page.SetDirty(true)
	return true, nil
}
func (tree *BTree) handleUnderflow(node *BNode, page *PagePackage.Page, childNode *BNode, childPage *PagePackage.Page, childIndex int) error {
	// To handle the underflow, first try to borrow from another sibling
	// and if you can't, then try merging siblings
	minKeys := int((tree.MaxChilds - 1) / 2)
	if childIndex > 0 {
		// Try to borrow from left sibling
		leftSiblingID := node.PageIDs[childIndex-1]
		leftSiblingPage, err := tree.BufferPool.ReadPage(leftSiblingID)
		if err != nil {
			return err
		}
		leftSiblingNode, err := DeSerializeBNode(leftSiblingPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), false)
			return err
		}
		if len(leftSiblingNode.Keys) > minKeys {
			// Now you can borrow becuase left sibling has enough keys
			if err := tree.borrowFromLeftSibling(node, page, childNode, childPage, leftSiblingNode, leftSiblingPage, childIndex); err != nil {
				tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), true)
				return err
			}
			tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), true)
			return nil
		}
		tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), false)

	} else if childIndex < len(childNode.PageIDs)-1 {
		rightSiblingID := node.PageIDs[childIndex+1]
		rightSiblingPage, err := tree.BufferPool.ReadPage(rightSiblingID)
		if err != nil {
			return err
		}
		rightSiblingNode, err := DeSerializeBNode(rightSiblingPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), false)
			return err
		}
		if len(rightSiblingNode.Keys) > minKeys {
			// You can borrow
			if err := tree.borrowFromRightSibling(node, page, childNode, childPage, rightSiblingNode, rightSiblingPage, childIndex); err != nil {
				tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), true)
				return err
			}
			tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), true)
			return nil
		}
		tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), false)

	}
	if childIndex > 0 {
		// Try meging with left sibling
		leftSiblingID := node.PageIDs[childIndex-1]
		leftSiblingPage, err := tree.BufferPool.ReadPage(leftSiblingID)
		if err != nil {
			return err
		}
		leftSiblingNode, err := DeSerializeBNode(leftSiblingPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), false)
			return err
		}

		if err := tree.mergeWithLeftSibling(node, page, childNode, childPage, leftSiblingNode, leftSiblingPage, childIndex); err != nil {
			tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), true)
			return err
		}
		tree.BufferPool.UnPinPage(leftSiblingPage.GetID(), true)
		return nil
	} else if childIndex < len(node.Keys)-1 {
		rightSiblingID := node.PageIDs[childIndex+1]
		rightSiblingPage, err := tree.BufferPool.ReadPage(rightSiblingID)
		if err != nil {
			return err
		}
		rightSiblingNode, err := DeSerializeBNode(rightSiblingPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), false)
			return err
		}
		if err := tree.mergeWithRightSibling(node, page, childNode, childPage, rightSiblingNode, rightSiblingPage, childIndex); err != nil {
			tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), true)
			return err
		}
		tree.BufferPool.UnPinPage(rightSiblingPage.GetID(), true)
		return nil
	}
	return errors.New("failed to handle underflow, no siblings are available")
}

func (tree *BTree) borrowFromLeftSibling(node *BNode, page *PagePackage.Page, childNode *BNode, childPage *PagePackage.Page, leftSiblingNode *BNode, leftSiblingPage *PagePackage.Page, childIndex int) error {
	if childNode.isLeaf {
		// Just borrow the rightmost data from the left silbling
		// and add to the beginning of the child node since its key
		// smaller than all of the keys in child node
		borrowedKey := leftSiblingNode.Keys[len(leftSiblingNode.Keys)-1]
		borrowedValue := leftSiblingNode.Values[len(leftSiblingNode.Values)-1]

		childNode.Keys = append([][]byte{borrowedKey}, childNode.Keys...)
		childNode.Values = append([][]byte{borrowedValue}, childNode.Values...)

		// pop the borrowed element
		leftSiblingNode.Keys = leftSiblingNode.Keys[:len(leftSiblingNode.Keys)-1]
		leftSiblingNode.Values = leftSiblingNode.Values[:len(leftSiblingNode.Values)-1]

		// since the borrowed element is now the smallest one in the child node
		// we should update it in tha parent node to be our new seperator
		node.Keys[childIndex-1] = borrowedKey

		/*
			   [12]
			   /     \
			[2,4,6,8] [12,15]

			delete(12)

			    [12]
			   /     \
			[2,4,6] [8,15]

			    [8]
			   /     \
			[2,4,6] [8,15]
		*/
	} else {
		// For internal nodes, we rotate
		// Move the kay parent down to the beginning of child node (the one to the right) ans it should points to the child of the borrowed node
		// Move the rightmost key of the leftsibling up to the parent

		parentKey := node.Keys[childIndex-1]

		borrowedKey := leftSiblingNode.Keys[len(leftSiblingNode.Keys)-1]
		borrowedPageID := leftSiblingNode.PageIDs[len(leftSiblingNode.PageIDs)-1]

		childNode.Keys = append([][]byte{parentKey}, childNode.Keys...)
		childNode.PageIDs = append([]types.PageID{borrowedPageID}, childNode.PageIDs...)

		borrowedPage, err := tree.BufferPool.ReadPage(borrowedPageID)
		if err != nil {
			return err
		}

		borrowedChildNode, err := DeSerializeBNode(borrowedPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(borrowedPage.GetID(), false)
			return err
		}

		borrowedChildNode.Parent = childNode.PageID
		borrowedPage.Data = borrowedChildNode.SerializeBNode()
		borrowedPage.SetDirty(true)
		tree.BufferPool.UnPinPage(borrowedPage.GetID(), true)

		// push up the borrowed key to the parent after popping it from the left sibling
		leftSiblingNode.Keys = leftSiblingNode.Keys[:len(leftSiblingNode.Keys)-1]
		leftSiblingNode.PageIDs = leftSiblingNode.PageIDs[:len(leftSiblingNode.PageIDs)-1]

		node.Keys[childIndex-1] = borrowedKey
	}
	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	childPage.Data = node.SerializeBNode()
	childPage.SetDirty(true)

	leftSiblingPage.Data = node.SerializeBNode()
	leftSiblingPage.SetDirty(true)

	return nil
}

func (tree *BTree) borrowFromRightSibling(node *BNode, page *PagePackage.Page, childNode *BNode, childPage *PagePackage.Page, rightSiblingNode *BNode, rightSiblingPage *PagePackage.Page, childIndex int) error {
	if childNode.isLeaf {
		borrowedKey := rightSiblingNode.Keys[0]
		borrowedValue := rightSiblingNode.Values[0]

		childNode.Keys = append(childNode.Keys, borrowedKey)
		childNode.Values = append(childNode.Values, borrowedValue)

		rightSiblingNode.Keys = rightSiblingNode.Keys[1:]
		rightSiblingNode.Values = rightSiblingNode.Values[1:]

		node.Keys[childIndex] = rightSiblingNode.Keys[0]
	} else {
		parentKey := node.Keys[childIndex]

		borrowedKey := rightSiblingNode.Keys[0]
		borrowedPageID := rightSiblingNode.PageIDs[0]

		childNode.Keys = append(childNode.Keys, parentKey)
		childNode.PageIDs = append(childNode.PageIDs, borrowedPageID)

		borrowedChildPage, err := tree.BufferPool.ReadPage(borrowedPageID)
		if err != nil {
			return err
		}
		borrowedChildNode, err := DeSerializeBNode(borrowedChildPage.GetData())
		if err != nil {
			tree.BufferPool.UnPinPage(borrowedChildPage.GetID(), false)
			return err
		}
		borrowedChildNode.Parent = childNode.PageID
		borrowedChildPage.Data = borrowedChildNode.SerializeBNode()
		borrowedChildPage.SetDirty(true)
		tree.BufferPool.UnPinPage(borrowedChildPage.GetID(), true)

		node.Keys[childIndex] = borrowedKey
	}
	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	childPage.Data = childNode.SerializeBNode()
	childPage.SetDirty(true)

	rightSiblingPage.Data = rightSiblingNode.SerializeBNode()
	rightSiblingPage.SetDirty(true)

	return nil
}

func (tree *BTree) mergeWithLeftSibling(node *BNode, page *PagePackage.Page, childNode *BNode, childPage *PagePackage.Page, leftSiblingNode *BNode, leftSiblingPage *PagePackage.Page, childIndex int) error {
	seperatorIndex := childIndex - 1
	if childNode.isLeaf {
		// Just merge them and update the next leaf pointer.
		leftSiblingNode.Keys = append(leftSiblingNode.Keys, childNode.Keys...)
		leftSiblingNode.Values = append(leftSiblingNode.Values, childNode.Values...)

		leftSiblingNode.NextLeaf = childNode.NextLeaf
	} else {
		// Remeber that the parent node is NOT underfull so bringing down the seperator key
		// from the parent to the left sibling will NOT make it empty (might be underfull after that)

		seperatorKey := node.Keys[seperatorIndex]
		leftSiblingNode.Keys = append(leftSiblingNode.Keys, seperatorKey)
		leftSiblingNode.Keys = append(leftSiblingNode.Keys, childNode.Keys...)
		leftSiblingNode.PageIDs = append(leftSiblingNode.PageIDs, childNode.PageIDs...)

		// Update the parent pointers

		for _, childID := range childNode.PageIDs {
			childNodeChildPage, err := tree.BufferPool.ReadPage(childID)
			if err != nil {
				return err
			}
			childNodeChildNode, err := DeSerializeBNode(childNodeChildPage.GetData())
			if err != nil {
				tree.BufferPool.UnPinPage(childNodeChildPage.GetID(), false)
				return err
			}
			childNodeChildNode.Parent = leftSiblingNode.PageID
			childNodeChildPage.Data = childNodeChildNode.SerializeBNode()
			childNodeChildPage.SetDirty(true)
			tree.BufferPool.UnPinPage(childNodeChildPage.GetID(), true)
		}
	}
	// Update the left sibling
	leftSiblingPage.Data = leftSiblingNode.SerializeBNode()
	leftSiblingPage.SetDirty(true)

	// remove the seperator key and page id since they moved to the left sibling
	node.Keys = append(node.Keys[:seperatorIndex], node.Keys[seperatorIndex+1:]...)
	node.PageIDs = append(node.PageIDs[:seperatorIndex+1], node.PageIDs[seperatorIndex+2:]...)

	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	// Delete the childNode
	tree.BufferPool.DeletePage(childNode.PageID)

	return nil
}

func (tree *BTree) mergeWithRightSibling(node *BNode, page *PagePackage.Page, childNode *BNode, childPage *PagePackage.Page, rightSiblingNode *BNode, rightSiblingPage *PagePackage.Page, childIndex int) error {
	seperatorIndex := childIndex

	if childNode.isLeaf {
		node.Keys = append(node.Keys, rightSiblingNode.Keys...)
		node.Values = append(node.Values, rightSiblingNode.Values...)
		node.NextLeaf = rightSiblingNode.NextLeaf
	} else {
		seperatorKey := node.Keys[seperatorIndex]

		childNode.Keys = append(childNode.Keys, seperatorKey)
		node.Keys = append(node.Keys, rightSiblingNode.Keys...)
		node.PageIDs = append(node.PageIDs, rightSiblingNode.PageIDs...)

		for _, childID := range rightSiblingNode.PageIDs {
			rightSiblingChildPage, err := tree.BufferPool.ReadPage(childID)
			if err != nil {
				return err
			}
			rightSiblingChildNode, err := DeSerializeBNode(rightSiblingChildPage.GetData())
			if err != nil {
				tree.BufferPool.UnPinPage(rightSiblingChildPage.GetID(), false)
				return err
			}
			rightSiblingChildNode.Parent = childNode.PageID
			rightSiblingChildPage.Data = rightSiblingChildNode.SerializeBNode()
			rightSiblingChildPage.SetDirty(true)
			tree.BufferPool.UnPinPage(rightSiblingChildPage.GetID(), true)
		}
	}

	childPage.Data = childNode.SerializeBNode()
	childPage.SetDirty(true)

	node.Keys = append(node.Keys[:seperatorIndex], node.Keys[seperatorIndex+1:]...)
	node.PageIDs = append(node.PageIDs[:seperatorIndex+1], node.PageIDs[seperatorIndex+2:]...)

	page.Data = node.SerializeBNode()
	page.SetDirty(true)

	// Delete the childNode
	tree.BufferPool.DeletePage(rightSiblingNode.PageID)

	return nil
}

func (tree *BTree) RangeQuery(startKey []byte, endKey []byte) ([][]byte, error) {
	// Lock for reading
	// tree.mu.Lock()
	// defer tree.mu.Unlock()
	leftNode, leftIndex, err := tree.lowerBound(startKey)
	if err != nil {
		return nil, err
	}
	if leftIndex == -1 {
		return make([][]byte, 0), nil
	}
	rightNode, rightIndex, err := tree.upperBound(endKey)
	if err != nil {
		return nil, err
	}
	if (leftNode.PageID > rightNode.PageID) || ((leftNode.PageID == rightNode.PageID) && (leftIndex > rightIndex)) {
		return nil, errors.New("start key must be before end key")
	}
	leftNodeCopy := *leftNode
	rightNodeCopy := *rightNode

	var resultValues [][]byte
	for {
		if (leftNodeCopy.PageID == rightNodeCopy.PageID) && (leftIndex == rightIndex) {
			break
		}
		if leftIndex == len(leftNodeCopy.Values) {
			nextPageID := leftNodeCopy.NextLeaf
			nextPage, err := tree.BufferPool.ReadPage(nextPageID)
			if err != nil {
				return nil, err
			}
			nextNode, err := DeSerializeBNode(nextPage.GetData())
			if err != nil {
				tree.BufferPool.UnPinPage(nextPage.GetID(), false)
				return nil, err
			}
			leftNodeCopy = *nextNode
			leftIndex = 0
		}
		resultValues = append(resultValues, leftNodeCopy.Values[leftIndex])
		leftIndex++

	}
	return resultValues, nil
}

func (tree *BTree) RangeUpdate(startKey []byte, endKey []byte, value []byte) error {
	// Lock for updating
	// tree.mu.Lock()
	// defer tree.mu.Unlock()
	leftNode, leftIndex, err := tree.lowerBound(startKey)
	if err != nil {
		return err
	}
	if leftIndex == -1 {
		return nil
	}
	rightNode, rightIndex, err := tree.upperBound(endKey)
	if err != nil {
		return err
	}
	log.Println("Starting...", leftIndex, leftNode.Keys, rightIndex, rightNode.Keys)
	if (leftNode.PageID > rightNode.PageID) || ((leftNode.PageID == rightNode.PageID) && (leftIndex > rightIndex)) {
		return errors.New("start key must be before end key")
	}
	for {
		if (leftNode.PageID == rightNode.PageID) && (leftIndex == rightIndex) {
			break
		}
		if leftIndex == len(leftNode.Values) {
			nextPageID := leftNode.NextLeaf
			nextPage, err := tree.BufferPool.ReadPage(nextPageID)
			if err != nil {
				return err
			}
			nextNode, err := DeSerializeBNode(nextPage.GetData())
			if err != nil {
				tree.BufferPool.UnPinPage(nextPage.GetID(), false)
				return err
			}
			leftNode = nextNode
			leftIndex = 0
			continue
		}

		pageID := leftNode.PageID
		leftNode.Values[leftIndex] = value
		leftIndex++
		tree.BufferPool.UnPinPage(pageID, true)

	}
	return nil
}
