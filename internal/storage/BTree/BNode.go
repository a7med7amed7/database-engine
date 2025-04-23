package storage

import (
	"bytes"
	"db-engine-v2/types"
	"encoding/binary"
	"log"
)

type BNode struct {
	// True if this node is a leaf node, false otherwise
	isLeaf bool
	// This node's page ID
	PageID types.PageID
	// Parent node's page ID
	Parent types.PageID
	// This node's Keys
	Keys [][]byte

	// -- Leaf Setup --

	// Values of the keys
	Values [][]byte
	// Next leaf pointer for range queries (similar to a linked list)
	NextLeaf types.PageID

	// -- Internal Setup --

	// Page IDs of child nodes (NumKeys + 1)
	PageIDs []types.PageID
}

/*
Converting the content of the node to bytes
so that we can flush it to the disk

The result bytes should have the following format
IsLeaf - KeysCount - Parent - NextLeaf - PageID - Keys[] - (Values[] || PageIDs)

When storing the keys, we should store it as a pair of (num of bytes, key)
so that we can jump to the next key easily when doing deserialization
The same goes for the values
*/
func (node *BNode) SerializeBNode() []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, node.isLeaf)
	binary.Write(buf, binary.LittleEndian, uint16(len(node.Keys)))
	binary.Write(buf, binary.LittleEndian, node.Parent)
	binary.Write(buf, binary.LittleEndian, node.NextLeaf)
	binary.Write(buf, binary.LittleEndian, node.PageID)

	for _, key := range node.Keys {
		err := binary.Write(buf, binary.LittleEndian, uint16(len(key)))
		ShowBNodeError(err)
		_, err = buf.Write(key)
		ShowBNodeError(err)
	}
	if node.isLeaf {
		for _, val := range node.Values {
			err := binary.Write(buf, binary.LittleEndian, uint16(len(val)))
			ShowBNodeError(err)
			_, err = buf.Write(val)
			ShowBNodeError(err)
		}
	} else {
		for _, pid := range node.PageIDs {
			err := binary.Write(buf, binary.LittleEndian, pid)
			ShowBNodeError(err)
		}
	}

	return buf.Bytes()
}

/*
Converting the raw bytes from the disk to its corresponding Bnode
*/
func DeSerializeBNode(RowBNode []byte) (*BNode, error) {
	buf := bytes.NewBuffer(RowBNode)
	node := &BNode{}

	// Read Headers
	var KeysCount uint16
	err := binary.Read(buf, binary.LittleEndian, &node.isLeaf)
	ShowBNodeError(err)
	err = binary.Read(buf, binary.LittleEndian, &KeysCount)
	ShowBNodeError(err)
	err = binary.Read(buf, binary.LittleEndian, &node.Parent)
	ShowBNodeError(err)
	err = binary.Read(buf, binary.LittleEndian, &node.NextLeaf)
	ShowBNodeError(err)
	err = binary.Read(buf, binary.LittleEndian, &node.PageID)
	ShowBNodeError(err)
	// Read Keys
	node.Keys = make([][]byte, KeysCount)
	for i := uint16(0); i < KeysCount; i++ {
		var KeySize uint16
		err := binary.Read(buf, binary.LittleEndian, &KeySize)
		ShowBNodeError(err)

		key := make([]byte, KeySize)
		buf.Read(key)
		node.Keys[i] = key
	}

	if node.isLeaf {
		// Read Values
		node.Values = make([][]byte, KeysCount)
		for i := uint16(0); i < KeysCount; i++ {
			var ValSize uint16
			err := binary.Read(buf, binary.LittleEndian, &ValSize)
			ShowBNodeError(err)

			val := make([]byte, ValSize)
			buf.Read(val)
			node.Values[i] = val
		}
	} else {
		// Read PageIDs
		node.PageIDs = make([]types.PageID, KeysCount+1)
		for i := uint16(0); i < KeysCount+1; i++ {
			err := binary.Read(buf, binary.LittleEndian, &node.PageIDs[i])
			ShowBNodeError(err)
		}

	}

	return node, nil
}
func ShowBNodeError(err error) {
	if err != nil {
		log.Fatal("BNode Error", err)
	}
}
