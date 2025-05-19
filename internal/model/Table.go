package model

import (
	"bytes"
	ConcurrencyPackage "db-engine-v2/internal/concurrency"
	TreePackage "db-engine-v2/internal/storage/BTree"
	BufferPoolPackage "db-engine-v2/internal/storage/BufferPool"
	"log"

	"encoding/binary"
	"errors"

	"db-engine-v2/types"
)

const (
	ColumnTypeInt types.ColumnType = iota
	ColumnTypeFloat
	ColumnTypeString
	ColumnTypeBoolean
)

type Column struct {
	Name    string
	Type    types.ColumnType
	NotNull bool
}

type Schema struct {
	Columns    []Column
	PrimaryKey string
}

type NonClusteredIndex struct {
	ColumnName string // column name which should be indexed
	IndexTree  *TreePackage.BTree
	PrimaryKey string
}

func NewNonClusteredIndex(columnName string, primaryKey string, bufferPool *BufferPoolPackage.BufferPoolManager, lockManager *ConcurrencyPackage.LockManager, maxChild uint16) (*NonClusteredIndex, error) {
	indexTree, err := TreePackage.NewBTree(bufferPool, lockManager, 4)
	if err != nil {
		return nil, err
	}
	return &NonClusteredIndex{
		ColumnName: columnName,
		PrimaryKey: primaryKey,
		IndexTree:  indexTree,
	}, nil
}
func (idx *NonClusteredIndex) Insert(secondaryKey []byte, primaryKey []byte, TransID types.TransactionID) error {
	return idx.IndexTree.Insert(secondaryKey, primaryKey, TransID)
}
func (idx *NonClusteredIndex) Delete(secondaryKey []byte, primaryKey []byte, TransID types.TransactionID) error {
	return idx.IndexTree.Delete(secondaryKey, TransID)
}
func (idx *NonClusteredIndex) Search(secondaryKey []byte) (map[string][]byte, error) {
	rowData, err := idx.IndexTree.SearchValue(secondaryKey)
	if err != nil {
		return nil, err
	}
	return deserializeRow(rowData)
}
func (idx *NonClusteredIndex) RangeQuery(startKey []byte, endKey []byte) ([][]byte, error) {
	rowData, err := idx.IndexTree.RangeQuery(startKey, endKey)
	if err != nil {
		return nil, err
	}
	return rowData, nil
}

type Table struct {
	Name                string
	Schema              Schema
	ClusteredIndex      *TreePackage.BTree
	NonClusteredIndexes map[string]*NonClusteredIndex
}

func NewTable(name string, schema Schema, bufferPool *BufferPoolPackage.BufferPoolManager, lockManager *ConcurrencyPackage.LockManager, maxChild uint16) (*Table, error) {
	indexTree, err := TreePackage.NewBTree(bufferPool, lockManager, 4)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name:                name,
		Schema:              schema,
		ClusteredIndex:      indexTree,
		NonClusteredIndexes: make(map[string]*NonClusteredIndex),
	}, nil
}
func (t *Table) AddNonClusteredIndex(columnName string, buferPool *BufferPoolPackage.BufferPoolManager, lockManager *ConcurrencyPackage.LockManager, maxChilds uint16) error {

	columnExists := false
	for _, col := range t.Schema.Columns {
		if col.Name == columnName {
			columnExists = true
			break
		}
	}

	if !columnExists {
		return errors.New("column doesn't exists")
	}

	index, err := NewNonClusteredIndex(columnName, t.Schema.PrimaryKey, buferPool, lockManager, maxChilds)
	if err != nil {
		return err
	}
	t.NonClusteredIndexes[columnName] = index
	return nil
}

func (t *Table) Insert(row map[string][]byte, TransID types.TransactionID) error {

	primaryKey, ok := row[t.Schema.PrimaryKey]
	if !ok {
		return errors.New("primary key is not provided")
	}

	rowData, err := serializeRow(row)
	if err != nil {
		return err
	}
	if err := t.ClusteredIndex.Insert(primaryKey, rowData, TransID); err != nil {
		return err
	}

	for columnName, index := range t.NonClusteredIndexes {
		if err := index.Insert(row[columnName], primaryKey, TransID); err != nil {
			return err
		}
	}
	return nil
}
func (t *Table) Update(row map[string][]byte, TransID types.TransactionID) error {

	primaryKey, ok := row[t.Schema.PrimaryKey]
	if !ok {
		return errors.New("primary key is not provided")
	}

	rowData, err := serializeRow(row)
	if err != nil {
		return err
	}
	// we can use the Insert function to do our update since if there's a match of the pk
	// we can just update its value since we're sure it does exists
	if err := t.ClusteredIndex.Insert(primaryKey, rowData, TransID); err != nil {
		return err
	}

	// for columnName, index := range t.NonClusteredIndexes {
	// 	if err := index.Insert(row[columnName], primaryKey, TransID); err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
func (t *Table) Delete(row map[string][]byte, TransID types.TransactionID) error {
	primaryKey, ok := row[t.Schema.PrimaryKey]
	if !ok {
		return errors.New("primary key is not provided")
	}
	if err := t.ClusteredIndex.Delete(primaryKey, TransID); err != nil {
		return err
	}
	for columnName, index := range t.NonClusteredIndexes {
		if err := index.Delete(row[columnName], primaryKey, TransID); err != nil {
			return err
		}
	}
	return nil
}
func serializeRow(row map[string][]byte) ([]byte, error) {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.LittleEndian, uint16(len(row)))

	for key, value := range row {
		binary.Write(buf, binary.LittleEndian, uint16(len(key)))
		buf.Write([]byte(key))

		binary.Write(buf, binary.LittleEndian, uint32(len(value)))
		buf.Write(value)
	}
	return buf.Bytes(), nil
}

func deserializeRow(data []byte) (map[string][]byte, error) {
	buf := bytes.NewBuffer(data)
	var fields uint16
	binary.Read(buf, binary.LittleEndian, &fields)

	row := make(map[string][]byte)
	for i := uint16(0); i < fields; i++ {
		var keyLen uint16
		binary.Read(buf, binary.LittleEndian, &keyLen)
		keyBytes := make([]byte, keyLen)
		buf.Read(keyBytes)
		key := string(keyBytes)

		var valLen uint32
		binary.Read(buf, binary.LittleEndian, &valLen)
		valBytes := make([]byte, valLen)
		buf.Read(valBytes)

		row[key] = valBytes

	}
	return row, nil
}

func (t *Table) FindByClusteredIndex(key []byte) (map[string][]byte, error) {
	rowData, err := t.ClusteredIndex.SearchValue(key)
	if err != nil {
		return nil, err
	}
	return deserializeRow(rowData)
}

func (t *Table) RangeQueryByClusteredIndex(startKey []byte, endKey []byte) ([]map[string][]byte, error) {
	log.Println("Range query: ", startKey, endKey)
	rowData, err := t.ClusteredIndex.RangeQuery(startKey, endKey)
	if err != nil {
		return nil, err
	}
	var result []map[string][]byte
	for _, data := range rowData {
		deserializedRow, err := deserializeRow(data)
		if err != nil {
			return nil, err
		}
		result = append(result, deserializedRow)
	}
	return result, nil
}

func (t *Table) FindByNonClusteredIndex(columnName string, key []byte) (map[string][]byte, error) {
	index, ok := t.NonClusteredIndexes[columnName]
	if !ok {
		return nil, errors.New("no such index on this column")
	}

	primaryKeyData, err := index.Search(key)
	if err != nil {
		return nil, err
	}

	row, err := t.FindByClusteredIndex(primaryKeyData[t.Schema.PrimaryKey])
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (t *Table) RangeQueryByNonClusteredIndex(columnName string, startKey []byte, endKey []byte) ([]map[string][]byte, error) {
	index, ok := t.NonClusteredIndexes[columnName]
	if !ok {
		return nil, errors.New("no such index on this column")
	}

	primaryKeyData, err := index.RangeQuery(startKey, endKey)
	if err != nil {
		return nil, err
	}
	var result []map[string][]byte
	for _, pkMap := range primaryKeyData {
		row, err := t.FindByClusteredIndex(pkMap)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, nil
}
