package model

import (
	"bytes"
	TransactionPackage "db-engine-v2/internal/transaction"
	"errors"
	"fmt"
)

type QueryType int

const (
	QueryTypeInsert QueryType = iota
	QueryTypeDelete
	QueryTypeUpdate
	QueryTypeSelect
)

type Query struct {
	Type     QueryType
	Column   string
	StartKey []byte
	EndKey   []byte            // For SELECT range query
	Row      map[string][]byte // update cols with values or insert them based on the query
	Trans    *TransactionPackage.Transaction
	Table    *Table
}

func NewQuery(queryType QueryType, column string, startKey []byte, endKey []byte, row map[string][]byte, trans *TransactionPackage.Transaction, table *Table) *Query {
	return &Query{
		Type:     queryType,
		StartKey: startKey,
		EndKey:   endKey,
		Row:      row,
		Trans:    trans,
		Table:    table,
		Column:   column,
	}
}
func (q *Query) Execute() ([]map[string][]byte, error) {
	if q.Type == QueryTypeSelect {
		if q.Table.Schema.PrimaryKey == q.Column {
			if bytes.Equal(q.StartKey, q.EndKey) {
				row, err := q.Table.FindByClusteredIndex(q.StartKey, q.Trans)
				if err != nil {
					return nil, err
				}
				var result []map[string][]byte
				result = append(result, row)
				return result, nil
			} else if bytes.Compare(q.EndKey, q.StartKey) > 0 {
				return q.Table.RangeQueryByClusteredIndex(q.StartKey, q.EndKey, q.Trans)
			} else {
				return nil, errors.New("end key must be smaller than start key")
			}
		} else if _, ok := q.Table.NonClusteredIndexes[q.Column]; ok {
			if bytes.Equal(q.StartKey, q.EndKey) {
				row, err := q.Table.FindByNonClusteredIndex(q.Column, q.StartKey, q.Trans)
				if err != nil {
					return nil, err
				}
				var result []map[string][]byte
				result = append(result, row)
				return result, nil
			} else if bytes.Compare(q.EndKey, q.StartKey) > 0 {
				return q.Table.RangeQueryByNonClusteredIndex(q.Column, q.StartKey, q.EndKey, q.Trans)
			} else {
				return nil, errors.New("end key must be smaller than start key")
			}
		} else {
			// TODO -> Full table scan
			fmt.Println("Full table scan")
		}
	} else if q.Type == QueryTypeInsert {
		if err := q.Table.Insert(q.Row, q.Trans); err != nil {
			return nil, err
		}
	} else if q.Type == QueryTypeDelete {
		q2 := NewQuery(QueryTypeSelect, q.Column, q.StartKey, q.EndKey, q.Row, q.Trans, q.Table)
		rows, err := q2.Execute()
		if err != nil {
			return nil, err
		}
		// idk if removing as a bulk would affect the rotation or not (probably not)
		for _, row := range rows {
			if err := q.Table.Delete(row, q.Trans); err != nil {
				return nil, err
			}
		}
	} else if q.Type == QueryTypeUpdate {
		// for key, value := range q.Row {
		// 	if _, ok := q.Table.NonClusteredIndexes[key]; !ok {
		// 		continue
		// 	}
		// 	// Key is indexed
		// 	q2 := NewQuery(QueryTypeSelect, key, q.StartKey, q.EndKey, q.Row, q.Trans, q.Table)
		// 	rows, err := q2.Execute()
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	index := q.Table.NonClusteredIndexes[key]
		// 	for _, row := range rows {
		// 		if row[index.PrimaryKey] != q.Row[q.Table.Schema.PrimaryKey] {
		// 			continue
		// 		}
		// 		err := index.IndexTree.Delete(value, q.Trans)
		// 		serializedRow, err := serializeRow(row)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		// if inserting a existing key, the value will be updated
		// 		if err := q.Table.ClusteredIndex.Insert(row[q.Table.Schema.PrimaryKey], serializedRow, q.Trans); err != nil {
		// 			return nil, err
		// 		}
		// 	}
		// 	q3 := NewQuery(QueryTypeDelete, q.Column, q.StartKey, q.EndKey, nil, q.Trans, q.Table)
		// 	_, err = q3.Execute()
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	if key == q.Table.Schema.PrimaryKey {
		// 		continue
		// 	}
		// 	for i, _ := range rows {
		// 		rows[i][key] = value
		// 	}
		// }

	}
	return nil, nil
}
