package WriteAheadLog

import (
	"db-engine-v2/types"
	"fmt"
)

type WALRecord struct {
	Timestamp  uint64
	TransID    types.TransactionID
	RecordType WALRecordType
	TableName  string // The name of the table being modified
	Key        []byte
	OldData    []byte // Previous data (for rollback)
	NewData    []byte
}

type WALRecordType uint64

const (
	WALRecordTypeUpdate WALRecordType = iota
	WALRecordTypeInsert
	WALRecordTypeDelete
	WALRecordTypeBegin
	WALRecordTypeCommit
	WALRecordTypeAbort
)

func NewWALRecord(timestamp uint64, transID types.TransactionID, recordType WALRecordType,
	tableName string, key, oldData, newData []byte) *WALRecord {
	record := &WALRecord{
		Timestamp:  timestamp,
		TransID:    transID,
		RecordType: recordType,
		TableName:  tableName,
		Key:        key,
		OldData:    oldData,
		NewData:    newData,
	}
	record.debugWALRecord()
	return record
}

func (record *WALRecord) debugWALRecord() {
	fmt.Println(record.Timestamp, record.RecordType)
}
