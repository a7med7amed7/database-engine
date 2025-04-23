package WriteAheadLog

import (
	"db-engine-v2/types"
)

type WALRecord struct {
	Timestamp  uint64
	TransID    types.TransactionID
	RecordType WALRecordType
	PageID     types.PageID // The ID of the modified page
	oldData    []byte       // Previous data (for rollback)
	newData    []byte
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
