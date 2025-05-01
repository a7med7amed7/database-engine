package transaction

import (
	WriteAheadLogPackage "db-engine-v2/internal/wal"
	"db-engine-v2/types"
)

type Transaction struct {
	ID         types.TransactionID
	StartTime  uint64
	State      TransactionState
	WALManager *WriteAheadLogPackage.WALManager
}

type TransactionState uint64

const (
	TransactionStateGrowing TransactionState = iota
	TransactionStateShrinking
	TransactionStateCommitted
	TransactionStateAborted
)
