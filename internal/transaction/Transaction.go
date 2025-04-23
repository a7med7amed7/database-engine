package transaction

import "db-engine-v2/types"

type Transaction struct {
	ID        types.TransactionID
	StartTime uint64
	State     TransactionState
}

type TransactionState uint64

const (
	TransactionStateGrowing TransactionState = iota
	TransactionStateShrinking
	TransactionStateCommitted
	TransactionStateAborted
)
