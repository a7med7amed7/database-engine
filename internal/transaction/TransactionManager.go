package transaction

import (
	ConcurrencyPackage "db-engine-v2/internal/concurrency"
	BufferPoolPackage "db-engine-v2/internal/storage/BufferPool"
	WriteAheadLogPackage "db-engine-v2/internal/wal"
	"db-engine-v2/types"
	"errors"
	"sync"
	"time"
)

type TransactionManager struct {
	NextTransactionID  types.TransactionID
	ActiveTransactions map[types.TransactionID]*Transaction
	WALManager         *WriteAheadLogPackage.WALManager
	BufferPool         *BufferPoolPackage.BufferPoolManager
	LockManager        *ConcurrencyPackage.LockManager
	mu                 sync.Mutex
}

func NewTransactionManager(wal *WriteAheadLogPackage.WALManager, bpm *BufferPoolPackage.BufferPoolManager, lm *ConcurrencyPackage.LockManager) *TransactionManager {
	return &TransactionManager{
		NextTransactionID:  1,
		ActiveTransactions: make(map[types.TransactionID]*Transaction),
		WALManager:         wal,
		BufferPool:         bpm,
		LockManager:        lm,
	}
}

func (tm *TransactionManager) Begin() (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	TransID := tm.NextTransactionID
	tm.NextTransactionID++
	NewTransaction := &Transaction{
		ID:        TransID,
		StartTime: uint64(time.Now().Unix()),
		State:     TransactionStateGrowing,
	}
	WALRecord := WriteAheadLogPackage.WALRecord{
		Timestamp:  uint64(NewTransaction.StartTime),
		TransID:    TransID,
		RecordType: WriteAheadLogPackage.WALRecordTypeBegin,
	}
	if err := tm.WALManager.LogRecord(WALRecord); err != nil {
		return nil, err
	}
	tm.ActiveTransactions[TransID] = NewTransaction
	return NewTransaction, nil
}

func (tm *TransactionManager) Commit(Trans *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if Trans.State == TransactionStateAborted {
		return errors.New("can't commit an aborted transaction")
	}
	WALRecord := WriteAheadLogPackage.WALRecord{
		Timestamp:  uint64(time.Now().Unix()),
		RecordType: WriteAheadLogPackage.WALRecordTypeCommit,
		TransID:    Trans.ID,
	}
	if err := tm.WALManager.LogRecord(WALRecord); err != nil {
		return err
	}

	tm.LockManager.ReleaseAllLocks(Trans.ID)
	Trans.State = TransactionStateCommitted
	delete(tm.ActiveTransactions, Trans.ID)
	return nil
}
func (tm *TransactionManager) Abort(Trans *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if Trans.State == TransactionStateAborted {
		return errors.New("can't abort a committed transaction")
	}
	WALRecord := WriteAheadLogPackage.WALRecord{
		Timestamp:  uint64(time.Now().Unix()),
		RecordType: WriteAheadLogPackage.WALRecordTypeAbort,
		TransID:    Trans.ID,
	}
	if err := tm.WALManager.LogRecord(WALRecord); err != nil {
		return err
	}

	// TODO -> Rollback by applying WAL records in reverse

	tm.LockManager.ReleaseAllLocks(Trans.ID)
	Trans.State = TransactionStateCommitted
	delete(tm.ActiveTransactions, Trans.ID)
	return nil
}
