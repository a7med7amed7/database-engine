package concurrency

import (
	"db-engine-v2/types"
	"errors"
	"fmt"
	"sync"
)

type LockMode int

const (
	LockModeShared    LockMode = iota // Multiple transactions can read
	LockModeExclusive                 // Only one transaction can read/write
)

type LockManager struct {
	// Which transactions hold which resources
	LockTable map[types.ResourceID]map[types.TransactionID]LockMode
	// User for handling deadlocks
	WaitForGraph map[types.TransactionID]map[types.TransactionID]bool
	mu           sync.Mutex
}

// Constructor
func NewLockManager() *LockManager {
	return &LockManager{
		LockTable:    make(map[types.ResourceID]map[types.TransactionID]LockMode),
		WaitForGraph: make(map[types.TransactionID]map[types.TransactionID]bool),
	}
}

func (lm *LockManager) AcquireLock(transID types.TransactionID, resourceID types.ResourceID, mode LockMode) error {
	fmt.Printf("Transaction %d attempting to acquire %d lock on resource %d\n", transID, mode, resourceID)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Initialize entry if not exists
	if _, ok := lm.LockTable[resourceID]; !ok {
		lm.LockTable[resourceID] = make(map[types.TransactionID]LockMode)
	}

	// Check if the transaction already holds a lock at this resource
	if curMode, ok := lm.LockTable[resourceID][transID]; ok {
		if curMode == mode || curMode == LockModeExclusive {
			return nil
		}
		// Upgrade the lock to exclusive if needed
		if mode == LockModeExclusive && curMode == LockModeShared {
			// If other transactions are using this resource, we can't upgrade
			if len(lm.LockTable[resourceID]) > 1 {
				return errors.New("lock upgrade not possible, resource held by other transactions")
			}
			lm.LockTable[resourceID][transID] = mode
			return nil
		}
	}
	// If all locks are shared and our mode is also shared, we can acquire, otherwise, nope
	for otherTransID, otherMode := range lm.LockTable[resourceID] {
		if otherTransID == transID {
			continue
		}
		if !(mode == LockModeShared && otherMode == LockModeShared) {
			return errors.New("lock acquisition failed, other transaction is using such resource")
			// TODO in future version
			// Update WaitForGraph and check for deadlocks
			// You shouldn't return an error, you should wait until the resource is free
		}
	}
	lm.LockTable[resourceID][transID] = mode
	fmt.Printf("Transaction %d successfully acquired %d lock on resource %d\n", transID, mode, resourceID)

	return nil
}

func (lm *LockManager) ReleaseLock(transID types.TransactionID, resourceID types.ResourceID) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if transLocks, ok := lm.LockTable[resourceID]; ok {
		if _, hasLock := transLocks[transID]; hasLock {
			// Remove the transaction from the resource lock list
			delete(transLocks, transID)
			// If no transaction acquires this resource, free it as some transaction will use it in the future
			if len(transLocks) == 0 {
				delete(lm.LockTable, resourceID)
			}
			return true
		}
	}
	return false
}

func (lm *LockManager) ReleaseAllLocks(transID types.TransactionID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	/*
		TODO: optimization
		We can map the used resources by each transaction so that when we
		need to remove all used resources, we can remove it from there
		without any need to loop over all resources
	*/
	for resourceID, transLocks := range lm.LockTable {
		if _, hasTransID := transLocks[transID]; hasTransID {
			delete(transLocks, transID)
			if len(transLocks) == 0 {
				delete(lm.LockTable, resourceID)
			}
		}
	}
	delete(lm.WaitForGraph, transID)
	// Delete this transaction from the wait-list of each transaction
	// We can also optimize it as above
	for otherTransID, waitsFor := range lm.WaitForGraph {
		delete(waitsFor, transID)
		if len(waitsFor) == 0 {
			delete(lm.WaitForGraph, otherTransID)
		}
	}
}
