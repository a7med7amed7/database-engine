package model

import (
	ConcurrencyPackage "db-engine-v2/internal/concurrency"
	BufferPoolPackage "db-engine-v2/internal/storage/BufferPool"
	DiskManager "db-engine-v2/internal/storage/Disk"
	WriteAheadLog "db-engine-v2/internal/wal"
	"errors"
)

type Database struct {
	Name        string
	Tables      map[string]*Table
	BufferPool  *BufferPoolPackage.BufferPoolManager
	DiskManager *DiskManager.DiskManager
	LockManager *ConcurrencyPackage.LockManager
	WALManager  *WriteAheadLog.WALManager
}

func NewDatabase(name string, dbFilePath string, logFilePath string, pageSize uint32, poolSize uint32) (*Database, error) {
	diskManager, err := DiskManager.NewDiskManager(dbFilePath, uint16(pageSize))
	if err != nil {
		return nil, err
	}
	lockManager := ConcurrencyPackage.NewLockManager()
	bufferPoolManager := BufferPoolPackage.NewBufferPoolManager(diskManager, poolSize)
	walManager, err := WriteAheadLog.NewWALManager(logFilePath)
	if err != nil {
		return nil, err
	}
	return &Database{
		Name:        name,
		Tables:      make(map[string]*Table),
		LockManager: lockManager,
		BufferPool:  bufferPoolManager,
		DiskManager: diskManager,
		WALManager:  walManager,
	}, nil
}

func (db *Database) CreateTable(name string, schema Schema, maxChilds uint16) (*Table, error) {

	if _, exists := db.Tables[name]; exists {
		return nil, errors.New("table already exists")
	}
	table, err := NewTable(name, schema, db.BufferPool, db.LockManager, maxChilds)
	if err != nil {
		return nil, err
	}
	db.Tables[name] = table
	return table, nil
}

func (db *Database) GetTable(name string) (*Table, error) {
	table, exists := db.Tables[name]
	if !exists {
		return nil, errors.New("table doesn't exists")
	}
	return table, nil
}

func (db *Database) Close() error {
	if err := db.BufferPool.FlushAllPages(); err != nil {
		return err
	}
	if err := db.DiskManager.DBFile.Close(); err != nil {
		return err
	}
	if err := db.WALManager.LogFile.Close(); err != nil {
		return err
	}
	return nil
}
