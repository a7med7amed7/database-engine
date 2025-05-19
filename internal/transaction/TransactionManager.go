package transaction

import (
	"bytes"
	modelPackage "db-engine-v2/internal/model"
	WriteAheadLogPackage "db-engine-v2/internal/wal"
	"db-engine-v2/types"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

func DeleteFile(f *os.File) error {
	// Always close before deleting
	err := f.Close()
	if err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Remove the file by its name
	err = os.Remove(f.Name()) // Name -> path in cwd (current working directory)
	if err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	return nil
}

type TransactionManager struct {
	NextTransactionID  types.TransactionID
	ActiveTransactions map[types.TransactionID]*Transaction
	DB                 *modelPackage.Database
	mu                 sync.Mutex
}

func NewTransactionManager(db *modelPackage.Database) *TransactionManager {
	return &TransactionManager{
		NextTransactionID:  1,
		ActiveTransactions: make(map[types.TransactionID]*Transaction),
		DB:                 db,
	}

}

func (tm *TransactionManager) Begin() (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	TransID := tm.NextTransactionID
	tm.NextTransactionID++
	WALFilePath := fmt.Sprintf("logs/trans_%d.wal", TransID)
	WALManager, err := WriteAheadLogPackage.NewWALManager(WALFilePath)
	if err != nil {
		return nil, err
	}
	NewTransaction := &Transaction{
		ID:         TransID,
		StartTime:  uint64(time.Now().Unix()),
		State:      TransactionStateGrowing,
		WALManager: WALManager,
	}
	WALRecord := &WriteAheadLogPackage.WALRecord{
		Timestamp:  uint64(NewTransaction.StartTime),
		TransID:    TransID,
		RecordType: WriteAheadLogPackage.WALRecordTypeBegin,
	}
	if err := WALManager.LogRecord(WALRecord); err != nil {
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
	// WALRecord := &WriteAheadLogPackage.WALRecord{
	// 	Timestamp:  uint64(time.Now().Unix()),
	// 	RecordType: WriteAheadLogPackage.WALRecordTypeCommit,
	// 	TransID:    Trans.ID,
	// }
	// if err := tm.DB.WALManager.LogRecord(WALRecord); err != nil {
	// 	return err
	// }
	tm.DB.LockManager.ReleaseAllLocks(Trans.ID)
	Trans.State = TransactionStateCommitted
	delete(tm.ActiveTransactions, Trans.ID)
	DeleteFile(Trans.WALManager.LogFile)
	return nil
}
func (tm *TransactionManager) Abort(Trans *Transaction) error {
	// tm.mu.Lock()
	// defer tm.mu.Unlock()
	fmt.Println("Aborting", Trans.ID)
	if Trans.State == TransactionStateCommitted {
		return errors.New("can't abort a committed transaction")
	}
	WALRecord := &WriteAheadLogPackage.WALRecord{
		Timestamp:  uint64(time.Now().Unix()),
		RecordType: WriteAheadLogPackage.WALRecordTypeAbort,
		TransID:    Trans.ID,
	}
	if err := tm.DB.WALManager.LogRecord(WALRecord); err != nil {
		return err
	}
	records, err := Trans.WALManager.ReadRecords()
	if err != nil {
		return err
	}
	if err := tm.rollback(records, Trans); err != nil {
		return err
	}
	tm.DB.LockManager.ReleaseAllLocks(Trans.ID)
	Trans.State = TransactionStateCommitted
	delete(tm.ActiveTransactions, Trans.ID)
	DeleteFile(Trans.WALManager.LogFile)
	return nil
}
func (tm *TransactionManager) rollback(records []WriteAheadLogPackage.WALRecord, Trans *Transaction) error {
	// need to query here
	log.Print("Rolling back ", Trans.ID)
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].RecordType == WriteAheadLogPackage.WALRecordTypeBegin {
			break
		} else if records[i].RecordType == WriteAheadLogPackage.WALRecordTypeInsert {
			row := map[string][]byte{"id": records[i].Key}
			tm.Delete(records[i].TableName, row, Trans)
		}
	}
	return tm.Commit(Trans)
}

type Query struct {
	Type     string
	Column   string
	StartKey []byte
	EndKey   []byte            // For SELECT range query
	Row      map[string][]byte // update cols with values or insert them based on the query
	TransID  types.TransactionID
	Table    *modelPackage.Table
}

func NewQuery(queryType string, column string, startKey []byte, endKey []byte, row map[string][]byte, transID types.TransactionID, table *modelPackage.Table) *Query {
	return &Query{
		Type:     queryType,
		StartKey: startKey,
		EndKey:   endKey,
		Row:      row,
		TransID:  transID,
		Table:    table,
		Column:   column,
	}
}

// utils

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

func (tm *TransactionManager) Insert(tableName string, row map[string][]byte, transaction *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// Clustered Index
	pk := row[tm.DB.Tables[tableName].Schema.PrimaryKey]
	serializedRow, err := serializeRow(row)
	if err != nil {
		return nil
	}
	record := WriteAheadLogPackage.NewWALRecord(
		uint64(time.Now().Unix()),
		transaction.ID,
		WriteAheadLogPackage.WALRecordTypeInsert,
		tableName,
		pk,
		nil,
		serializedRow,
	)
	currentTransaction := tm.ActiveTransactions[transaction.ID]
	currentTransaction.WALManager.LogRecord(record)
	if err := tm.DB.Tables[tableName].Insert(row, transaction.ID); err != nil {
		return err
	}
	return nil
}
func (tm *TransactionManager) Update(tableName string, row map[string][]byte, transaction *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// Clustered Index
	pk := row[tm.DB.Tables[tableName].Schema.PrimaryKey]
	serializedRow, err := serializeRow(row)
	if err != nil {
		return err
	}
	oldRow, err := tm.DB.Tables[tableName].FindByClusteredIndex(pk)
	if err != nil {
		return err
	}
	serializedOldRow, err := serializeRow(oldRow)
	if err != nil {
		return err
	}
	record := WriteAheadLogPackage.NewWALRecord(
		uint64(time.Now().Unix()),
		transaction.ID,
		WriteAheadLogPackage.WALRecordTypeUpdate,
		tableName,
		pk,
		serializedOldRow,
		serializedRow,
	)
	currentTransaction := tm.ActiveTransactions[transaction.ID]
	currentTransaction.WALManager.LogRecord(record)
	if err := tm.DB.Tables[tableName].Update(row, transaction.ID); err != nil {
		return err
	}
	return nil
}

func (tm *TransactionManager) Delete(tableName string, row map[string][]byte, transaction *Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// Clustered Index
	pk := row[tm.DB.Tables[tableName].Schema.PrimaryKey]
	serializedRow, err := serializeRow(row)
	if err != nil {
		return nil
	}
	record := WriteAheadLogPackage.NewWALRecord(
		uint64(time.Now().Unix()),
		transaction.ID,
		WriteAheadLogPackage.WALRecordTypeDelete,
		tableName,
		pk,
		serializedRow,
		nil,
	)
	currentTransaction := tm.ActiveTransactions[transaction.ID]
	currentTransaction.WALManager.LogRecord(record)
	if err := tm.DB.Tables[tableName].Delete(row, transaction.ID); err != nil {
		return err
	}
	return nil
}
func (tm *TransactionManager) Select(tableName string, startKey []byte, endKey []byte, transaction *Transaction) ([]map[string][]byte, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	data, err := tm.DB.Tables[tableName].RangeQueryByClusteredIndex(startKey, endKey)
	if err != nil {
		return nil, err
	}
	return data, nil
}
