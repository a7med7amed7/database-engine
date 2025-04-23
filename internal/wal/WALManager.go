package WriteAheadLog

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

type WALManager struct {
	LogFile *os.File
	mu      sync.Mutex
}

func NewWALManager(logFilePath string) (*WALManager, error) {
	// We're going to do a sequantial i/o (O_APPEND)
	file, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &WALManager{
		LogFile: file,
	}, nil
}

func (walm *WALManager) LogRecord(record WALRecord) error {
	walm.mu.Lock()
	defer walm.mu.Unlock()
	buf := new(bytes.Buffer) // store the log data (ready for persistance)
	binary.Write(buf, binary.LittleEndian, record.Timestamp)
	binary.Write(buf, binary.LittleEndian, record.TransID)
	binary.Write(buf, binary.LittleEndian, record.RecordType)
	binary.Write(buf, binary.LittleEndian, record.PageID)

	binary.Write(buf, binary.LittleEndian, uint32(len(record.oldData)))
	buf.Write(record.oldData)
	binary.Write(buf, binary.LittleEndian, uint32(len(record.newData)))
	buf.Write(record.newData)

	// Write buffer to log file
	// This is actually not persistant in the disk, it written in the page cache
	// So until now, the data is buffered
	_, err := walm.LogFile.Write(buf.Bytes())
	if err != nil {
		return err
	}
	// Force write to the disk (fsync)
	return walm.LogFile.Sync()
}

func (walm *WALManager) Recover() ([]WALRecord, error) {
	// should return all record from the wal file
	// loop through it in a reverse order to rollback
	return nil, nil
}
