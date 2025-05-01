package WriteAheadLog

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sync"

	// modelPackage "db-engine-v2/internal/model"
	"encoding/binary"
	"os"
	"path/filepath"
)

func CreateOrOpenFile(logFilePath string) (*os.File, error) {
	// Ensure the directory exists
	dir := filepath.Dir(logFilePath)
	err := os.MkdirAll(dir, os.ModePerm) // creates /logs if it doesn't exist
	if err != nil {
		return nil, err
	}

	// Now safely open or create the file
	file, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return file, nil
}

type WALManager struct {
	LogFile *os.File
	mu      sync.Mutex
}

func NewWALManager(logFilePath string) (*WALManager, error) {
	// We're going to do a sequantial i/o (O_APPEND)
	file, err := CreateOrOpenFile(logFilePath)
	if err != nil {
		return nil, err
	}
	return &WALManager{
		LogFile: file,
	}, nil
}

func (walm *WALManager) LogRecord(record *WALRecord) error {
	walm.mu.Lock()
	defer walm.mu.Unlock()
	// defer walm.LogFile.Close() // when commiting
	buf := new(bytes.Buffer)

	// Write fixed-size components
	binary.Write(buf, binary.LittleEndian, uint64(record.Timestamp))
	binary.Write(buf, binary.LittleEndian, uint64(record.TransID))
	binary.Write(buf, binary.LittleEndian, uint64(record.RecordType))

	binary.Write(buf, binary.LittleEndian, uint32(len(record.TableName)))
	buf.Write([]byte(record.TableName))
	// Write variable-length components
	// First key
	binary.Write(buf, binary.LittleEndian, uint32(len(record.Key)))
	buf.Write(record.Key)

	// Then old data
	binary.Write(buf, binary.LittleEndian, uint32(len(record.OldData)))
	buf.Write(record.OldData)

	// Finally new data
	binary.Write(buf, binary.LittleEndian, uint32(len(record.NewData)))
	buf.Write(record.NewData)
	// Write to log file and sync
	_, err := walm.LogFile.Write(buf.Bytes())
	if err != nil {
		return err
	}

	// fsync (force writing to the disk)
	return walm.LogFile.Sync()
}

func (walm *WALManager) ReadRecords() ([]WALRecord, error) {
	walm.mu.Lock()
	defer walm.mu.Unlock()

	// Use the existing file handle instead of reopening
	_, err := walm.LogFile.Seek(0, io.SeekStart) // Reset to start
	if err != nil {
		return nil, fmt.Errorf("seek error: %w", err)
	}

	reader := bufio.NewReader(walm.LogFile)
	var records []WALRecord

	// Fixed-length header size: Timestamp (8) + TransID (8) + RecordType (1) = 17 bytes
	headerSize := 24
	buffer := make([]byte, headerSize)

	for {
		// Attempt to read the entire fixed header
		_, err := io.ReadFull(reader, buffer)
		if err != nil {
			if err == io.EOF {
				break // Clean exit
			} else if err == io.ErrUnexpectedEOF {
				// Partial header, discard corrupted data
				break
			}
			return nil, fmt.Errorf("header read failed: %w", err)
		}

		// Parse fixed-length fields
		record := WALRecord{}
		buf := bytes.NewReader(buffer)
		binary.Read(buf, binary.LittleEndian, &record.Timestamp)
		binary.Read(buf, binary.LittleEndian, &record.TransID)
		binary.Read(buf, binary.LittleEndian, &record.RecordType)
		// Read variable-length fields
		if err := readVarLenField(reader, &record.TableName); err != nil {
			break // Discard partial record
		}
		if err := readVarLenBytes(reader, &record.Key); err != nil {
			break
		}
		if err := readVarLenBytes(reader, &record.OldData); err != nil {
			break
		}
		if err := readVarLenBytes(reader, &record.NewData); err != nil {
			break
		}

		records = append(records, record)
	}
	return records, nil
}

// Helper function to read variable-length strings
func readVarLenField(r io.Reader, field *string) error {
	var len uint32
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return err
	}
	data := make([]byte, len)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	*field = string(data)
	return nil
}

// Helper function to read variable-length byte slices
func readVarLenBytes(r io.Reader, field *[]byte) error {
	var len uint32
	if err := binary.Read(r, binary.LittleEndian, &len); err != nil {
		return err
	}
	*field = make([]byte, len)
	if _, err := io.ReadFull(r, *field); err != nil {
		return err
	}
	return nil
}
