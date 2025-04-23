package DiskManager

import (
	PagePackage "db-engine-v2/internal/storage/Page"
	"db-engine-v2/types"
	"os"
	"sync"
)

type DiskManager struct {
	DBFile     *os.File
	PageSize   uint16 // in bytes
	NextPageID types.PageID
	mu         sync.Mutex // Lock for thread safety
	// Mutux -> Allow only one user to lock while the other wait
}

// Constructor
func NewDiskManager(dbFilePath string, pageSize uint16) (*DiskManager, error) {
	// Read the page file with (read/write) access (if file doesn't exists, we should create one)
	file, err := os.OpenFile(dbFilePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return &DiskManager{
		DBFile:     file,
		PageSize:   pageSize,
		NextPageID: 0,
	}, nil
}

func (dm *DiskManager) ReadPageFromDisk(PageId types.PageID) (*PagePackage.Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	data := make([]byte, dm.PageSize)
	offset := int64(PageId) * int64(dm.PageSize)
	_, err := dm.DBFile.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	return &PagePackage.Page{
		ID:              PageId,
		Data:            data,
		Dirty:           false,
		ConcurrentUsers: 0,
	}, nil
}

func (dm *DiskManager) WritePageAtDisk(page *PagePackage.Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(page.ID) * int64(dm.PageSize)
	_, err := dm.DBFile.WriteAt(page.Data, int64(offset))
	return err
}
func (dm *DiskManager) AllocatePage() types.PageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	PageId := dm.NextPageID
	dm.NextPageID++
	return PageId
}
