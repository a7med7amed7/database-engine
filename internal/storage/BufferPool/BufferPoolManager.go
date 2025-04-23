package BufferPool

import (
	"db-engine-v2/types"
	"errors"
	"sync"

	PagePackage "db-engine-v2/internal/storage/Page"

	DiskPackage "db-engine-v2/internal/storage/Disk"
)

type BufferPoolManager struct {
	// Our cache
	Pages       map[types.PageID]*PagePackage.Page
	DiskManager *DiskPackage.DiskManager
	// Max Number of pages we can store
	MaxPages uint32
	// Allow multiple reads but only one user can write
	mu sync.RWMutex
	// LRU replacement pplicy
	// the pages that haven't been recently accessed should be removed when the cache is full
	ReplacerQueue []types.PageID
}

// Constructor
func NewBufferPoolManager(diskManager *DiskPackage.DiskManager, maxPages uint32) *BufferPoolManager {
	return &BufferPoolManager{
		Pages:         make(map[types.PageID]*PagePackage.Page),
		DiskManager:   diskManager,
		MaxPages:      maxPages,
		ReplacerQueue: make([]types.PageID, 0),
	}
}

/*
1. Check if the page exists in our buffer pool cache
  - if so, update the replacer queue and return the page
  - otherwise, we need to fetch it from the disk

2. First, we need to check if we can cache it or we need to evict an existing old page from the cache
3. Use the disk manager to read the page from the disk
4. Update our buffer pool by caching this page
*/
func (bpm *BufferPoolManager) ReadPage(pageID types.PageID) (*PagePackage.Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if page, ok := bpm.Pages[pageID]; ok {
		bpm.updateReplacerQueue(pageID)
		page.IncrementConcurrentUsers()
		return page, nil
	}
	if uint32(len(bpm.Pages)) >= bpm.MaxPages {
		if err := bpm.evictPage(); err != nil {
			return nil, err
		}
	}

	page, err := bpm.DiskManager.ReadPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}
	bpm.Pages[pageID] = page
	bpm.ReplacerQueue = append(bpm.ReplacerQueue, pageID)
	page.IncrementConcurrentUsers()
	return page, nil
}
func (bpm *BufferPoolManager) FlushPageFromBufferPoolToDisk(pageID types.PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if page, ok := bpm.Pages[pageID]; ok && page.IsDirty() {
		if err := bpm.DiskManager.WritePageAtDisk(page); err != nil {
			return err
		}
		page.SetDirty(false)
	}
	return nil
}
func (bpm *BufferPoolManager) NewPage() (*PagePackage.Page, error) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if uint32(len(bpm.Pages)) >= bpm.MaxPages {
		if err := bpm.evictPage(); err != nil {
			return nil, err
		}
	}
	pageID := bpm.DiskManager.AllocatePage()
	page := &PagePackage.Page{
		ID:              pageID,
		Data:            make([]byte, bpm.DiskManager.PageSize),
		Dirty:           true,
		ConcurrentUsers: 1,
	}
	bpm.Pages[pageID] = page
	bpm.ReplacerQueue = append(bpm.ReplacerQueue, pageID)
	return page, nil
}

func (bpm *BufferPoolManager) UnPinPage(pageID types.PageID, isDirty bool) bool {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	if page, ok := bpm.Pages[pageID]; ok {
		if page.GetConcurrentUsers() <= 0 {
			return false
		}
		page.DecrementConcurrentUsers()
		page.SetDirty(true)
		return true
	}
	return false
}
func (bpm *BufferPoolManager) FlushAllPages() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	for _, page := range bpm.Pages {
		if page.IsDirty() {
			if err := bpm.DiskManager.WritePageAtDisk(page); err != nil {
				return err
			}
			page.SetDirty(false)
		}
	}
	return nil
}
func (bpm *BufferPoolManager) evictPage() error {
	for i, pageID := range bpm.ReplacerQueue {
		page := bpm.Pages[pageID]
		if page.GetConcurrentUsers() == 0 {
			// Nobody uses this page so we can evict it
			if page.IsDirty() {
				if err := bpm.DiskManager.WritePageAtDisk(page); err != nil {
					return err
				}
				delete(bpm.Pages, pageID)
				// remove this pageID from this position
				bpm.ReplacerQueue = append(bpm.ReplacerQueue[:i], bpm.ReplacerQueue[i+1:]...)
				return nil
			}
		}
	}
	return errors.New("all pages are being used right now")
}

// Add the page to the end of the queue
func (bpm *BufferPoolManager) updateReplacerQueue(pageID types.PageID) {
	for i, pid := range bpm.ReplacerQueue {
		if pid == pageID {
			// remove this pageID from this position
			bpm.ReplacerQueue = append(bpm.ReplacerQueue[:i], bpm.ReplacerQueue[i+1:]...)
			break
		}
	}
	// Add pageID to the end of the queue as it's the most recently used page
	bpm.ReplacerQueue = append(bpm.ReplacerQueue, pageID)
}

func (bpm *BufferPoolManager) deleteFromReplacerQueue(pageID types.PageID) {
	for i, pid := range bpm.ReplacerQueue {
		if pid == pageID {
			// remove this pageID from this position
			bpm.ReplacerQueue = append(bpm.ReplacerQueue[:i], bpm.ReplacerQueue[i+1:]...)
			break
		}
	}
}

func (bpm *BufferPoolManager) DeletePage(pageID types.PageID) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	delete(bpm.Pages, pageID)
	bpm.deleteFromReplacerQueue(pageID)
}
