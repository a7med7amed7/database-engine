package Page

import "db-engine-v2/types"

type Page struct {
	ID              types.PageID
	Data            []byte
	Dirty           bool // Whether the page has been modified in memory or not
	ConcurrentUsers int
}

func (p *Page) GetID() types.PageID       { return p.ID }
func (p *Page) GetData() []byte           { return p.Data }
func (p *Page) IsDirty() bool             { return p.Dirty }
func (p *Page) SetDirty(dirty bool)       { p.Dirty = dirty }
func (p *Page) GetConcurrentUsers() int   { return p.ConcurrentUsers }
func (p *Page) IncrementConcurrentUsers() { p.ConcurrentUsers++ }
func (p *Page) DecrementConcurrentUsers() { p.ConcurrentUsers-- }
