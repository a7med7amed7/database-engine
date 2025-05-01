package enums

type QueryType int

const (
	QueryTypeInsert QueryType = iota
	QueryTypeDelete
	QueryTypeUpdate
	QueryTypeSelect
)
