package transaction

import (
	modelPackage "db-engine-v2/internal/model"
	"fmt"
	"strconv"
)

type FilterManager struct {
	db        *modelPackage.Database
	tableName string
	rows      []map[string][]byte
}

func NewFilterManager(db *modelPackage.Database, tableName string, rows []map[string][]byte) *FilterManager {
	return &FilterManager{
		db:        db,
		tableName: tableName,
		rows:      rows,
	}
}

func FilterGreater[T comparable](fm *FilterManager, column string, val T) {
	var newRows []map[string][]byte

	for _, row := range fm.rows {
		raw, ok := row[column]
		if !ok || len(raw) < 8 {
			continue
		}
		var keep bool
		switch any(val).(type) {
		case int:
			parsed, err := strconv.Atoi(string(raw))
			if err == nil && parsed > any(val).(int) {
				keep = true
			}
		case float64:
			parsed, err := strconv.ParseFloat(string(raw), 64)
			if err == nil && parsed > any(val).(float64) {
				keep = true
			}
		case string:
			if string(raw) > any(val).(string) {
				keep = true
			}
		default:
			// Unsupported type
			continue
		}

		if keep {
			newRows = append(newRows, row)
		}
	}

	fm.rows = newRows
	fmt.Println("NewRows", newRows)
}
