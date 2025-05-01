package main

import (
	model "db-engine-v2/internal/model"
	TransactionPackage "db-engine-v2/internal/transaction"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomString(length int) string {
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(result)
}

func IntToBytes(n int) []byte {
	b := make([]byte, 8) // assuming 64-bit int
	binary.BigEndian.PutUint64(b, uint64(n))
	return b
}
func main() {
	db, err := model.NewDatabase("testdb", "testdb.dat", "testdb.wal", 4096, 20)
	if err != nil {
		fmt.Println("error creating a database")
		return
	}
	defer db.Close()
	schema := model.Schema{
		Columns: []model.Column{
			{Name: "id", Type: model.ColumnTypeInt, NotNull: true},
			{Name: "name", Type: model.ColumnTypeString, NotNull: true},
			{Name: "age", Type: model.ColumnTypeInt, NotNull: true},
			{Name: "score", Type: model.ColumnTypeInt, NotNull: false},
		},
		PrimaryKey: "id",
	}
	_, err = db.CreateTable("users", schema, 4)
	if err != nil {
		fmt.Println("error creating a table")
		return
	}
	_, err = db.GetTable("users")
	if err != nil {
		fmt.Println("error getting a table")
		return
	}
	TransManager := TransactionPackage.NewTransactionManager(db)
	var wg sync.WaitGroup
	for i := 1; i < 2; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Create a new transaction for each goroutine
			localTrans, err := TransManager.Begin()
			if err != nil {
				fmt.Println("Error beginning transaction:", err)
				return
			}
			row := map[string][]byte{
				"id":    IntToBytes(index),
				"name":  []byte(RandomString(6)),
				"age":   []byte{0, 0, 0, 25},
				"score": []byte{0, 0, 0, 11},
			}
			err = TransManager.Insert("users", row, localTrans)
			if err != nil {
				fmt.Printf("Error inserting row %d: %v\n", i, err)
			}
			// Don't forget to commit!
			TransManager.Abort(localTrans)
		}(i)
	}
	wg.Wait()

}
