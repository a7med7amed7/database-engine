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
	for i := 1; i < 5; i++ {
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
			TransManager.Commit(localTrans)
		}(i)
	}
	wg.Wait()
	selectTrans, err := TransManager.Begin()
	if err != nil {
		fmt.Println("Error beginning transaction:", err)
		return
	}
	data, err := TransManager.Select("users", IntToBytes(3), IntToBytes(8), selectTrans)
	TransManager.Commit(selectTrans)
	if err != nil {
		fmt.Println("Error selecting data")
		return
	}
	fmt.Println("Selected data ", data)

	var wg3 sync.WaitGroup
	updateTrans, err := TransManager.Begin()
	if err != nil {
		fmt.Println("Error beginning transaction:", err)
		return
	}
	updatedRow := map[string][]byte{
		"id":    IntToBytes(3),
		"name":  []byte(RandomString(6)),
		"age":   []byte{0, 0, 0, 10},
		"score": []byte{0, 0, 0, 6},
	}
	wg3.Add(1)
	go func() {
		defer wg3.Done()
		err := TransManager.Update("users", updatedRow, updateTrans)
		if err != nil {
			fmt.Println("Error updating row:", err)
			return
		}
		TransManager.Commit(updateTrans)
	}()
	wg3.Wait()
	selectTrans2, err := TransManager.Begin()
	if err != nil {
		fmt.Println("Error beginning transaction:", err)
		return
	}
	data2, err := TransManager.Select("users", IntToBytes(3), IntToBytes(8), selectTrans2)
	TransManager.Commit(selectTrans2)
	if err != nil {
		fmt.Println("Error selecting data")
		return
	}
	fmt.Println("Selected data ", data2)
}
