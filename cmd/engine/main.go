package main

import (
	model "db-engine-v2/internal/model"
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
	usersTable, err := db.GetTable("users")
	if err != nil {
		fmt.Println("error getting a table")
		return
	}
	// err = usersTable.AddNonClusteredIndex("age", db.BufferPool, db.LockManager, 4)
	// if err != nil {
	// 	fmt.Println("error adding a non-clustered index")
	// 	return
	// }
	Trans, err := db.TransactionManager.Begin()
	if err != nil {
		fmt.Println("Error beginning transaction:", err)
		return
	}
	// err = usersTable.AddNonClusteredIndex("name", db.BufferPool, db.LockManager, 4)
	// if err != nil {
	// 	fmt.Println(err)
	// }
	// row := map[string][]byte{
	// 	"id":    []byte{0, 0, 0, 1}, // Int 1
	// 	"name":  []byte("Ahmed"),
	// 	"age":   []byte{0, 0, 0, 25}, // Int 25
	// 	"score": []byte{0, 0, 0, 11}, // Int 11
	// }
	// queryInsert := model.NewQuery(model.QueryTypeInsert, "", nil, nil, row, Trans, usersTable)
	// _, err = queryInsert.Execute()
	// if err != nil {
	// 	fmt.Print("An error occured while inserting", err)
	// 	return
	// }

	// row = map[string][]byte{
	// 	"id":    []byte{0, 0, 0, 2}, // Int 2
	// 	"name":  []byte("Belal"),
	// 	"age":   []byte{0, 0, 0, 25}, // Int 25
	// 	"score": []byte{0, 0, 0, 11}, // Int 11
	// }
	// queryInsert = model.NewQuery(model.QueryTypeInsert, "", nil, nil, row, Trans, usersTable)
	// _, err = queryInsert.Execute()
	// if err != nil {
	// 	fmt.Print(err)
	// 	return
	// }
	// updateRow := map[string][]byte{
	// 	"name": []byte("Baraa"),
	// }
	// queryUpdate := model.NewQuery(model.QueryTypeUpdate, "id", []byte{0, 0, 0, 1}, []byte{0, 0, 0, 2}, updateRow, Trans, usersTable)
	// _, err = queryUpdate.Execute()
	// if err != nil {
	// 	fmt.Print("x", err)
	// 	return
	// }
	// querySelect := model.NewQuery(model.QueryTypeSelect, "id", []byte{0, 0, 0, 1}, []byte{0, 0, 0, 2}, nil, Trans, usersTable)
	// data, err := querySelect.Execute()
	// if err != nil {
	// 	fmt.Print(err)
	// 	return
	// }
	// for _, item := range data {
	// 	fmt.Println("Name found", string(item["name"]))
	// }
	// queryDelete := model.NewQuery(model.QueryTypeDelete, "id", []byte{0, 0, 0, 1}, []byte{0, 0, 0, 1}, nil, Trans, usersTable)
	// _, err = queryDelete.Execute()
	// if err != nil {
	// 	fmt.Print(err)
	// 	return
	// }
	// querySelect = model.NewQuery(model.QueryTypeSelect, "name", []byte("A"), []byte("C"), nil, Trans, usersTable)
	// data, err = querySelect.Execute()
	// if err != nil {
	// 	fmt.Print("x", err)
	// 	return
	// }
	// for _, item := range data {
	// 	fmt.Println("Name found", string(item["name"]))
	// }
	var wg sync.WaitGroup
	for i := 1; i < 20; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			// Create a new transaction for each goroutine
			localTrans, err := db.TransactionManager.Begin()
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
			queryInsert := model.NewQuery(model.QueryTypeInsert, "", nil, nil, row, localTrans, usersTable)
			_, err = queryInsert.Execute()
			if err != nil {
				fmt.Printf("Error inserting row %d: %v\n", i, err)
			}
			// Don't forget to commit!
			db.TransactionManager.Commit(localTrans)
		}(i)
	}
	wg.Wait()
	usersTable.ClusteredIndex.PrintTree(Trans)
	querySelect := model.NewQuery(model.QueryTypeSelect, "id", IntToBytes(1), IntToBytes(30), nil, Trans, usersTable)
	data, err := querySelect.Execute()
	if err != nil {
		fmt.Print(err)
		return
	}
	fmt.Println(data)
	for i, item := range data {
		fmt.Println("Name found ", i+1, string(item["name"]))
	}

}
