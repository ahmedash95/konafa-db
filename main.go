package main

import (
	"ahmedash95/konafa-db/konafadb"
	"fmt"
)

func main() {
	db, err := konafadb.New("database.db")

	if err != nil {
		panic(err)
	}

	defer db.Close()

	ok := db.Insert("users", map[string]interface{}{
		"name":  "John",
		"email": "john@example.com",
	})

	if !ok {
		panic("Could not insert user")
	}

	fmt.Printf("Users count: %d\n", db.Count("users"))
}
