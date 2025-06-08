package main

import (
	"fmt"
	"log"
	"password_manager/utils"
)

type Address struct {
	Id     int
	Street string
}

type User struct {
	Id      int
	Name    string
	Address Address
}

func main() {
	// fmt.Println("Hello world")
	// ctx := context.Background()
	// conn, err := pgx.Connect(ctx, "postgres://myuser:mypassword@localhost:5431/mydb")
	// if err != nil {
	// 	panic(err)
	// }
	// defer conn.Close(ctx)

	// users, err := utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("Users:")
	// fmt.Printf("%+v\n", users)

	// newUser := models.User{
	// 	Id:       "test2",
	// 	Username: "test",
	// 	Email:    "test",
	// }

	// err = utils.PGInsert(newUser, "users")
	// if err != nil {
	// 	panic(err)
	// }
	// time.Sleep(1 * time.Second)

	// users, err = utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("Users:")
	// fmt.Printf("%+v\n", users)

	// err = utils.PGDelete(newUser, "users")
	// if err != nil {
	// 	panic(err)
	// }

	// time.Sleep(1 * time.Second)

	// users, err = utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	// if err != nil {
	// 	panic(err)
	// }

	// fmt.Println("Users:")
	// fmt.Printf("%+v\n", users)
	user := User{
		Id:   1,
		Name: "Mike",
		Address: Address{
			Id:     10,
			Street: "123 Main",
		},
	}

	dependencyGraph, err := utils.GenerateSQLStructure(user)
	if err != nil {
		log.Fatal("Error generating dependency graph:", err)
	}

	fmt.Printf("Dependency Graph:\n")
	for _, entry := range dependencyGraph {
		fmt.Printf("Table: %s, UID: %s\n", entry.Table, entry.UID)
		for col, val := range entry.SQLData {
			fmt.Printf("  %s = %v\n", col, val)
		}
	}

}
