package main

import (
	"context"
	"fmt"
	"password_manager/models"
	"password_manager/utils"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	fmt.Println("Hello world")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://myuser:mypassword@localhost:5431/mydb")
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	users, err := utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	if err != nil {
		panic(err)
	}

	fmt.Println("Users:")
	fmt.Printf("%+v\n", users)

	newUser := models.User{
		Id:       "test2",
		Username: "test",
		Email:    "test",
	}

	err = utils.PGInsert(newUser, "users")
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	users, err = utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	if err != nil {
		panic(err)
	}

	fmt.Println("Users:")
	fmt.Printf("%+v\n", users)

	err = utils.PGDelete(newUser, "users")
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)

	users, err = utils.PGQuery[models.User](ctx, conn, "SELECT * FROM users")
	if err != nil {
		panic(err)
	}

	fmt.Println("Users:")
	fmt.Printf("%+v\n", users)

}
