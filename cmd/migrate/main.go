package main

import (
	"context"
	"fmt"
	"os"

	"github.com/DIMO-Network/model-garage/pkg/migrations"
)

func main() {
	ctx := context.Background()
	chConfig := getClickhouseENV()
	if err := migrations.RunGoose(ctx, os.Args[1:], chConfig); err != nil {
		fmt.Printf("failed to run goose: %v\n", err)
		pswSet := chConfig.Password != ""
		fmt.Printf("Clickhouse Host %s, Port %s, Database %s, User %s Password Set: %b\n", chConfig.Host, chConfig.Port, chConfig.DataBase, chConfig.User, pswSet)
		os.Exit(1)
	}
}

func getClickhouseENV() migrations.ClickhouseConfig {
	host, ok := os.LookupEnv("CLICKHOUSE_HOST")
	if !ok {
		host = "localhost"
	}
	port, ok := os.LookupEnv("CLICKHOUSE_PORT")
	if !ok {
		port = "9000"
	}

	dbName := os.Getenv("CLICKHOUSE_DATABASE")
	if dbName == "" {
		dbName = "default"
	}

	user, ok := os.LookupEnv("CLICKHOUSE_USER")
	if !ok {
		user = "default"
	}
	password, ok := os.LookupEnv("CLICKHOUSE_PASSWORD")
	if !ok {
		password = ""
	}
	return migrations.ClickhouseConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		DataBase: dbName,
	}
}
