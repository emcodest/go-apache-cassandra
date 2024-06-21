package main

import (
    "fmt"
    "log"

    "github.com/gocql/gocql"
)

func main() {
    // Connect to the Cassandra cluster
    cluster := gocql.NewCluster("127.0.0.1") // Replace with your Cassandra node IP
    cluster.Keyspace = "example"             // Ensure this matches the created keyspace
    cluster.Consistency = gocql.Quorum

    session, err := cluster.CreateSession()
    if err != nil {
        log.Fatalf("Unable to connect to Cassandra: %v", err)
    }
    defer session.Close()

    // Create a table
    if err := session.Query(`CREATE TABLE IF NOT EXISTS users (id UUID PRIMARY KEY, name TEXT, age INT)`).Exec(); err != nil {
        log.Fatalf("Failed to create table: %v", err)
    }

    // Insert data
    userId := gocql.TimeUUID()
    if err := session.Query(`INSERT INTO users (id, name, age) VALUES (?, ?, ?)`, userId, "John Doe", 30).Exec(); err != nil {
        log.Fatalf("Failed to insert data: %v", err)
    }

    // Retrieve data
    var id gocql.UUID
    var name string
    var age int
    if err := session.Query(`SELECT id, name, age FROM users WHERE id = ?`, userId).Scan(&id, &name, &age); err != nil {
        log.Fatalf("Failed to retrieve data: %v", err)
    }

    fmt.Printf("Retrieved user: ID=%s, Name=%s, Age=%d\n", id, name, age)
}
