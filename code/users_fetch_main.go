package main

import (
  "database/sql"
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "os"
  "strconv"
  "sync"
  "time"

  _ "github.com/lib/pq"
  "github.com/joho/godotenv"
)

// LoadEnvironmentVariables loads environment variables from a .env file if it exists
func LoadEnvironmentVariables() {
  if err := godotenv.Load(); err != nil {
    log.Println("No .env file found, using environment variables")
  }
}

var (
  h3CellsMap = make(map[string]bool)
  mu         sync.Mutex
)

func generateGeoJSON() {
  host := os.Getenv("DB_HOST")
  port := os.Getenv("DB_PORT")
  user := os.Getenv("DB_USER")
  password := os.Getenv("DB_PASSWORD")
  dbname := os.Getenv("DB_NAME")
  endpointID := os.Getenv("ENDPOINT_ID")

  portNum, err := strconv.Atoi(port)
  if err != nil {
    log.Fatalf("Invalid port number: %v", err)
  }

  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require options=endpoint=%s",
    host, portNum, user, password, dbname, endpointID)

  var db *sql.DB
  for attempts := 0; attempts < 5; attempts++ {
    db, err = sql.Open("postgres", psqlInfo)
    if err == nil {
      err = db.Ping()
      if err == nil {
        break
      }
    }
    log.Printf("Failed to connect to database: %v. Retrying in 10 seconds...\n", err)
    time.Sleep(10 * time.Second)
  }
  if err != nil {
    log.Fatalf("Failed to connect to database after multiple attempts: %v", err)
  }
  defer db.Close()

  fmt.Println("Successfully connected to the database")

  rows, err := db.Query("SELECT h3cell FROM cities_with_users WHERE h3cell IS NOT NULL")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()

  var h3cells []string
  for rows.Next() {
    var h3cell string
    err = rows.Scan(&h3cell)
    if err != nil {
      log.Fatal(err)
    }
    h3cells = append(h3cells, h3cell)
  }

  if err := rows.Err(); err != nil {
    log.Fatal(err)
  }

  h3CellsJSON := map[string][]string{
    "h3cells": h3cells,
  }

  file, err := os.Create("http/h3cells_replit.json")
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()

  encoder := json.NewEncoder(file)
  if err := encoder.Encode(h3CellsJSON); err != nil {
    log.Fatal(err)
  }

  fmt.Println("JSON file created: h3cells.json")
}

func main() {
  LoadEnvironmentVariables()

  go func() {
    for {
      generateGeoJSON()
      time.Sleep(3 * time.Hour)
    }
  }()

  fs := http.FileServer(http.Dir("./http"))
  http.Handle("/", fs)

  log.Println("Serving on port 8080")
  if err := http.ListenAndServe(":8080", nil); err != nil {
    log.Fatal(err)
  }
}