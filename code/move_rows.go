package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
)

// Function to load environment variables from .env file
func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

// Function to connect to the database using a connection URL from the environment variable
func connectDB(envVar string) (*sql.DB, error) {
  dbURL := os.Getenv(envVar)
  if dbURL == "" {
    log.Fatalf("%s not set in environment variables", envVar)
  }
  return sql.Open("postgres", dbURL)
}

func fetchDataFromSource(db *sql.DB) ([][]interface{}, error) {
  rows, err := db.Query("SELECT city, latitude, longitude, visits, h3l7, last_visit FROM cities_with_users;")
  if err != nil {
    return nil, fmt.Errorf("failed to fetch data from source: %w", err)
  }
  defer rows.Close()

  var data [][]interface{}
  for rows.Next() {
    var (
      city      string
      latitude  float64
      longitude float64
      visits    sql.NullInt64
      h3l7      sql.NullString
      lastVisit sql.NullTime
    )
    if err := rows.Scan(&city, &latitude, &longitude, &visits, &h3l7, &lastVisit); err != nil {
      return nil, fmt.Errorf("failed to scan row: %w", err)
    }
    row := []interface{}{city, latitude, longitude, visits, h3l7.String, lastVisit}
    data = append(data, row)
  }
  if err := rows.Err(); err != nil {
    return nil, fmt.Errorf("rows iteration error: %w", err)
  }

  return data, nil
}

func insertOnlyNewData(db *sql.DB, data [][]interface{}) error {
  tx, err := db.Begin()
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }
  defer tx.Rollback()

  stmtCheck, err := tx.Prepare("SELECT 1 FROM cities_with_users WHERE h3l7=$1")
  if err != nil {
    return fmt.Errorf("failed to prepare check statement: %w", err)
  }
  defer stmtCheck.Close()

  stmtInsert, err := tx.Prepare("INSERT INTO cities_with_users (city, latitude, longitude, visits, h3l7, last_visit) VALUES ($1, $2, $3, $4, $5, $6)")
  if err != nil {
    return fmt.Errorf("failed to prepare insert statement: %w", err)
  }
  defer stmtInsert.Close()

  for _, row := range data {
    h3l7, ok := row[4].(string)
    if !ok {
      return fmt.Errorf("h3l7 is not a string: %v", row[4])
    }

    var exists int
    err := stmtCheck.QueryRow(h3l7).Scan(&exists)
    if err != nil && err == sql.ErrNoRows {
      // No row with this h3l7 value, so we can insert
      if _, err := stmtInsert.Exec(row[0], row[1], row[2], row[3], row[4], row[5]); err != nil {
        return fmt.Errorf("failed to execute insert statement: %w", err)
      }
      log.Printf("Inserted row: city=%s, h3l7=%s, last_visit=%s\n", row[0], h3l7, row[5].(sql.NullTime).Time)
    } else if err != nil {
      return fmt.Errorf("failed to check for existing row: %w", err)
    }
  }

  if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
  }

  return nil
}

func main() {
  // Load environment variables from .env file
  loadEnv()

  sourceDBURL := os.Getenv("POSTGRES_URL")
  destDBURL := os.Getenv("SUPA_URL")

  if sourceDBURL == "" || destDBURL == "" {
    log.Fatal("Missing environment variables: POSTGRES_URL or SUPA_URL not set")
  }

  log.Println("Source DB URL:", sourceDBURL)
  log.Println("Destination DB URL:", destDBURL)

  // Connect to the source database
  sourceDB, err := connectDB("POSTGRES_URL")
  if err != nil {
    log.Fatalf("Failed to connect to source database: %v", err)
  }
  defer sourceDB.Close()

  // Fetch data from source database
  data, err := fetchDataFromSource(sourceDB)
  if err != nil {
    log.Fatalf("Failed to fetch data from source: %v", err)
  }
  log.Println("Data fetched from source successfully.")

  // Connect to the destination database
  destDB, err := connectDB("SUPA_URL")
  if err != nil {
    log.Fatalf("Failed to connect to destination database: %v", err)
  }
  defer destDB.Close()

  // Insert only new data into the destination database
  if err := insertOnlyNewData(destDB, data); err != nil {
    log.Fatalf("Failed to insert new data into destination: %v", err)
  }
  log.Println("Data inserted into destination successfully.")
}