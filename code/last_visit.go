package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"
  "time"

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

func insertOrUpdateData(db *sql.DB, data [][]interface{}) error {
  tx, err := db.Begin()
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }
  defer tx.Rollback()

  stmtCheck, err := tx.Prepare("SELECT last_visit FROM cities_with_users WHERE h3l7=$1")
  if err != nil {
    return fmt.Errorf("failed to prepare check statement: %w", err)
  }
  defer stmtCheck.Close()

  stmtInsert, err := tx.Prepare("INSERT INTO cities_with_users (city, latitude, longitude, visits, h3l7, last_visit) VALUES ($1, $2, $3, $4, $5, $6)")
  if err != nil {
    return fmt.Errorf("failed to prepare insert statement: %w", err)
  }
  defer stmtInsert.Close()

  stmtUpdate, err := tx.Prepare("UPDATE cities_with_users SET latitude=$1, longitude=$2, visits=$3, last_visit=$4 WHERE h3l7=$5")
  if err != nil {
    return fmt.Errorf("failed to prepare update statement: %w", err)
  }
  defer stmtUpdate.Close()

  updateCounter := 0
  maxUpdates := 100

  // Define cutoff date: August 2024
  cutoffDate := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)

  for _, row := range data {
    // Check if `last_visit` is after August 2024
    srcLastVisit := row[5].(sql.NullTime).Time
    if srcLastVisit.Before(cutoffDate) {
      continue
    }

    if updateCounter >= maxUpdates {
      log.Printf("Reached the maximum number of updates: %d\n", maxUpdates)
      break
    }

    h3l7, ok := row[4].(string)
    if !ok {
      return fmt.Errorf("h3l7 is not a string: %v", row[4])
    }

    var destLastVisit sql.NullTime

    err := stmtCheck.QueryRow(h3l7).Scan(&destLastVisit)
    if err != nil {
      if err == sql.ErrNoRows {
        // No row with this h3l7 value, so we can insert
        if _, err := stmtInsert.Exec(row[0], row[1], row[2], row[3], row[4], row[5]); err != nil {
          return fmt.Errorf("failed to execute insert statement: %w", err)
        }
        log.Printf("Inserted row: city=%s, h3l7=%s, last_visit=%s\n", row[0], h3l7, srcLastVisit)
        updateCounter++
      } else {
        // Unexpected error
        return fmt.Errorf("failed to check for existing row: %w", err)
      }
    } else {
      // Row exists, compare last_visit
      if !destLastVisit.Valid || srcLastVisit.After(destLastVisit.Time) {
        // Source last_visit is newer, update latitude, longitude, visits, and last_visit
        if _, err := stmtUpdate.Exec(row[1], row[2], row[3], srcLastVisit, h3l7); err != nil {
          return fmt.Errorf("failed to execute update statement: %w", err)
        }
        log.Printf("Updated city=%s, h3l7=%s, last_visit=%s\n", row[0], h3l7, srcLastVisit)
        updateCounter++
      }
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

  // Insert or update data into destination database
  if err := insertOrUpdateData(destDB, data); err != nil {
    log.Fatalf("Failed to insert or update data into destination: %v", err)
  }
  log.Println("Data inserted or updated in destination successfully.")
}