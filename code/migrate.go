package main

import (
  "database/sql"
  "fmt"
  "log"

  _ "github.com/lib/pq"
)

// Source and destination database connection URLs
const (
  sourceDBURL = "postgres://default:d1zWD7hyUFEx@ep-broken-tree-05982655-pooler.eu-central-1.aws.neon.tech:5432/verceldb?sslmode=require&options=endpoint%3Dep-broken-tree-05982655"
  destDBURL   = "postgresql://postgres.ylbjmqqjqifpfqcwtrpn:dUspyj-gahzec-madty9@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
)

func fetchDataFromSource(db *sql.DB) ([][]interface{}, error) {
  rows, err := db.Query("SELECT city, latitude, longitude, visits, h3l7 FROM cities_with_users;")
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
    )
    if err := rows.Scan(&city, &latitude, &longitude, &visits, &h3l7); err != nil {
      return nil, fmt.Errorf("failed to scan row: %w", err)
    }
    row := []interface{}{city, latitude, longitude, nil, nil}
    if visits.Valid {
      row[3] = visits.Int64
    }
    if h3l7.Valid {
      row[4] = h3l7.String
    }
    data = append(data, row)
  }
  if err := rows.Err(); err != nil {
    return nil, fmt.Errorf("rows iteration error: %w", err)
  }

  return data, nil
}

func insertDataIntoDestination(db *sql.DB, data [][]interface{}) error {
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

  stmtInsert, err := tx.Prepare("INSERT INTO cities_with_users (city, latitude, longitude, visits, h3l7) VALUES ($1, $2, $3, $4, $5)")
  if err != nil {
    return fmt.Errorf("failed to prepare insert statement: %w", err)
  }
  defer stmtInsert.Close()

  for _, row := range data {
    var exists int
    if err := stmtCheck.QueryRow(row[4]).Scan(&exists); err != nil {
      if err == sql.ErrNoRows {
        // No row with this h3l7 value, so we can insert
        if _, err := stmtInsert.Exec(row...); err != nil {
          return fmt.Errorf("failed to execute insert statement: %w", err)
        }
      } else {
        // Unexpected error
        return fmt.Errorf("failed to check for existing row: %w", err)
      }
    }
  }

  if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
  }

  return nil
}

func main() {
  // Connect to the source database
  sourceDB, err := sql.Open("postgres", sourceDBURL)
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
  destDB, err := sql.Open("postgres", destDBURL)
  if err != nil {
    log.Fatalf("Failed to connect to destination database: %v", err)
  }
  defer destDB.Close()

  // Insert data into destination database
  if err := insertDataIntoDestination(destDB, data); err != nil {
    log.Fatalf("Failed to insert data into destination: %v", err)
  }
  log.Println("Data inserted into destination successfully.")
}