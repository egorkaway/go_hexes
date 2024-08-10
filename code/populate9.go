package main

import (
  "database/sql"
  "fmt"
  "log"
  "math/rand"
  "time"

  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
)

// Database connection URL
const dbURL = "sup"

func connectDB() (*sql.DB, error) {
  return sql.Open("postgres", dbURL)
}

func insertH3Level9Data(db *sql.DB) error {
  rows, err := db.Query(`
    SELECT h3l7, visits
    FROM cities_with_users
    WHERE visits > 3 AND h3l7 IS NOT NULL
  `)
  if err != nil {
    return fmt.Errorf("failed to fetch data from database: %w", err)
  }
  defer rows.Close()

  tx, err := db.Begin()
  if err != nil {
    return fmt.Errorf("failed to begin transaction: %w", err)
  }

  var h3l7 string
  var visits int

  for rows.Next() {
    err := rows.Scan(&h3l7, &visits)
    if err != nil {
      tx.Rollback()
      return fmt.Errorf("failed to scan row: %w", err)
    }

    // Convert h3l7 to H3Index
    h3Index := h3.FromString(h3l7)
    if h3Index == 0 {
      tx.Rollback()
      return fmt.Errorf("failed to convert h3l7 to H3Index: %s", h3l7)
    }

    // Get all children of level 9
    children := h3.ToChildren(h3Index, 9)

    // Select a random child
    rand.Seed(time.Now().UnixNano())
    randomChild := children[rand.Intn(len(children))]

    // Convert H3Index back to string
    h3l9 := h3.ToString(randomChild)

    // Insert the new h3l9 value along with visits into h3_level_9 table
    _, err = tx.Exec(`
      INSERT INTO h3_level_9 (h3_index, visits)
      VALUES ($1, $2)
      ON CONFLICT (h3_index) DO UPDATE SET visits = EXCLUDED.visits
    `, h3l9, visits)
    if err != nil {
      tx.Rollback()
      return fmt.Errorf("failed to insert into h3_level_9 table: %w", err)
    }
  }

  err = tx.Commit()
  if err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
  }

  return nil
}

func main() {
  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  err = insertH3Level9Data(db)
  if err != nil {
    log.Fatal("Failed to insert H3 level 9 data:", err)
  }

  log.Println("Successfully inserted H3 level 9 data for all relevant rows.")
}