package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"

  h3 "github.com/uber/h3-go/v3"
  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
)

func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

func connectDB(envVar string) (*sql.DB, error) {
  dbURL := os.Getenv(envVar)
  if dbURL == "" {
    log.Fatalf("%s not set in environment variables", envVar)
  }
  return sql.Open("postgres", dbURL)
}

func fetchSpanishDeviceLocations(db *sql.DB) ([][]interface{}, error) {
  query := "SELECT latitude, longitude FROM device_location WHERE language = 'es'"

  rows, err := db.Query(query)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  var data [][]interface{}
  for rows.Next() {
    var lat, lon float64
    if err := rows.Scan(&lat, &lon); err != nil {
      return nil, err
    }
    data = append(data, []interface{}{lat, lon})
  }

  return data, rows.Err()
}

func createSpanishL7Table(db *sql.DB) error {
  tableCreationQuery := `
    CREATE TABLE IF NOT EXISTS spanish_l4 (
      h3_index TEXT PRIMARY KEY,
      devices INT
    );
  `
  _, err := db.Exec(tableCreationQuery)
  return err
}

func calculateH3Index(data [][]interface{}, resolution int) map[string]int {
  h3Counts := make(map[string]int)
  for _, record := range data {
    lat := record[0].(float64)
    lon := record[1].(float64)
    h3Index := h3.ToString(h3.FromGeo(h3.GeoCoord{Latitude: lat, Longitude: lon}, resolution))
    h3Counts[h3Index]++
  }
  return h3Counts
}

func insertH3Counts(db *sql.DB, h3Counts map[string]int, table string) error {
  tx, err := db.Begin()
  if err != nil {
    return err
  }

  _, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", table))
  if err != nil {
    tx.Rollback()
    return err
  }

  query := fmt.Sprintf("INSERT INTO %s (h3_index, devices) VALUES ($1, $2)", table)
  stmt, err := tx.Prepare(query)
  if err != nil {
    tx.Rollback()
    return err
  }
  defer stmt.Close()

  for h3Index, count := range h3Counts {
    _, err = stmt.Exec(h3Index, count)
    if err != nil {
      tx.Rollback()
      return err
    }
  }

  return tx.Commit()
}

func main() {
  loadEnv()

  sourceDB, err := connectDB("GOOG_URL")
  if err != nil {
    log.Fatalf("Failed to connect to source database: %v", err)
  }
  defer sourceDB.Close()

  destDB, err := connectDB("SUPA_URL")
  if err != nil {
    log.Fatalf("Failed to connect to destination database: %v", err)
  }
  defer destDB.Close()

  data, err := fetchSpanishDeviceLocations(sourceDB)
  if err != nil {
    log.Fatalf("Failed to fetch data from device_location: %v", err)
  }

  err = createSpanishL7Table(destDB)
  if err != nil {
    log.Fatalf("Failed to create table spanish_l4: %v", err)
  }

  h3Counts := calculateH3Index(data, 4)

  err = insertH3Counts(destDB, h3Counts, "spanish_l4")
  if err != nil {
    log.Fatalf("Failed to insert H3 counts: %v", err)
  }

  log.Println("Successfully processed and inserted H3 level 4 counts for Spanish device locations.")
}