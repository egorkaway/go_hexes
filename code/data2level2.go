package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"
  "time"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
)

func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

func connectDB() (*sql.DB, error) {
  supaURL := os.Getenv("SUPA_URL")
  if supaURL == "" {
    log.Fatal("SUPA_URL not set in environment variables")
  }
  return sql.Open("postgres", supaURL)
}

func createTableIfNotExists(db *sql.DB, table string) error {
  tableCreationQuery := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
      h3_index TEXT PRIMARY KEY,
      visits INT,
      last_visit TIMESTAMP
    );
  `, table)
  _, err := db.Exec(tableCreationQuery)
  return err
}

func fetchCitiesWithUsers(db *sql.DB) ([][4]interface{}, error) {
  query := "SELECT latitude, longitude, visits, last_visit FROM cities_with_users WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND visits > 1"

  rows, err := db.Query(query)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  var data [][4]interface{}
  for rows.Next() {
    var lat, lon float64
    var visits sql.NullInt64
    var lastVisit sql.NullTime
    err := rows.Scan(&lat, &lon, &visits, &lastVisit)
    if err != nil {
      return nil, err
    }

    if visits.Valid && visits.Int64 > 1 && lastVisit.Valid {
      data = append(data, [4]interface{}{lat, lon, visits.Int64, lastVisit.Time})
    }
  }

  return data, nil
}

func aggregateHighestVisits(data [][4]interface{}) map[string][2]interface{} {
  aggregated := make(map[string][2]interface{})
  for _, record := range data {
    lat := record[0].(float64)
    lon := record[1].(float64)
    visits := record[2].(int64)
    lastVisit := record[3].(time.Time)
    h3Index := h3.ToString(h3.FromGeo(h3.GeoCoord{Latitude: lat, Longitude: lon}, 2))

    if existing, ok := aggregated[h3Index]; ok {
      if visits > existing[0].(int64) {
        aggregated[h3Index] = [2]interface{}{visits, lastVisit}
      }
    } else {
      aggregated[h3Index] = [2]interface{}{visits, lastVisit}
    }
  }
  return aggregated
}

func purgeAndInsertData(db *sql.DB, table string, aggregated map[string][2]interface{}) error {
  tx, err := db.Begin()
  if err != nil {
    return err
  }

  // Delete everything in the h3_level_2 table before inserting fresh data
  _, err = tx.Exec(fmt.Sprintf("DELETE FROM %s", table))
  if err != nil {
    tx.Rollback()
    return err
  }

  for h3Index, values := range aggregated {
    visits := values[0].(int64)
    lastVisit := values[1].(time.Time)

    _, err = tx.Exec(
      fmt.Sprintf("INSERT INTO %s (h3_index, visits, last_visit) VALUES ($1, $2, $3)", table),
      h3Index, visits, lastVisit,
    )
    if err != nil {
      tx.Rollback()
      return err
    }
  }

  err = tx.Commit()
  if err != nil {
    return err
  }

  return nil
}

func countRows(db *sql.DB, table string) (int, error) {
  var count int
  row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
  err := row.Scan(&count)
  return count, err
}

func main() {
  loadEnv()

  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  level := struct {
    table string
  }{
    table: "h3_level_2",
  }

  // Ensure the table exists before any operations
  err = createTableIfNotExists(db, level.table)
  if err != nil {
    log.Fatalf("Failed to create table %s if not exists: %v", level.table, err)
  }

  // Count rows in h3_level_2 before processing
  beforeCount, err := countRows(db, level.table)
  if err != nil {
    log.Fatalf("Failed to count rows in table %s before processing: %v", level.table, err)
  }
  log.Printf("Number of rows in table %s before processing: %d", level.table, beforeCount)

  rawData, err := fetchCitiesWithUsers(db)
  if err != nil {
    log.Fatalf("Failed to fetch data from cities_with_users: %v", err)
  }

  aggregatedData := aggregateHighestVisits(rawData)

  err = purgeAndInsertData(db, level.table, aggregatedData)
  if err != nil {
    log.Fatalf("Failed to insert aggregated data: %v", err)
  }

  // Count rows in h3_level_2 after processing
  afterCount, err := countRows(db, level.table)
  if err != nil {
    log.Fatalf("Failed to count rows in table %s after processing: %v", level.table, err)
  }
  log.Printf("Number of rows in table %s after processing: %d", level.table, afterCount)

  log.Println("Successfully aggregated and updated visits for level 2.")
}