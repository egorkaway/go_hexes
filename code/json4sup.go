package main

import (
  "database/sql"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "os"
  "path/filepath"
  "time"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
)

// Directory to store exported files
const exportDir = "output"

// H3Data struct represents the data structure to be exported
type H3Data struct {
  H3Index   string     `json:"h3_index"`
  Visits    int        `json:"visits"`
  LastVisit *time.Time `json:"last_visit,omitempty"`
}

// loadEnv loads environment variables from .env file
func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

// connectDB establishes a connection to the database using SUPA_URL environment variable
func connectDB() (*sql.DB, error) {
  supaURL := os.Getenv("SUPA_URL")
  if supaURL == "" {
    log.Fatal("SUPA_URL not set in environment variables")
  }
  return sql.Open("postgres", supaURL)
}

// tableExists checks if a table exists in the database
func tableExists(db *sql.DB, tableName string) (bool, error) {
  query := fmt.Sprintf("SELECT to_regclass('%s')", tableName)
  var table sql.NullString
  err := db.QueryRow(query).Scan(&table)
  if err != nil {
    return false, err
  }
  return table.Valid, nil
}

// fetchH3Data fetches H3 data from the specified table in the database
func fetchH3Data(db *sql.DB, tableName string, hasLastVisit bool) ([]H3Data, error) {
  var rows *sql.Rows
  var err error

  if hasLastVisit {
    rows, err = db.Query(fmt.Sprintf("SELECT h3_index, visits, last_visit FROM %s", tableName))
  } else {
    rows, err = db.Query(fmt.Sprintf("SELECT h3_index, visits FROM %s", tableName))
  }

  if err != nil {
    return nil, err
  }
  defer rows.Close()

  var data []H3Data
  for rows.Next() {
    var h3Index string
    var visits int
    var lastVisit sql.NullTime

    if hasLastVisit {
      err := rows.Scan(&h3Index, &visits, &lastVisit)
      if err != nil {
        return nil, err
      }

      var lastVisitPtr *time.Time
      if lastVisit.Valid {
        lastVisitPtr = &lastVisit.Time
      }

      data = append(data, H3Data{
        H3Index:   h3Index,
        Visits:    visits,
        LastVisit: lastVisitPtr,
      })
    } else {
      err := rows.Scan(&h3Index, &visits)
      if err != nil {
        return nil, err
      }

      data = append(data, H3Data{
        H3Index:   h3Index,
        Visits:    visits,
        LastVisit: nil,
      })
    }
  }
  return data, nil
}

// fetchAndSaveComplete7 fetches complete data from the cities_with_users table and saves it as complete_7.json
func fetchAndSaveComplete7(db *sql.DB) error {
  query := "SELECT visits, h3l7, last_visit FROM cities_with_users WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
  rows, err := db.Query(query)
  if err != nil {
    return err
  }
  defer rows.Close()

  var data []map[string]interface{}
  for rows.Next() {
    var (
      visits    sql.NullInt64
      h3l7      sql.NullString
      lastVisit sql.NullTime
    )
    err := rows.Scan(&visits, &h3l7, &lastVisit)
    if err != nil {
      return err
    }

    // Filter out rows where visits is 0 or NULL and last_visit is NULL
    if visits.Valid && visits.Int64 > 0 && lastVisit.Valid {
      data = append(data, map[string]interface{}{
        "visits":    visits.Int64,
        "h3l7":      h3l7.String,
        "last_visit": lastVisit.Time,
      })
    }
  }

  // Save the filtered data to complete_7.json
  filename := filepath.Join(exportDir, "complete_7.json")
  file, err := json.MarshalIndent(data, "", "  ")
  if err != nil {
    return err
  }

  return ioutil.WriteFile(filename, file, 0644)
}

// saveToJSON saves the given H3 data to a JSON file
func saveToJSON(data []H3Data, filename string) error {
  file, err := json.MarshalIndent(data, "", "  ")
  if err != nil {
    return err
  }
  return ioutil.WriteFile(filename, file, 0644)
}

// main function is the entry point of the program
func main() {
  loadEnv()

  if err := os.MkdirAll(exportDir, os.ModePerm); err != nil {
    log.Fatal("Failed to create output directory:", err)
  }

  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  levels := []struct {
    level       int
    tableName   string
    hasLastVisit bool
  }{
    {3, "h3_level_3", true},
    {4, "h3_level_4", true},
    {5, "h3_level_5", true},
    {6, "h3_level_6", true},
    {7, "h3_level_7", true},
  }

  for _, l := range levels {
    exists, err := tableExists(db, l.tableName)
    if err != nil {
      log.Fatalf("Failed to check if table %s exists: %v", l.tableName, err)
    }
    if !exists {
      log.Printf("Table %s does not exist, skipping...", l.tableName)
      continue
    }

    h3Data, err := fetchH3Data(db, l.tableName, l.hasLastVisit)
    if err != nil {
      log.Fatalf("Failed to fetch data for %s: %v", l.tableName, err)
    }

    filename := filepath.Join(exportDir, fmt.Sprintf("h3_level_%d.json", l.level))
    err = saveToJSON(h3Data, filename)
    if err != nil {
      log.Fatalf("Failed to save JSON file for %s: %v", l.tableName, err)
    }
  }

  // Fetch and save complete_7 data
  err = fetchAndSaveComplete7(db)
  if err != nil {
    log.Fatalf("Failed to fetch and save complete_7 data: %v", err)
  }

  log.Print("Successfully processed all levels and saved to output directory")
}