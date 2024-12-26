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
const exportDir = "export"

// H3Data struct represents the data structure to be exported
type H3Data struct {
    H3Index   string     `json:"h3_index"`
    Visits    int        `json:"total"`
    LastVisit *time.Time `json:"last_visit,omitempty"`
}

// loadEnv loads environment variables from .env file
func loadEnv() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
}

// connectDB establishes a connection to the database using POSTGRES_URL environment variable
func connectDB() (*sql.DB, error) {
    postgresURL := os.Getenv("POSTGRES_URL")
    if postgresURL == "" {
        log.Fatal("POSTGRES_URL not set in environment variables")
    }
    return sql.Open("postgres", postgresURL)
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
        log.Fatal("Failed to create export directory:", err)
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

    log.Print("Successfully processed all levels and saved to export directory")
}