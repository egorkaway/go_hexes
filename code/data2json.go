package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "path/filepath"

    "github.com/joho/godotenv"
    _ "github.com/lib/pq"
)

const (
    exportDir = "export"
)

type H3Data struct {
    H3Index string `json:"h3_index"`
    Visits  int    `json:"visits"`
}

func loadEnv() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
}

func connectDB() (*sql.DB, error) {
    postgresURL := os.Getenv("POSTGRES_URL")
    if postgresURL == "" {
        log.Fatal("POSTGRES_URL not set in environment variables")
    }
    return sql.Open("postgres", postgresURL)
}

func fetchH3Data(db *sql.DB, tableName string) ([]H3Data, error) {
    rows, err := db.Query(fmt.Sprintf("SELECT h3_index, visits FROM %s", tableName))
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var data []H3Data
    for rows.Next() {
        var h3Index string
        var visits int
        err := rows.Scan(&h3Index, &visits)
        if err != nil {
            return nil, err
        }
        data = append(data, H3Data{H3Index: h3Index, Visits: visits})
    }
    return data, nil
}

func saveToJSON(data []H3Data, filename string) error {
    file, err := json.MarshalIndent(data, "", "  ")
    if err != nil {
        return err
    }
    return ioutil.WriteFile(filename, file, 0644)
}

func main() {
    loadEnv()

    // Create the export directory if it doesn't exist
    if err := os.MkdirAll(exportDir, os.ModePerm); err != nil {
        log.Fatal("Failed to create export directory:", err)
    }

    db, err := connectDB()
    if err != nil {
        log.Fatal("Failed to connect to database:", err)
    }
    defer db.Close()

    levels := []struct {
        level    int
        tableName string
    }{
        {3, "h3_level_3"},
        {4, "h3_level_4"},
        {5, "h3_level_5"},
    }

    for _, l := range levels {
        h3Data, err := fetchH3Data(db, l.tableName)
        if err != nil {
            log.Fatalf("Failed to fetch data for %s: %v", l.tableName, err)
        }

        filename := filepath.Join(exportDir, fmt.Sprintf("h3_level_%d.json", l.level))
        err = saveToJSON(h3Data, filename)
        if err != nil {
            log.Fatalf("Failed to save JSON file for %s: %v", l.tableName, err)
        }
    }
    log.Print("Successfully processed all levels and saved to /export directory")
}