package main

import (
    "bytes"
    "database/sql"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "path/filepath"

    "github.com/joho/godotenv"
    _ "github.com/lib/pq"
)

const (
    exportDir = "export"
)

var (
    bucketName = "" // Bucket name will be fetched dynamically
    token      = "" // New token for authentication
)

type H3Data struct {
    H3Index string `json:"h3_index"`
    Visits  int    `json:"visits"`
}

type BucketResponse struct {
    BucketID string `json:"bucketId"` // Adjust the field name to match JSON key
}

type TokenResponse struct {
    AccessToken string `json:"access_token"`
}

func loadEnv() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
}

func fetchDefaultBucket() (string, error) {
    resp, err := http.Get("http://127.0.0.1:1106/object-storage/default-bucket")
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return "", err
    }

    fmt.Printf("Bucket response: %s\n", string(body)) // Debugging: Print the raw response

    var result BucketResponse
    err = json.Unmarshal(body, &result)
    if err != nil {
        return "", err
    }

    // Catch unexpected empty bucket ID
    if result.BucketID == "" {
        return "", fmt.Errorf("fetched bucket ID is empty")
    }

    return result.BucketID, nil
}

func fetchToken() (string, error) {
    resp, err := http.Post("http://127.0.0.1:1106/token", "application/json", nil)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return "", err
    }

    fmt.Printf("Token response: %s\n", string(body)) // Debugging: Print the raw response

    var result TokenResponse
    err = json.Unmarshal(body, &result)
    if err != nil {
        return "", err
    }

    return result.AccessToken, nil
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

func uploadToObjectStorage(filename, bucketName, token string) error {
    if bucketName == "" {
        return fmt.Errorf("bucket name is empty")
    }

    fileData, err := ioutil.ReadFile(filename)
    if err != nil {
        return err
    }

    url := fmt.Sprintf("https://storage.googleapis.com/upload/storage/v1/b/%s/o?uploadType=media&name=%s", bucketName, filepath.Base(filename))
    fmt.Printf("Uploading to URL: %s\n", url) // Debugging line to confirm URL

    req, err := http.NewRequest("POST", url, bytes.NewReader(fileData))
    if err != nil {
        return err
    }
    req.Header.Set("Authorization", "Bearer "+token)
    req.Header.Set("Content-Type", "application/octet-stream")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        bodyBytes, _ := ioutil.ReadAll(resp.Body)
        return fmt.Errorf("failed to upload file: %s", string(bodyBytes))
    }

    return nil
}

func main() {
    loadEnv()

    var err error
    bucketName, err = fetchDefaultBucket()
    if err != nil {
        log.Fatal("Failed to fetch default bucket ID:", err)
    }
    fmt.Printf("Fetched bucket ID: %s\n", bucketName)

    if bucketName == "" {
        log.Fatal("Bucket name is empty")
    }

    token, err = fetchToken()
    if err != nil {
        log.Fatal("Failed to fetch token:", err)
    }
    fmt.Printf("Fetched token: %s\n", token)

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
        {6, "h3_level_6"},
        {7, "h3_level_7"},
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

        err = uploadToObjectStorage(filename, bucketName, token)
        if err != nil {
            log.Fatalf("Failed to upload JSON file to Object Storage for %s: %v", l.tableName, err)
        }
    }

    log.Print("Successfully processed all levels and uploaded to Object Storage")
}