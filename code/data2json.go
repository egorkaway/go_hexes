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
    "time"

    "github.com/joho/godotenv"
    _ "github.com/lib/pq"
)

// Directory to store exported files
const exportDir = "export"

// Variables to store bucket name and token
var (
    bucketName = "" // Bucket name will be fetched dynamically
    token      = "" // Token for authentication
)

// H3Data struct represents the data structure to be exported
type H3Data struct {
    H3Index   string     `json:"h3_index"`
    Visits    int        `json:"visits"`
    LastVisit *time.Time `json:"last_visit,omitempty"`
}

// BucketResponse represents the structure of the bucket ID response
type BucketResponse struct {
    BucketID string `json:"bucketId"`
}

// TokenResponse represents the structure of the token response
type TokenResponse struct {
    AccessToken string `json:"access_token"`
}

// loadEnv loads environment variables from .env file
func loadEnv() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
}

// fetchDefaultBucket fetches the default bucket ID from the object storage service
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

    var result BucketResponse
    err = json.Unmarshal(body, &result)
    if err != nil {
        return "", err
    }

    if result.BucketID == "" {
        return "", fmt.Errorf("fetched bucket ID is empty")
    }

    return result.BucketID, nil
}

// fetchToken fetches an authentication token from the token service
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

    var result TokenResponse
    err = json.Unmarshal(body, &result)
    if err != nil {
        return "", err
    }

    return result.AccessToken, nil
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

// uploadToObjectStorage uploads the given file to the object storage
func uploadToObjectStorage(filename, bucketName, token string) error {
    if bucketName == "" {
        return fmt.Errorf("bucket name is empty")
    }

    fileData, err := ioutil.ReadFile(filename)
    if err != nil {
        return err
    }

    url := fmt.Sprintf("https://storage.googleapis.com/upload/storage/v1/b/%s/o?uploadType=media&name=%s", bucketName, filepath.Base(filename))

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

// main function is the entry point of the program
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
        level       int
        tableName   string
        hasLastVisit bool
    }{
        {3, "h3_level_3", false},
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

        err = uploadToObjectStorage(filename, bucketName, token)
        if err != nil {
            log.Fatalf("Failed to upload JSON file to Object Storage for %s: %v", l.tableName, err)
        }
    }

    log.Print("Successfully processed all levels and uploaded to Object Storage")
}