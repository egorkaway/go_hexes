package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"

    "cloud.google.com/go/storage"
    "github.com/joho/godotenv"
)

const (
    exportDir = "export"
)

var (
    bucketName = "" // Bucket name will be fetched dynamically
    token      = "" // New token for authentication
)

func loadEnv() {
    if err := godotenv.Load(); err != nil {
        log.Printf("Error loading .env file: %v", err)
    }
}

func fetchDefaultBucket() (string, error) {
    resp, err := http.Get("http://127.0.0.1:1106/object-storage/default-bucket")
    if err != nil {
        log.Printf("Error fetching default bucket: %v", err)
        return "", err
    }
    defer resp.Body.Close()

    var result struct {
        BucketID string `json:"bucketId"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        log.Printf("Error decoding bucket response: %v", err)
        return "", err
    }

    if result.BucketID == "" {
        log.Printf("Fetched bucket ID is empty")
        return "", fmt.Errorf("Fetched bucket ID is empty")
    }

    log.Printf("Fetched bucket ID: %s", result.BucketID)
    return result.BucketID, nil
}

func fetchToken() (string, error) {
    resp, err := http.Post("http://127.0.0.1:1106/token", "application/json", nil)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return "", err
    }

    fmt.Printf("Token response: %s\n", string(body)) // Debugging: Print the raw response

    var result struct {
        AccessToken string `json:"access_token"`
    }
    err = json.Unmarshal(body, &result)
    if err != nil {
        return "", err
    }

    return result.AccessToken, nil
}

func serveFromGCS(w http.ResponseWriter, r *http.Request) {
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        http.Error(w, "Failed to create client", http.StatusInternalServerError)
        log.Printf("Error creating GCS client: %v", err)
        return
    }
    defer client.Close()

    bucketName, err := fetchDefaultBucket()
    if err != nil {
        http.Error(w, "Failed to bucket ID", http.StatusInternalServerError)
        log.Printf("Failed to fetch default bucket ID: %v", err)
        return
    }

    objectName := "h3_level_7.geojson" // The object name you want to fetch
    rc, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
    if err != nil {
        http.Error(w, "Failed to read object", http.StatusInternalServerError)
        log.Printf("Failed to read object: %v", err)
        return
    }
    defer rc.Close()

    w.Header().Set("Content-Type", "application/json")
    if _, err := io.Copy(w, rc); err != nil {
        http.Error(w, "Failed to copy data", http.StatusInternalServerError)
        log.Printf("Failed to copy data: %v", err)
        return
    }

    log.Printf("Successfully served %s from bucket %s", objectName, bucketName)
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
        log.Printf("Failed to fetch token, using default token: %v", err)
        token = "YOUR_DEFAULT_TOKEN"
    }
    fmt.Printf("Fetched token: %s\n", token)

    if err := os.MkdirAll(exportDir, os.ModePerm); err != nil {
        log.Fatal("Failed to create export directory:", err)
    }

    http.HandleFunc("/h3_level_7.geojson", serveFromGCS)

    // Serve the root as index.html
    fs := http.FileServer(http.Dir("http"))
    http.Handle("/", http.StripPrefix("/", fs))

    log.Println("Listening on :8080...")
    err = http.ListenAndServe(":8080", nil)
    if err != nil {
        log.Fatal(err)
    }
}