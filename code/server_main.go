package main

import (
  "log"
  "net/http"
  "os"
  "time"

  "github.com/joho/godotenv"
)

// LoadEnvironmentVariables loads environment variables from a .env file if it exists
func LoadEnvironmentVariables() {
  if err := godotenv.Load(); err != nil {
    log.Println("No .env file found, using environment variables")
  }
}

func generateGeoJSON() {
  // Function to generate or verify the existence of geojson data
  if _, err := os.Stat("http/h3cells.geojson"); os.IsNotExist(err) {
    log.Fatalf("h3cells.geojson file does not exist: %v", err)
  }

  // Placeholder: Verify existence of h3cells.geojson
  log.Println("Verified existence of h3cells.geojson")
}

func main() {
  LoadEnvironmentVariables()

  go func() {
    for {
      generateGeoJSON()
      time.Sleep(3 * time.Hour) // Use time.Sleep for periodic task, if applicable
    }
  }()

  fs := http.FileServer(http.Dir("./http"))
  http.Handle("/", fs)

  // Serve the 'index_parents.html' on a specific path
  http.HandleFunc("/parents", func(w http.ResponseWriter, r *http.Request) {
    http.ServeFile(w, r, "http/index_parents.html")
  })

  log.Println("Serving on port 8080")
  if err := http.ListenAndServe(":8080", nil); err != nil {
    log.Fatal(err)
  }
}