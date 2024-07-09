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
  if _, err := os.Stat("http/h3cells_weather_h4.geojson"); os.IsNotExist(err) {
    log.Fatalf("h3cells_weather_h4.geojson file does not exist: %v", err)
  }

  // Placeholder: Verify existence of h3cells_weather_h4.geojson
  log.Println("Verified existence of h3cells_weather_h4.geojson")
}

func main() {
  LoadEnvironmentVariables()

  go func() {
    for {
      generateGeoJSON()
      time.Sleep(3 * time.Hour) // Use time.Sleep for periodic task, if applicable
    }
  }()

  http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    http.ServeFile(w, r, "http/index.html")
  })

  fs := http.FileServer(http.Dir("http"))
  http.Handle("/static/", http.StripPrefix("/static", fs))

  log.Println("Serving on port 8080")
  if err := http.ListenAndServe(":8080", nil); err != nil {
    log.Fatal(err)
  }
}