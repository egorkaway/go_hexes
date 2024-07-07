package main

import (
  "database/sql"
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "os"
  "strconv"
  "sync"
  "time"

  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
  "github.com/joho/godotenv"
)

// LoadEnvironmentVariables loads environment variables from a .env file if it exists
func LoadEnvironmentVariables() {
  if err := godotenv.Load(); err != nil {
    log.Println("No .env file found, using environment variables")
  }
}

var (
  h3CellsMap = make(map[string]bool)
  mu         sync.Mutex
)

func generateGeoJSON() {
  // Function to establish connection
  host := os.Getenv("DB_HOST")
  port := os.Getenv("DB_PORT")
  user := os.Getenv("DB_USER")
  password := os.Getenv("DB_PASSWORD")
  dbname := os.Getenv("DB_NAME")
  endpointID := os.Getenv("ENDPOINT_ID")

  portNum, err := strconv.Atoi(port)
  if err != nil {
    log.Fatalf("Invalid port number: %v", err)
  }

  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require options=endpoint=%s",
    host, portNum, user, password, dbname, endpointID)

  var db *sql.DB
  // Retry loop for the connection
  for attempts := 0; attempts < 5; attempts++ {
    db, err = sql.Open("postgres", psqlInfo)
    if err == nil {
      err = db.Ping()
      if err == nil {
        break
      }
    }
    log.Printf("Failed to connect to database: %v. Retrying in 10 seconds...\n", err)
    time.Sleep(10 * time.Second)
  }
  if err != nil {
    log.Fatalf("Failed to connect to database after multiple attempts: %v", err)
  }
  defer db.Close()

  fmt.Println("Successfully connected to the database")

  rows, err := db.Query("SELECT h3cell, visits FROM cities_with_users WHERE h3cell IS NOT NULL")
  if err != nil {
    log.Fatal(err)
  }
  defer rows.Close()

  type CellData struct {
    h3cell string
    visits sql.NullInt32
  }

  var h3cells []CellData
  for rows.Next() {
    var cellData CellData
    err = rows.Scan(&cellData.h3cell, &cellData.visits)
    if err != nil {
      log.Fatal(err)
    }
    h3cells = append(h3cells, cellData)
  }

  if err := rows.Err(); err != nil {
    log.Fatal(err)
  }

  type GeoJSONFeature struct {
    Type       string                 `json:"type"`
    Geometry   map[string]interface{} `json:"geometry"`
    Properties map[string]interface{} `json:"properties"`
  }

  var features []GeoJSONFeature

  mu.Lock()
  defer mu.Unlock()

  for _, cellData := range h3cells {
    isNew := false
    if _, exists := h3CellsMap[cellData.h3cell]; !exists {
      isNew = true
      h3CellsMap[cellData.h3cell] = true
    }

    cellIndex := h3.FromString(cellData.h3cell)
    boundary := h3.ToGeoBoundary(cellIndex)

    coordinates := make([][]float64, len(boundary))
    for j, coord := range boundary {
      coordinates[j] = []float64{coord.Longitude, coord.Latitude}
    }
    coordinates = append(coordinates, coordinates[0])

    coordsInterface := make([]interface{}, len(coordinates))
    for j, c := range coordinates {
      coordsInterface[j] = c
    }

    var fillColor string
    if cellData.visits.Valid && cellData.visits.Int32 > 30 {
      fillColor = "#800080" // Purple for cells with more than 30 visits
    } else if isNew {
      fillColor = "#0000ff" // Blue for new cells
    } else {
      fillColor = "#ff7800" // Orange for existing cells
    }

    features = append(features, GeoJSONFeature{
      Type: "Feature",
      Geometry: map[string]interface{}{
        "type":        "Polygon",
        "coordinates": []interface{}{coordsInterface},
      },
      Properties: map[string]interface{}{
        "h3cell":    cellData.h3cell,
        "fillColor": fillColor,
      },
    })
  }

  geoJSON := map[string]interface{}{
    "type":     "FeatureCollection",
    "features": features,
  }

  file, err := os.Create("http/h3cells.geojson")
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()

  encoder := json.NewEncoder(file)
  if err := encoder.Encode(geoJSON); err != nil {
    log.Fatal(err)
  }

  fmt.Println("GeoJSON file created: h3cells.geojson")
}

func main() {
  LoadEnvironmentVariables()

  go func() {
    for {
      generateGeoJSON()
      time.Sleep(3 * time.Hour)
    }
  }()

  fs := http.FileServer(http.Dir("./http"))
  http.Handle("/", fs)

  log.Println("Serving on port 8080")
  if err := http.ListenAndServe(":8080", nil); err != nil {
    log.Fatal(err)
  }
}