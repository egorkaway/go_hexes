package main

import (
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
  h3 "github.com/uber/h3-go/v3"
)

const (
  outputJSONFile = "reports_h3_l3.json"
  reportsDir     = "http/reports"
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

func buildPostgresURL() string {
  user := os.Getenv("POSTGRES_USER")
  password := os.Getenv("POSTGRES_PASSWORD")
  host := os.Getenv("POSTGRES_HOST")
  port := os.Getenv("DB_PORT")
  dbname := os.Getenv("POSTGRES_DATABASE")

  return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require", user, password, host, port, dbname)
}

func connectDB() (*sql.DB, error) {
  postgresURL := buildPostgresURL()
  if postgresURL == "" {
    log.Fatal("Postgres URL could not be built from environment variables")
  }
  return sql.Open("postgres", postgresURL)
}

func fetchH3Level3Data(db *sql.DB) ([]H3Data, error) {
  rows, err := db.Query("SELECT h3_index, visits FROM h3_level_3")
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

func generateIntermediateJSON(h3Data []H3Data, level int) ([]H3Data, error) {
  h3IndexMap := make(map[string]int)

  for _, row := range h3Data {
    var parentIndex string

    switch level {
    case 1:
      parentIndex = h3.ToString(h3.ToParent(h3.FromString(row.H3Index), 1))
    case 2:
      parentIndex = h3.ToString(h3.ToParent(h3.FromString(row.H3Index), 2))
    case 3:
      parentIndex = row.H3Index
    }

    h3IndexMap[parentIndex] += row.Visits
  }

  var intermediateData []H3Data
  for h3Index, visits := range h3IndexMap {
    intermediateData = append(intermediateData, H3Data{H3Index: h3Index, Visits: visits})
  }

  return intermediateData, nil
}

func generateGeoJSON(h3Data []H3Data) (map[string]interface{}, error) {
  features := []map[string]interface{}{}
  weatherCache := make(map[string]map[string]interface{}) // Cache for weather data
  totalHexes := len(h3Data)
  currentHex := 0

  for _, row := range h3Data {
    geoCoord := h3.ToGeo(h3.FromString(row.H3Index))

    var weatherData map[string]interface{}
    var err error

    // Check if the weather data is already fetched
    if cachedData, found := weatherCache[row.H3Index]; found {
      weatherData = cachedData
      log.Printf("Using cached weather data for %s (progress: %d/%d)\n", row.H3Index, currentHex+1, totalHexes)
    } else {
      log.Printf("Fetching weather data for %s (progress: %d/%d)\n", row.H3Index, currentHex+1, totalHexes)
      weatherData, err = getWeatherData(geoCoord.Latitude, geoCoord.Longitude)
      if err != nil {
        log.Printf("Failed to fetch weather data for %s: %v\n", row.H3Index, err)
        continue
      }
      weatherCache[row.H3Index] = weatherData // Cache weather data
      log.Printf("Successfully fetched weather data for %s\n", row.H3Index)
    }

    feature := map[string]interface{}{
      "type": "Feature",
      "geometry": map[string]interface{}{
        "type":        "Polygon",
        "coordinates": h3ToGeoBoundary(row.H3Index),
      },
      "properties": map[string]interface{}{
        "h3cell":      row.H3Index,
        "temperature": weatherData["main"].(map[string]interface{})["temp"], // Extract temperature
        "visits":      row.Visits, // Include visit count
      },
    }
    features = append(features, feature)
    currentHex++
  }

  return map[string]interface{}{
    "type":     "FeatureCollection",
    "features": features,
  }, nil
}

func getWeatherData(lat, lon float64) (map[string]interface{}, error) {
  openWeatherMapAPIKey := os.Getenv("OPENWEATHERMAP_API_KEY")
  if openWeatherMapAPIKey == "" {
    log.Fatal("OPENWEATHERMAP_API_KEY not set in environment")
  }

  url := fmt.Sprintf("https://api.openweathermap.org/data/2.5/weather?lat=%f&lon=%f&appid=%s&units=metric", lat, lon, openWeatherMapAPIKey)
  response, err := http.Get(url)
  if err != nil {
    return nil, err
  }
  defer response.Body.Close()

  if response.StatusCode != http.StatusOK {
    bodyBytes, _ := ioutil.ReadAll(response.Body)
    return nil, fmt.Errorf("failed to fetch weather data: %s", string(bodyBytes))
  }

  var weatherData map[string]interface{}
  err = json.NewDecoder(response.Body).Decode(&weatherData)
  if err != nil {
    return nil, err
  }

  return weatherData, nil
}

func h3ToGeoBoundary(h3ID string) interface{} {
  geoBoundary := h3.ToGeoBoundary(h3.FromString(h3ID))
  coordinates := make([][]float64, len(geoBoundary))

  for i, coord := range geoBoundary {
    coordinates[i] = []float64{coord.Longitude, coord.Latitude}
  }

  // Close the polygon by repeating the first coordinate
  if len(coordinates) > 0 {
    coordinates = append(coordinates, coordinates[0])
  }

  return [][]interface{}{arraysToInterfaces(coordinates)}
}

func arraysToInterfaces(arrays [][]float64) []interface{} {
  interfaces := make([]interface{}, len(arrays))
  for i, array := range arrays {
    interfaces[i] = array
  }
  return interfaces
}

func main() {
  loadEnv()

  // Create the reports directory if it doesn't exist
  if err := os.MkdirAll(reportsDir, os.ModePerm); err != nil {
    log.Fatal("Failed to create reports directory:", err)
  }

  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  h3Data, err := fetchH3Level3Data(db)
  if err != nil {
    log.Fatal("Failed to fetch data:", err)
  }

  // Generate and save intermediate JSON files
  for _, level := range []int{3, 2, 1} {
    intermediateData, err := generateIntermediateJSON(h3Data, level)
    if err != nil {
      log.Fatal("Failed to generate intermediate JSON:", err)
    }
    filePath := filepath.Join(reportsDir, fmt.Sprintf("reports_h3_level_%d.json", level))
    err = saveToJSON(intermediateData, filePath)
    if err != nil {
      log.Fatal("Failed to save intermediate JSON:", err)
    }
  }

  // Generate final GeoJSON files
  for _, level := range []int{3, 2, 1} {
    filePath := filepath.Join(reportsDir, fmt.Sprintf("reports_h3_level_%d.json", level))
    data, err := ioutil.ReadFile(filePath)
    if err != nil {
      log.Fatal("Failed to read intermediate JSON:", err)
    }

    var h3DataLevel []H3Data
    err = json.Unmarshal(data, &h3DataLevel)
    if err != nil {
      log.Fatal("Failed to unmarshal JSON:", err)
    }

    geoJSON, err := generateGeoJSON(h3DataLevel)
    if err != nil {
      log.Fatal("Failed to generate GeoJSON:", err)
    }

    file, err := json.MarshalIndent(geoJSON, "", "  ")
    if err != nil {
      log.Fatal("Failed to marshal GeoJSON:", err)
    }

    err = ioutil.WriteFile(filepath.Join(reportsDir, fmt.Sprintf("reports_h3_level_%d.geojson", level)), file, 0644)
    if err != nil {
      log.Fatal("Failed to write GeoJSON file:", err)
    }
  }
}