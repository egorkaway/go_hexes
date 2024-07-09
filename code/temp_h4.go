package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "os"

  h3 "github.com/uber/h3-go/v3"
  "github.com/joho/godotenv"
)

const openWeatherMapAPIKey = "e7e06f3f2654e34e138f3d09ea001917"

// LoadEnvironmentVariables loads environment variables from a .env file if it exists
func LoadEnvironmentVariables() {
  if err := godotenv.Load(); err != nil {
    log.Println("No .env file found, using environment variables")
  }
}

type GeoJSONFeature struct {
  Type       string                 `json:"type"`
  Geometry   map[string]interface{} `json:"geometry"`
  Properties map[string]interface{} `json:"properties"`
}

func fetchTemperature(lat, lon float64) (float64, error) {
  url := fmt.Sprintf("https://api.openweathermap.org/data/2.5/weather?lat=%f&lon=%f&appid=%s&units=metric", lat, lon, openWeatherMapAPIKey)
  resp, err := http.Get(url)
  if err != nil {
    return 0, err
  }
  defer resp.Body.Close()

  var result map[string]interface{}
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return 0, err
  }
  if err := json.Unmarshal(body, &result); err != nil {
    return 0, err
  }

  main, ok := result["main"].(map[string]interface{})
  if !ok {
    return 0, fmt.Errorf("invalid response format")
  }

  temp, ok := main["temp"].(float64)
  if !ok {
    return 0, fmt.Errorf("invalid temperature data")
  }

  return temp, nil
}

func fetchWeatherDataForH3Cells(inputFile string, outputFile string) {
  // Read the h3cells.json file
  data, err := ioutil.ReadFile(inputFile)
  if err != nil {
    log.Fatalf("Failed to read input JSON file: %v", err)
  }

  var h3cells map[string][]string
  if err := json.Unmarshal(data, &h3cells); err != nil {
    log.Fatalf("Failed to parse input JSON file: %v", err)
  }

  features := make([]GeoJSONFeature, 0, len(h3cells["h3cells"]))

  for i, h3cell := range h3cells["h3cells"] {
    log.Printf("Fetching weather data for cell %d/%d: %s", i+1, len(h3cells["h3cells"]), h3cell)

    // Compute the cell index and get its boundary
    cellIndex := h3.FromString(h3cell)
    cellBoundary := h3.ToGeoBoundary(cellIndex)

    // Get the center of the cell to fetch temperature data
    cellCenter := h3.ToGeo(cellIndex)

    // Fetch temperature data
    temp, err := fetchTemperature(cellCenter.Latitude, cellCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", h3cell, err)
      temp = 0 // Default to 0 if we fail to fetch the temperature
    }

    // Create coordinates for the cell
    cellCoordinates := make([][]float64, len(cellBoundary))
    for j, coord := range cellBoundary {
      cellCoordinates[j] = []float64{coord.Longitude, coord.Latitude}
    }
    cellCoordinates = append(cellCoordinates, cellCoordinates[0])

    cellCoordsInterface := make([]interface{}, len(cellCoordinates))
    for j, c := range cellCoordinates {
      cellCoordsInterface[j] = c
    }

    feature := GeoJSONFeature{
      Type: "Feature",
      Geometry: map[string]interface{}{
        "type":        "Polygon",
        "coordinates": []interface{}{cellCoordsInterface},
      },
      Properties: map[string]interface{}{
        "h3cell":     h3cell,
        "temperature": temp,
      },
    }
    features = append(features, feature)
  }

  outputGeoJSON := map[string]interface{}{
    "type":     "FeatureCollection",
    "features": features,
  }

  file, err := os.Create(outputFile)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()

  encoder := json.NewEncoder(file)
  encoder.SetIndent("", "  ")
  if err := encoder.Encode(outputGeoJSON); err != nil {
    log.Fatal(err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func main() {
  LoadEnvironmentVariables()

  // Define input and output files
  inputFile := "http/h3cells.json"
  outputFile := "http/h3cells_weather_h4.geojson"

  // Fetch weather data for H3 cells and generate GeoJSON
  fetchWeatherDataForH3Cells(inputFile, outputFile)
}