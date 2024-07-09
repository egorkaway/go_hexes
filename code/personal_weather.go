package main

import (
  "bytes"
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"

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
  Geometry   GeoJSONGeometry        `json:"geometry"`
  Properties map[string]interface{} `json:"properties"`
}

type GeoJSONGeometry struct {
  Type        string          `json:"type"`
  Coordinates [][][]float64   `json:"coordinates"`
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

func generateGeoJSONFeature(h3cell string, temperature float64) (GeoJSONFeature, error) {
  cellIndex := h3.FromString(h3cell)
  cellBoundary := h3.ToGeoBoundary(cellIndex)

  // Create coordinates for the cell in the correct format
  coordinates := make([][]float64, len(cellBoundary)+1)
  for i, coord := range cellBoundary {
    coordinates[i] = []float64{coord.Longitude, coord.Latitude}
  }
  // Close the polygon by repeating the first set of coordinates
  coordinates[len(cellBoundary)] = coordinates[0]

  return GeoJSONFeature{
    Type: "Feature",
    Geometry: GeoJSONGeometry{
      Type:        "Polygon",
      Coordinates: [][][]float64{coordinates},
    },
    Properties: map[string]interface{}{
      "h3cell":     h3cell,
      "temperature": temperature,
    },
  }, nil
}

func fetchWeatherDataForH3Cells(h3Cells []string, outputFile string) {
  features := make([]GeoJSONFeature, 0, len(h3Cells))

  for i, h3cell := range h3Cells {
    log.Printf("Fetching weather data for cell %d/%d: %s", i+1, len(h3Cells), h3cell)

    // Get the center of the cell to fetch temperature data
    cellCenter := h3.ToGeo(h3.FromString(h3cell))

    // Fetch temperature data
    temp, err := fetchTemperature(cellCenter.Latitude, cellCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", h3cell, err)
      temp = 0 // Default to 0 if we fail to fetch the temperature
    }

    // Generate GeoJSON feature for the cell
    feature, err := generateGeoJSONFeature(h3cell, temp)
    if err != nil {
      log.Printf("Failed to generate GeoJSON feature for cell %s: %v", h3cell, err)
      continue
    }

    features = append(features, feature)
  }

  outputGeoJSON := map[string]interface{}{
    "type":     "FeatureCollection",
    "features": features,
  }

  var buf bytes.Buffer
  encoder := json.NewEncoder(&buf)
  encoder.SetEscapeHTML(false)
  if err := encoder.Encode(outputGeoJSON); err != nil {
    log.Fatal(err)
  }

  // Remove unnecessary line breaks and whitespaces
  output := buf.String()
  output = output[:len(output)-1] // Remove trailing newline

  if err := ioutil.WriteFile(outputFile, []byte(output), 0644); err != nil {
    log.Fatal(err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func main() {
  LoadEnvironmentVariables()

  // List of parent H3 cells JSON files for different levels
  parentHexFiles := map[int]string{
    5: "http/h3parents_level5.json",
    4: "http/h3parents_level4.json",
    3: "http/h3parents_level3.json",
    2: "http/h3parents_level2.json",
    1: "http/h3parents_level1.json",
  }

  // Output files corresponding to each level
  outputFiles := map[int]string{
    5: "http/h3parents_level5_weather.geojson",
    4: "http/h3parents_level4_weather.geojson",
    3: "http/h3parents_level3_weather.geojson",
    2: "http/h3parents_level2_weather.geojson",
    1: "http/h3parents_level1_weather.geojson",
  }

  for level, inputFile := range parentHexFiles {
    data, err := ioutil.ReadFile(inputFile)
    if err != nil {
      log.Fatalf("Failed to read input JSON file (%s): %v", inputFile, err)
    }

    // Parse the JSON data
    var h3CellsJSON map[string][]string
    if err := json.Unmarshal(data, &h3CellsJSON); err != nil {
      log.Fatalf("Failed to parse input JSON file (%s): %v", inputFile, err)
    }

    h3Cells := h3CellsJSON[fmt.Sprintf("h3cells_level%d", level)]
    outputFile := outputFiles[level]

    // Fetch weather data for H3 cells and generate GeoJSON
    fetchWeatherDataForH3Cells(h3Cells, outputFile)
  }
}