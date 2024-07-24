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

type H3Cell struct {
  Index string `json:"index"`
  Value int    `json:"value"`
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

  // Create coordinates in the correct format
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

func fetchWeatherDataForH3Cells(h3Cells map[string][]int, resolution int, outputDir string) {
  features := make([]GeoJSONFeature, 0, len(h3Cells))

  index := 0
  for cell := range h3Cells {
    index++
    log.Printf("Fetching weather data for cell %d/%d: %s", index, len(h3Cells), cell)

    // Get the center of the cell to fetch temperature data
    cellCenter := h3.ToGeo(h3.FromString(cell))

    // Fetch temperature data
    temp, err := fetchTemperature(cellCenter.Latitude, cellCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", cell, err)
      temp = 0 // Default to 0 if we fail to fetch the temperature
    }

    // Generate GeoJSON feature for the cell
    feature, err := generateGeoJSONFeature(cell, temp)
    if err != nil {
      log.Printf("Failed to generate GeoJSON feature for cell %s: %v", cell, err)
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

  outputFile := fmt.Sprintf("%s/h3parent_level%d_weather.geojson", outputDir, resolution)
  if err := ioutil.WriteFile(outputFile, []byte(output), 0644); err != nil {
    log.Fatal(err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func aggregateH3CellsToParents(h3Cells []H3Cell, parentResolution int) map[string][]int {
  parentMap := make(map[string][]int)
  for _, cell := range h3Cells {
    parentCell := h3.ToParent(h3.FromString(cell.Index), parentResolution)
    parentCellID := h3.ToString(parentCell)
    parentMap[parentCellID] = append(parentMap[parentCellID], cell.Value)
  }
  return parentMap
}

func main() {
  LoadEnvironmentVariables()

  // Read JSON file with H3 cells
  data, err := ioutil.ReadFile("./http/users/2.json")
  if err != nil {
    log.Fatalf("Failed to read input JSON file: %v", err)
  }

  var h3Cells []H3Cell
  if err := json.Unmarshal(data, &h3Cells); err != nil {
    log.Fatalf("Failed to parse input JSON file: %v", err)
  }

  outputDir := "./http/users"

  // Generate GeoJSON for resolutions 5 to 1
  for resolution := 5; resolution >= 1; resolution-- {
    parentH3Cells := aggregateH3CellsToParents(h3Cells, resolution)
    fetchWeatherDataForH3Cells(parentH3Cells, resolution, outputDir)
  }

  // Generate GeoJSON for resolution 6 (the original data)
  h3CellsMap := make(map[string][]int)
  for _, cell := range h3Cells {
    h3CellsMap[cell.Index] = append(h3CellsMap[cell.Index], cell.Value)
  }
  fetchWeatherDataForH3Cells(h3CellsMap, 6, outputDir)
}