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
  Type        string        `json:"type"`
  Coordinates [][][]float64 `json:"coordinates"`
}

type H3CellInt struct {
  Index string `json:"index"`
  Value int    `json:"value"`
}

type H3CellString struct {
  Index string `json:"index"`
  Value string `json:"value"`
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

  coordinates := make([][]float64, len(cellBoundary)+1)
  for i, coord := range cellBoundary {
    coordinates[i] = []float64{coord.Longitude, coord.Latitude}
  }
  coordinates[len(cellBoundary)] = coordinates[0]

  return GeoJSONFeature{
    Type: "Feature",
    Geometry: GeoJSONGeometry{
      Type:        "Polygon",
      Coordinates: [][][]float64{coordinates},
    },
    Properties: map[string]interface{}{
      "h3cell":      h3cell,
      "temperature": temperature,
    },
  }, nil
}

func fetchWeatherDataForH3Cells(h3Cells map[string][]interface{}, resolution int, outputDir string) {
  features := make([]GeoJSONFeature, 0, len(h3Cells))

  index := 0
  for cell := range h3Cells {
    index++
    log.Printf("Fetching weather data for cell %d/%d: %s", index, len(h3Cells), cell)

    cellCenter := h3.ToGeo(h3.FromString(cell))

    temp, err := fetchTemperature(cellCenter.Latitude, cellCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", cell, err)
      temp = 0
    }

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

  output := buf.String()
  output = output[:len(output)-1]

  outputFile := fmt.Sprintf("%s/h3parent_level%d_weather.geojson", outputDir, resolution)
  if err := ioutil.WriteFile(outputFile, []byte(output), 0644); err != nil {
    log.Fatal(err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func aggregateH3CellsToParents(h3Cells []interface{}, parentResolution int) map[string][]interface{} {
  parentMap := make(map[string][]interface{})
  for _, cell := range h3Cells {
    var index string
    var value interface{}

    switch c := cell.(type) {
    case H3CellInt:
      index = c.Index
      value = c.Value
    case H3CellString:
      index = c.Index
      value = c.Value
    default:
      log.Printf("Unknown cell type: %v", c)
      continue
    }

    parentCell := h3.ToParent(h3.FromString(index), parentResolution)
    parentCellID := h3.ToString(parentCell)
    parentMap[parentCellID] = append(parentMap[parentCellID], value)
  }
  return parentMap
}

func main() {
  LoadEnvironmentVariables()

  var h3Cells []interface{}
  userFiles := []string{"./http/users/1.json", "./http/users/4.json"}

  for _, file := range userFiles {
    data, err := ioutil.ReadFile(file)
    if err != nil {
      log.Fatalf("Failed to read input JSON file %s: %v", file, err)
    }

    var h3CellsInt []H3CellInt
    if err := json.Unmarshal(data, &h3CellsInt); err == nil {
      for _, cell := range h3CellsInt {
        h3Cells = append(h3Cells, cell)
      }
      continue
    }

    var h3CellsString []H3CellString
    if err := json.Unmarshal(data, &h3CellsString); err == nil {
      for _, cell := range h3CellsString {
        h3Cells = append(h3Cells, cell)
      }
      continue
    }

    log.Fatalf("Failed to parse input JSON file %s", file)
  }

  outputDir := "./http/users"

  for resolution := 5; resolution >= 1; resolution-- {
    parentH3Cells := aggregateH3CellsToParents(h3Cells, resolution)
    fetchWeatherDataForH3Cells(parentH3Cells, resolution, outputDir)
  }

  h3CellsMap := make(map[string][]interface{})
  for _, cell := range h3Cells {
    var index string
    var value interface{}

    switch c := cell.(type) {
    case H3CellInt:
      index = c.Index
      value = c.Value
    case H3CellString:
      index = c.Index
      value = c.Value
    default:
      log.Printf("Unknown cell type: %v", c)
      continue
    }

    h3CellsMap[index] = append(h3CellsMap[index], value)
  }
  fetchWeatherDataForH3Cells(h3CellsMap, 6, outputDir)
}