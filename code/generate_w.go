package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "os"

  h3 "github.com/uber/h3-go/v3"
)

type WeatherData struct {
  Index string `json:"index"`
  Value int    `json:"value"`
}

type GeoJSONFeature struct {
  Type       string                 `json:"type"`
  Geometry   map[string]interface{} `json:"geometry"`
  Properties map[string]interface{} `json:"properties"`
}

func main() {
  filename := "http/weather_export.json"
  data, err := ioutil.ReadFile(filename)
  if err != nil {
    log.Fatalf("Failed to read input JSON file: %v", err)
  }

  var weatherData []WeatherData
  if err := json.Unmarshal(data, &weatherData); err != nil {
    log.Fatalf("Failed to parse JSON data: %v", err)
  }

  var filteredData []WeatherData
  for _, data := range weatherData {
    if data.Value > 3 {
      filteredData = append(filteredData, data)
    }
  }

  var features []GeoJSONFeature

  for _, data := range filteredData {
    cellIndex := h3.FromString(data.Index)
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

    features = append(features, GeoJSONFeature{
      Type: "Feature",
      Geometry: map[string]interface{}{
        "type":        "Polygon",
        "coordinates": []interface{}{coordsInterface},
      },
      Properties: map[string]interface{}{
        "h3cell": data.Index,
        "value":  data.Value,
      },
    })
  }

  geoJSON := map[string]interface{}{
    "type":     "FeatureCollection",
    "features": features,
  }

  outputFile := "http/weather_filtered.geojson"
  file, err := os.Create(outputFile)
  if err != nil {
    log.Fatalf("Failed to create output GeoJSON file: %v", err)
  }
  defer file.Close()

  encoder := json.NewEncoder(file)
  if err := encoder.Encode(geoJSON); err != nil {
    log.Fatalf("Failed to encode GeoJSON data: %v", err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}