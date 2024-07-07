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

func generateParentCellsGeoJSON(inputFile string, outputFile string) {
  // Read the existing h3cells.geojson file
  data, err := ioutil.ReadFile(inputFile)
  if err != nil {
    log.Fatalf("Failed to read input GeoJSON file: %v", err)
  }

  var geoJSON map[string]interface{}
  if err := json.Unmarshal(data, &geoJSON); err != nil {
    log.Fatalf("Failed to parse input GeoJSON file: %v", err)
  }

  features := geoJSON["features"].([]interface{})

  parentFeaturesMap := make(map[string]GeoJSONFeature)

  for _, f := range features {
    feature := f.(map[string]interface{})
    properties := feature["properties"].(map[string]interface{})
    h3cell := properties["h3cell"].(string)

    // Compute parent cell (resolution reduced by 1)
    cellIndex := h3.FromString(h3cell)
    resolution := h3.Resolution(cellIndex)
    parentIndex := h3.ToParent(cellIndex, resolution-1)
    parentBoundary := h3.ToGeoBoundary(parentIndex)

    // Get the center of the parent cell to fetch temperature data
    parentCenter := h3.ToGeo(parentIndex)

    // Fetch temperature data
    temp, err := fetchTemperature(parentCenter.Latitude, parentCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", h3.ToString(parentIndex), err)
      temp = 0 // Default to 0 if we fail to fetch the temperature
    }

    // Create coordinates for the parent cell
    parentCoordinates := make([][]float64, len(parentBoundary))
    for j, coord := range parentBoundary {
      parentCoordinates[j] = []float64{coord.Longitude, coord.Latitude}
    }
    parentCoordinates = append(parentCoordinates, parentCoordinates[0])

    parentCoordsInterface := make([]interface{}, len(parentCoordinates))
    for j, c := range parentCoordinates {
      parentCoordsInterface[j] = c
    }

    parentH3IndexStr := h3.ToString(parentIndex)

    // Check if parent index already exists
    if _, exists := parentFeaturesMap[parentH3IndexStr]; !exists {
      parentFeature := GeoJSONFeature{
        Type: "Feature",
        Geometry: map[string]interface{}{
          "type":        "Polygon",
          "coordinates": []interface{}{parentCoordsInterface},
        },
        Properties: map[string]interface{}{
          "h3cell":     parentH3IndexStr,
          "temperature": temp,
        },
      }
      parentFeaturesMap[parentH3IndexStr] = parentFeature
    }
  }

  parentFeatures := make([]GeoJSONFeature, 0, len(parentFeaturesMap))
  for _, feature := range parentFeaturesMap {
    parentFeatures = append(parentFeatures, feature)
  }

  parentGeoJSON := map[string]interface{}{
    "type":     "FeatureCollection",
    "features": parentFeatures,
  }

  file, err := os.Create(outputFile)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()

  encoder := json.NewEncoder(file)
  if err := encoder.Encode(parentGeoJSON); err != nil {
    log.Fatal(err)
  }

  fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func main() {
  LoadEnvironmentVariables()

  // Define input and output files
  inputFile := "http/h3cells.geojson"
  outputFile := "http/h3parents.geojson"

  // Generate parent cells GeoJSON
  generateParentCellsGeoJSON(inputFile, outputFile)
}