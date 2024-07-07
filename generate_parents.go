package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "os"

  h3 "github.com/uber/h3-go/v3"
  "github.com/joho/godotenv"
)

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

  parentFeatures := []GeoJSONFeature{}

  for _, f := range features {
    feature := f.(map[string]interface{})
    properties := feature["properties"].(map[string]interface{})
    h3cell := properties["h3cell"].(string)

    // Compute parent cell (resolution reduced by 1)
    cellIndex := h3.FromString(h3cell)
    resolution := h3.Resolution(cellIndex)
    parentIndex := h3.ToParent(cellIndex, resolution-1)
    parentBoundary := h3.ToGeoBoundary(parentIndex)

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

    parentFeature := GeoJSONFeature{
      Type: "Feature",
      Geometry: map[string]interface{}{
        "type":        "Polygon",
        "coordinates": []interface{}{parentCoordsInterface},
      },
      Properties: map[string]interface{}{
        "h3cell": h3.ToString(parentIndex),
      },
    }
    parentFeatures = append(parentFeatures, parentFeature)
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