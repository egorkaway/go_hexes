package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
	body, err := io.ReadAll(resp.Body)
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

func fetchWeatherDataForH3Cells(inputFile, outputFile string) {
	// Read the h3cells.json file
	data, err := os.ReadFile(inputFile)
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

	if err := os.WriteFile(outputFile, []byte(output), 0644); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("GeoJSON file created: %s\n", outputFile)
}

func main() {
	LoadEnvironmentVariables()

	inputFile := "http/h3cells_cleaned.json"
	outputFile := "http/h3cells_weather_h4.geojson"

	// Fetch weather data for H3 cells and generate GeoJSON
	fetchWeatherDataForH3Cells(inputFile, outputFile)
}