package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "path/filepath"

    "github.com/joho/godotenv"
    "github.com/rivo/uniseg"
    "github.com/uber/h3-go/v3"
)

const (
    europeDir          = "http/europe"
    inputJSONFile      = "europe_h3_level_2.json"
    outputGeoJSONFile  = "europe_h3_level_2_emoji.geojson"
    weatherCodesFile   = "weather_codes.json"
)

type H3Data struct {
    H3Index string `json:"h3_index"`
    Visits  int    `json:"visits"`
}

type WeatherCode struct {
    Emoji     string `json:"emoji"`
    Condition string `json:"condition"`
}

var weatherCodes map[string]WeatherCode

func loadEnv() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
}

func readWeatherCodes() {
    data, err := ioutil.ReadFile(weatherCodesFile)
    if err != nil {
        log.Fatalf("Failed to read weather codes file: %v", err)
    }

    if err := json.Unmarshal(data, &weatherCodes); err != nil {
        log.Fatalf("Failed to parse weather codes file: %v", err)
    }
}

func getFirstVisibleCharacter(emoji string) string {
    var graphemeStream = uniseg.NewGraphemes(emoji)
    _ = graphemeStream.Next() // Move to the first character
    return graphemeStream.Str() // Get the first grapheme
}

func getEmojiForWeatherCode(code string) string {
    if wc, exists := weatherCodes[code]; exists {
        return getFirstVisibleCharacter(wc.Emoji) // Only use the first visible character
    }
    return ""
}

func fetchWeatherData(lat, lon float64) (float64, string, error) {
    openWeatherMapAPIKey := os.Getenv("OPENWEATHERMAP_API_KEY")
    if openWeatherMapAPIKey == "" {
        return 0, "", fmt.Errorf("OPENWEATHERMAP_API_KEY not set in environment")
    }

    url := fmt.Sprintf("https://api.openweathermap.org/data/2.5/weather?lat=%f&lon=%f&appid=%s&units=metric", lat, lon, openWeatherMapAPIKey)
    response, err := http.Get(url)
    if err != nil {
        return 0, "", err
    }
    defer response.Body.Close()

    if response.StatusCode != http.StatusOK {
        bodyBytes, _ := ioutil.ReadAll(response.Body)
        return 0, "", fmt.Errorf("failed to fetch weather data: %s", string(bodyBytes))
    }

    var weatherData map[string]interface{}
    if err := json.NewDecoder(response.Body).Decode(&weatherData); err != nil {
        return 0, "", err
    }

    main, ok := weatherData["main"].(map[string]interface{})
    if !ok {
        return 0, "", fmt.Errorf("invalid response format")
    }

    temp, ok := main["temp"].(float64)
    if !ok {
        return 0, "", fmt.Errorf("invalid temperature data")
    }

    weather := weatherData["weather"].([]interface{})[0].(map[string]interface{})
    weatherCode := fmt.Sprintf("%v", weather["id"])

    return temp, weatherCode, nil
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

func generateGeoJSONFeature(h3cell string, temperature float64, weatherCode string) (GeoJSONFeature, error) {
    cellIndex := h3.FromString(h3cell)
    cellBoundary := h3.ToGeoBoundary(cellIndex)

    coordinates := make([][]float64, len(cellBoundary)+1)
    for i, coord := range cellBoundary {
        coordinates[i] = []float64{coord.Longitude, coord.Latitude}
    }
    coordinates[len(cellBoundary)] = coordinates[0] // Close the polygon

    emoji := getEmojiForWeatherCode(weatherCode)

    return GeoJSONFeature{
        Type: "Feature",
        Geometry: GeoJSONGeometry{
            Type:        "Polygon",
            Coordinates: [][][]float64{coordinates},
        },
        Properties: map[string]interface{}{
            "h3cell":      h3cell,
            "temperature": temperature,
            "weather_code": weatherCode,
            "emoji":       emoji,
        },
    }, nil
}

func fetchWeatherDataForH3Cells(h3Data []H3Data, outputPath string) error {
    features := make([]GeoJSONFeature, 0, len(h3Data))

    for _, data := range h3Data {
        log.Printf("Fetching weather data for H3 cell: %s", data.H3Index)

        cellCenter := h3.ToGeo(h3.FromString(data.H3Index))

        temp, weatherCode, err := fetchWeatherData(cellCenter.Latitude, cellCenter.Longitude)
        if err != nil {
            log.Printf("Failed to fetch weather data for cell %s: %v", data.H3Index, err)
            temp = 0
            weatherCode = "unknown"
        }

        feature, err := generateGeoJSONFeature(data.H3Index, temp, weatherCode)
        if err != nil {
            return err
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
        return err
    }

    if err := ioutil.WriteFile(outputPath, buf.Bytes(), 0644); err != nil {
        return err
    }

    return nil
}

func main() {
    loadEnv()
    readWeatherCodes()

    h3DataFile := filepath.Join(europeDir, inputJSONFile)
    data, err := ioutil.ReadFile(h3DataFile)
    if err != nil {
        log.Fatalf("Failed to read H3 data file: %v", err)
    }

    var h3Data []H3Data
    if err := json.Unmarshal(data, &h3Data); err != nil {
        log.Fatalf("Failed to parse H3 data: %v", err)
    }

    geoJSONFile := filepath.Join(europeDir, outputGeoJSONFile)
    err = fetchWeatherDataForH3Cells(h3Data, geoJSONFile)
    if err != nil {
        log.Fatalf("Failed to generate GeoJSON file with weather data: %v", err)
    }

    log.Println("Successfully generated GeoJSON file with weather data:", geoJSONFile)
}