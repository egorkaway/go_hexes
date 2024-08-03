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

  h3 "github.com/uber/h3-go/v3"
  "github.com/joho/godotenv"
)

const (
  exportDir = "export"
)

var (
  bucketName = "" // Bucket name will be fetched dynamically
  token      = "" // New token for authentication
)

type H3Data struct {
  H3Index string `json:"h3_index"`
  Visits  int    `json:"visits"`
}

type BucketResponse struct {
  BucketID string `json:"bucketId"`
}

type TokenResponse struct {
  AccessToken string `json:"access_token"`
}

func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

func fetchDefaultBucket() (string, error) {
  resp, err := http.Get("http://127.0.0.1:1106/object-storage/default-bucket")
  if err != nil {
    return "", err
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return "", err
  }

  fmt.Printf("Bucket response: %s\n", string(body)) // Debugging: Print the raw response

  var result BucketResponse
  err = json.Unmarshal(body, &result)
  if err != nil {
    return "", err
  }

  // Catch unexpected empty bucket ID
  if result.BucketID == "" {
    return "", fmt.Errorf("fetched bucket ID is empty")
  }

  return result.BucketID, nil
}

func fetchToken() (string, error) {
  resp, err := http.Post("http://127.0.0.1:1106/token", "application/json", nil)
  if err != nil {
    return "", err
  }
  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return "", err
  }

  fmt.Printf("Token response: %s\n", string(body)) // Debugging: Print the raw response

  var result TokenResponse
  err = json.Unmarshal(body, &result)
  if err != nil {
    return "", err
  }

  return result.AccessToken, nil
}

func uploadToObjectStorage(filename, bucketName, token string) error {
  if bucketName == "" {
    return fmt.Errorf("bucket name is empty")
  }

  fileData, err := ioutil.ReadFile(filename)
  if err != nil {
    return err
  }

  url := fmt.Sprintf("https://storage.googleapis.com/upload/storage/v1/b/%s/o?uploadType=media&name=%s", bucketName, filepath.Base(filename))
  fmt.Printf("Uploading to URL: %s\n", url) // Debugging line to confirm URL

  req, err := http.NewRequest("POST", url, bytes.NewReader(fileData))
  if err != nil {
    return err
  }
  req.Header.Set("Authorization", "Bearer "+token)
  req.Header.Set("Content-Type", "application/octet-stream")

  client := &http.Client{}
  resp, err := client.Do(req)
  if err != nil {
    return err
  }
  defer resp.Body.Close()

  if resp.StatusCode != http.StatusOK {
    bodyBytes, _ := ioutil.ReadAll(resp.Body)
    return fmt.Errorf("failed to upload file: %s", string(bodyBytes))
  }

  return nil
}

func fetchTemperature(lat, lon float64) (float64, error) {
  const openWeatherMapAPIKey = "e7e06f3f2654e34e138f3d09ea001917"

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

type GeoJSONFeature struct {
  Type       string            `json:"type"`
  Geometry   GeoJSONGeometry   `json:"geometry"`
  Properties map[string]interface{} `json:"properties"`
}

type GeoJSONGeometry struct {
  Type        string          `json:"type"`
  Coordinates [][][]float64   `json:"coordinates"`
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
      "h3cell":      h3cell,
      "temperature": temperature,
    },
  }, nil
}

func fetchWeatherDataForH3Cells(h3Data []H3Data, outputPath string) error {
  features := make([]GeoJSONFeature, 0, len(h3Data))

  for _, data := range h3Data {
    log.Printf("Fetching weather data for H3 cell: %s", data.H3Index)

    // Get the center of the cell to fetch temperature data
    cellCenter := h3.ToGeo(h3.FromString(data.H3Index))

    // Fetch temperature data
    temp, err := fetchTemperature(cellCenter.Latitude, cellCenter.Longitude)
    if err != nil {
      log.Printf("Failed to fetch temperature data for cell %s: %v", data.H3Index, err)
      temp = 0 // Default to 0 if we fail to fetch the temperature
    }

    // Generate GeoJSON feature for the cell
    feature, err := generateGeoJSONFeature(data.H3Index, temp)
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

  var err error
  bucketName, err = fetchDefaultBucket()
  if err != nil {
    log.Fatal("Failed to fetch default bucket ID:", err)
  }
  fmt.Printf("Fetched bucket ID: %s\n", bucketName)

  if bucketName == "" {
    log.Fatal("Bucket name is empty")
  }

  token, err = fetchToken()
  if err != nil {
    log.Fatal("Failed to fetch token:", err)
  }
  fmt.Printf("Fetched token: %s\n", token)

  if err := os.MkdirAll(exportDir, os.ModePerm); err != nil {
    log.Fatal("Failed to create export directory:", err)
  }

  levels := []struct {
    level int
  }{
    {3},
    {4},
    {5},
    {6},
    {7},
  }

  for _, l := range levels {
    jsonFilename := filepath.Join(exportDir, fmt.Sprintf("h3_level_%d.json", l.level))
    fileData, err := ioutil.ReadFile(jsonFilename)
    if err != nil {
      log.Fatalf("Failed to read JSON file for level %d: %v", l.level, err)
    }

    var h3Data []H3Data
    err = json.Unmarshal(fileData, &h3Data)
    if err != nil {
      log.Fatalf("Failed to unmarshal JSON data for level %d: %v", l.level, err)
    }

    geoJSONFilename := filepath.Join(exportDir, fmt.Sprintf("h3_level_%d.geojson", l.level))
    err = fetchWeatherDataForH3Cells(h3Data, geoJSONFilename)
    if err != nil {
      log.Fatalf("Failed to generate GeoJSON file for level %d: %v", l.level, err)
    }

    err = uploadToObjectStorage(geoJSONFilename, bucketName, token)
    if err != nil {
      log.Fatalf("Failed to upload GeoJSON file to Object Storage for level %d: %v", l.level, err)
    }
  }

  log.Print("Successfully processed all levels, uploaded GeoJSON files to Object Storage")
}