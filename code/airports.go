package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "strconv"

  h3 "github.com/uber/h3-go/v3"
)

type AirportCoordinates struct {
  Latitude  string `json:"latitude"`
  Longitude string `json:"longitude"`
}

type Airports map[string]AirportCoordinates

type H3Entry struct {
  Index string `json:"index"`
  Value string `json:"value"`
}

func main() {
  // Step 1: Read the existing airport coordinates JSON file
  data, err := ioutil.ReadFile("http/users/airport_coordinates.json")
  if err != nil {
    log.Fatalf("Failed to read airport coordinates file: %v", err)
  }

  var airports Airports
  if err := json.Unmarshal(data, &airports); err != nil {
    log.Fatalf("Failed to parse airport coordinates: %v", err)
  }

  // Step 2: Convert each coordinate to an H3 index at level 6
  var h3Entries []H3Entry
  for airportCode, coords := range airports {
    latitude := coords.Latitude
    longitude := coords.Longitude
    lat, err := strconv.ParseFloat(latitude, 64)
    if err != nil {
      log.Printf("Failed to parse latitude for airport %s: %v", airportCode, err)
      continue
    }
    lon, err := strconv.ParseFloat(longitude, 64)
    if err != nil {
      log.Printf("Failed to parse longitude for airport %s: %v", airportCode, err)
      continue
    }

    // Swap the lat and lon correctly in the GeoCoord
    h3Index := h3.FromGeo(h3.GeoCoord{Latitude: lat, Longitude: lon}, 6)
    h3Str := h3.ToString(h3Index)

    h3Entries = append(h3Entries, H3Entry{Index: h3Str, Value: airportCode})
  }

  // Step 3: Generate the new JSON structure
  jsonData, err := json.MarshalIndent(h3Entries, "", "  ")
  if err != nil {
    log.Fatalf("Failed to marshal H3 entries: %v", err)
  }

  // Step 4: Write the new JSON structure to a file
  outputFile := "http/users/airport_h3_level6.json"
  if err := ioutil.WriteFile(outputFile, jsonData, 0644); err != nil {
    log.Fatalf("Failed to write output file: %v", err)
  }

  fmt.Printf("H3 level 6 indexes for airports written to %s\n", outputFile)
}