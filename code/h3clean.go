package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
)

func main() {
  // Define input and output files
  inputFile := "http/h3cells.json"
  outputFile := "http/h3cells_cleaned.json"

  // Read the input JSON file
  data, err := ioutil.ReadFile(inputFile)
  if err != nil {
    log.Fatalf("Failed to read input JSON file: %v", err)
  }

  // Parse the JSON data
  var h3CellsJSON map[string][]string
  if err := json.Unmarshal(data, &h3CellsJSON); err != nil {
    log.Fatalf("Failed to parse input JSON file: %v", err)
  }

  // Use a map to track unique H3 indices
  uniqueH3Indices := make(map[string]bool)
  var uniqueH3Cells []string

  for _, h3cell := range h3CellsJSON["h3cells"] {
    if !uniqueH3Indices[h3cell] {
      uniqueH3Indices[h3cell] = true
      uniqueH3Cells = append(uniqueH3Cells, h3cell)
    }
  }

  // Create a new JSON object with unique H3 indices
  uniqueH3CellsJSON := map[string][]string{
    "h3cells": uniqueH3Cells,
  }

  // Convert the map back to JSON
  cleanedData, err := json.MarshalIndent(uniqueH3CellsJSON, "", "  ")
  if err != nil {
    log.Fatalf("Failed to marshal cleaned data to JSON: %v", err)
  }

  // Write the cleaned data to the output file
  if err := ioutil.WriteFile(outputFile, cleanedData, 0644); err != nil {
    log.Fatalf("Failed to write cleaned data to output file: %v", err)
  }

  fmt.Printf("Cleaned JSON file created: %s\n", outputFile)
}