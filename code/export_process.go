package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"

  h3 "github.com/uber/h3-go/v3"
)

func main() {
  // Define input and output files
  inputFile := "http/weather_export.json"
  outputFiles := map[int]string{
    5: "http/h3parents_level5.json",
    4: "http/h3parents_level4.json",
    3: "http/h3parents_level3.json",
    2: "http/h3parents_level2.json",
    1: "http/h3parents_level1.json",
  }

  // Read the input JSON file
  data, err := ioutil.ReadFile(inputFile)
  if err != nil {
    log.Fatalf("Failed to read input JSON file: %v", err)
  }

  // Parse the JSON data
  var h3Cells []map[string]interface{}
  if err := json.Unmarshal(data, &h3Cells); err != nil {
    log.Fatalf("Failed to parse input JSON file: %v", err)
  }

  // Initialize maps to track unique H3 indices for each parent level
  parentHexes := map[int]map[string]bool{
    5: {},
    4: {},
    3: {},
    2: {},
    1: {},
  }

  // Generate parent hexes at each level
  for _, cell := range h3Cells {
    h3IndexStr := cell["index"].(string)
    h3Index := h3.FromString(h3IndexStr)

    for res := 5; res >= 1; res-- {
      parentIndex := h3.ToParent(h3Index, res)
      parentIndexStr := h3.ToString(parentIndex)
      if !parentHexes[res][parentIndexStr] {
        parentHexes[res][parentIndexStr] = true
      }
    }
  }

  // Write each parent level to its respective output file
  for level, hexMap := range parentHexes {
    uniqueH3Cells := make([]string, 0, len(hexMap))
    for h3Index := range hexMap {
      uniqueH3Cells = append(uniqueH3Cells, h3Index)
    }

    uniqueH3CellsJSON := map[string][]string{
      fmt.Sprintf("h3cells_level%d", level): uniqueH3Cells,
    }

    // Convert the map back to JSON
    cleanedData, err := json.MarshalIndent(uniqueH3CellsJSON, "", "  ")
    if err != nil {
      log.Fatalf("Failed to marshal cleaned data to JSON: %v", err)
    }

    // Write the cleaned data to the output file
    if err := ioutil.WriteFile(outputFiles[level], cleanedData, 0644); err != nil {
      log.Fatalf("Failed to write cleaned data to output file: %v", err)
    }

    fmt.Printf("Cleaned JSON file for level %d created: %s\n", level, outputFiles[level])
  }
}