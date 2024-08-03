package main

import (
  "log"
  "os"
  "os/exec"
)

func runCommand(name string, arg ...string) {
  cmd := exec.Command(name, arg...)
  cmd.Stdout = os.Stdout
  cmd.Stderr = os.Stderr

  if err := cmd.Run(); err != nil {
    log.Fatalf("Command %s failed with error: %v", name, err)
  }
}

func main() {
  // Run data2json.go to fetch data and store it into JSON files
  runCommand("go", "run", "code/data2json.go")

  // Run json2weather.go to fetch weather data and generate GeoJSON files
  runCommand("go", "run", "code/json2weather.go")

  log.Print("Successfully completed data to weather processing")
}