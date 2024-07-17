#!/bin/bash

while true; do
  echo "Generating GeoJSON files..."
  go run code/personal_weather.go
  echo "GeoJSON files generated. Sleeping for 6 hours..."
  sleep 21600  # 6 hours in seconds
done