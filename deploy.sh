#!/bin/bash
while true; do
  echo "Generating new H3 cells map..."
  # Run the Go script to generate the GeoJSON file
  go run generate.go

  echo "Starting server..."
  # Run the HTTP server to serve the files
  go run server.go &

  # Save server PID
  SERVER_PID=$!

  # Sleep for 3 hours (10800 seconds)
  sleep 10800

  echo "Stopping server..."
  # Kill the server process
  kill $SERVER_PID
done