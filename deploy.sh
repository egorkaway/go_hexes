#!/bin/bash
while true; do
  echo "Starting server..."
  go run main.go &

  SERVER_PID=$!
  echo "Server running with PID $SERVER_PID"

  ./generate.sh &
  GENERATE_PID=$!
  echo "GeoJSON generation process running with PID $GENERATE_PID"

  # Sleep for 3 hours (10800 seconds)
  sleep 10800

  echo "Stopping server..."
  kill $SERVER_PID
  echo "Stopping GeoJSON generation process..."
  kill $GENERATE_PID
done