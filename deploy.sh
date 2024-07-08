#!/bin/bash
while true; do

  echo "Starting server..."
  # Run the HTTP server to serve the files
  go run main.go &

  # Save server PID
  SERVER_PID=$!

  # Sleep for 3 hours (10800 seconds)
  sleep 10800

  echo "Stopping server..."
  # Kill the server process
  kill $SERVER_PID
done