#!/bin/bash
while true; do
  echo "Starting server..."
  go run main.go &

  SERVER_PID=$!
  
  echo "Server running with PID $SERVER_PID"

  # Sleep for 3 hours (10800 seconds)
  sleep 10800

  echo "Stopping server..."
  kill $SERVER_PID
done