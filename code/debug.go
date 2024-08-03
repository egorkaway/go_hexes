package main

import (
  "database/sql"
  "log"
  "os"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
)

func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Fatal("Error loading .env file")
  }
}

func connectDB() (*sql.DB, error) {
  postgresURL := os.Getenv("POSTGRES_URL")
  if postgresURL == "" {
    log.Fatal("POSTGRES_URL not set in environment variables")
  }
  return sql.Open("postgres", postgresURL)
}

func traceH3CalculationForIndex(db *sql.DB, targetH3Index string, level int) {
  rows, err := db.Query(`
    SELECT latitude, longitude, visits 
    FROM cities_with_users 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
  `)
  if err != nil {
    log.Fatal("Failed to fetch data from cities_with_users:", err)
  }
  defer rows.Close()

  totalVisits := 0

  for rows.Next() {
    var latitude, longitude float64
    var visits int

    err := rows.Scan(&latitude, &longitude, &visits)
    if err != nil {
      log.Fatal("Failed to scan row:", err)
    }

    // Calculate the H3 index of the row
    h3Index := h3.ToString(h3.FromGeo(h3.GeoCoord{Latitude: latitude, Longitude: longitude}, level))

    // Check if it matches the target H3 index
    if h3Index == targetH3Index {
      totalVisits += visits
      log.Printf("Matching row found: Latitude: %f, Longitude: %f, Visits: %d, H3 Index: %s", latitude, longitude, visits, h3Index)
    }
  }

  log.Printf("Total visits aggregated for H3 index %s: %d", targetH3Index, totalVisits)
}

func main() {
  loadEnv()

  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  const level = 7
  h3Indices := []struct {
    index  string
    visits int
  }{
    {"87392201affffff", 113},
    {"8739220f4ffffff", 725},
    {"87392210bffffff", 68},
  }

  for _, h3Index := range h3Indices {
    log.Printf("Tracing calculation for H3 index %s with expected visits %d", h3Index.index, h3Index.visits)
    traceH3CalculationForIndex(db, h3Index.index, level)
  }
}