package main

import (
  "database/sql"
  "log"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
)

const (
  // New database connection URL
  postgresURL = "postgresql://postgres.ylbjmqqjqifpfqcwtrpn:dUspyj-gahzec-madty9@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
)

func loadEnv() {
  err := godotenv.Load()
  if err != nil {
    log.Println("Error loading .env file, proceeding without it")
  }
}

func connectDB() (*sql.DB, error) {
  if postgresURL == "" {
    log.Fatal("POSTGRES_URL not set in environment variables")
  }
  return sql.Open("postgres", postgresURL)
}

func addH3ColumnIfNotExists(db *sql.DB) error {
  _, err := db.Exec(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='cities_with_users' AND column_name='h3l7') THEN
        ALTER TABLE cities_with_users ADD COLUMN h3l7 VARCHAR(15);
      END IF;
    END $$;
  `)
  return err
}

func updateH3Column(db *sql.DB) error {
  rows, err := db.Query(`
    SELECT id, latitude, longitude 
    FROM cities_with_users 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND h3l7 IS NULL
  `)
  if err != nil {
    return err
  }
  defer rows.Close()

  var id int
  var latitude, longitude float64

  tx, err := db.Begin()
  if err != nil {
    return err
  }

  for rows.Next() {
    err := rows.Scan(&id, &latitude, &longitude)
    if err != nil {
      return err
    }

    h3Index := h3.ToString(h3.FromGeo(h3.GeoCoord{Latitude: latitude, Longitude: longitude}, 7))

    _, err = tx.Exec("UPDATE cities_with_users SET h3l7=$1 WHERE id=$2", h3Index, id)
    if err != nil {
      tx.Rollback()
      return err
    }
  }

  err = tx.Commit()
  if err != nil {
    return err
  }

  return nil
}

func main() {
  loadEnv()

  db, err := connectDB()
  if err != nil {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  err = addH3ColumnIfNotExists(db)
  if err != nil {
    log.Fatal("Failed to add h3l7 column:", err)
  }

  err = updateH3Column(db)
  if err != nil {
    log.Fatal("Failed to update h3l7 column:", err)
  }

  log.Println("Successfully updated h3l7 column with H3 indices of level 7 for all relevant rows.")
}