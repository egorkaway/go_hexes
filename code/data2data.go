package main

import (
  "database/sql"
  "fmt"
  "log"
  "os"
  "time"

  "github.com/joho/godotenv"
  _ "github.com/lib/pq"
  h3 "github.com/uber/h3-go/v3"
)

const (
  NW_CORNER_LEVEL4_LAT = 63.4305
  NW_CORNER_LEVEL4_LON = -31.1300
  SE_CORNER_LEVEL4_LAT = 27.2579
  SE_CORNER_LEVEL4_LON = 49.8671

  NW_CORNER_LEVEL5_LAT = 43.7914
  NW_CORNER_LEVEL5_LON = -9.3015
  SE_CORNER_LEVEL5_LAT = 35.9468
  SE_CORNER_LEVEL5_LON = 4.6362

  NW_CORNER_LEVEL6_7_LAT = 54.0
  NW_CORNER_LEVEL6_7_LON = -30.0
  SE_CORNER_LEVEL6_7_LAT = 9.3
  SE_CORNER_LEVEL6_7_LON = 3.0
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

func shouldIncludePoint(lat, lon, nwLat, nwLon, seLat, seLon float64) bool {
  return lat <= nwLat && lat >= seLat && lon >= nwLon && lon <= seLon
}

func ensureLastVisitColumn(db *sql.DB, table string) error {
  _, err := db.Exec(fmt.Sprintf(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='%s' AND column_name='last_visit') THEN
        ALTER TABLE %s ADD COLUMN last_visit TIMESTAMP;
      END IF;
    END $$;
  `, table, table))
  return err
}

func fetchDataForLevel(db *sql.DB, table string, minLat, minLon, maxLat, maxLon float64) ([][4]interface{}, error) {
  query := "SELECT latitude, longitude, visits, last_visit FROM cities_with_users WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
  if maxLat != 0 && maxLon != 0 {
    query += fmt.Sprintf(" AND latitude <= %f AND latitude >= %f AND longitude >= %f AND longitude <= %f", minLat, maxLat, minLon, maxLon)
  }

  rows, err := db.Query(query)
  if err != nil {
    return nil, err
  }
  defer rows.Close()

  var data [][4]interface{}
  for rows.Next() {
    var lat, lon float64
    var visits int
    var lastVisit sql.NullTime
    err := rows.Scan(&lat, &lon, &visits, &lastVisit)
    if err != nil {
      return nil, err
    }

    if (maxLat == 0 && maxLon == 0) || shouldIncludePoint(lat, lon, minLat, minLon, maxLat, maxLon) {
      if lastVisit.Valid {
        data = append(data, [4]interface{}{lat, lon, visits, lastVisit.Time})
      } else {
        data = append(data, [4]interface{}{lat, lon, visits, nil})
      }
    }
  }

  return data, nil
}

func aggregateData(data [][4]interface{}, level int) map[string][2]interface{} {
  aggregated := make(map[string][2]interface{})
  for _, record := range data {
    lat := record[0].(float64)
    lon := record[1].(float64)
    visits := record[2].(int)
    h3Index := h3.ToString(h3.FromGeo(h3.GeoCoord{Latitude: lat, Longitude: lon}, level))

    lastVisit, ok := record[3].(time.Time)
    if !ok {
      lastVisit = time.Time{}
    }
    if existing, ok := aggregated[h3Index]; ok {
      aggregated[h3Index] = [2]interface{}{max(existing[0].(int), visits), maxTime(existing[1].(time.Time), lastVisit)}
    } else {
      aggregated[h3Index] = [2]interface{}{visits, lastVisit}
    }
  }
  return aggregated
}

func max(a, b int) int {
  if a > b {
    return a
  }
  return b
}

func maxTime(t1, t2 time.Time) time.Time {
  if t1.After(t2) {
    return t1
  }
  return t2
}

func insertAggregatedData(db *sql.DB, table string, aggregated map[string][2]interface{}) error {
  tx, err := db.Begin()
  if err != nil {
    return err
  }

  for h3Index, values := range aggregated {
    visits := values[0].(int)
    lastVisit, ok := values[1].(time.Time)
    if !ok {
      lastVisit = time.Time{}
    }
    var currentLastVisit sql.NullTime
    err := tx.QueryRow(fmt.Sprintf("SELECT last_visit FROM %s WHERE h3_index = $1", table), h3Index).Scan(&currentLastVisit)
    if err != nil && err != sql.ErrNoRows {
      tx.Rollback()
      return err
    }

    if !currentLastVisit.Valid || (err == nil && currentLastVisit.Valid && currentLastVisit.Time.Before(lastVisit)) {
      log.Printf("%s updated from %s to %s", h3Index, currentLastVisit.Time.Format("2006-01-02 15:04"), lastVisit.Format("2006-01-02 15:04"))
    }

    _, err = tx.Exec(
      `INSERT INTO `+table+` (h3_index, visits, last_visit) VALUES ($1, $2, $3)
        ON CONFLICT (h3_index) DO UPDATE 
        SET visits = GREATEST(`+table+`.visits, EXCLUDED.visits), 
        last_visit = CASE WHEN `+table+`.last_visit IS NULL OR `+table+`.last_visit < EXCLUDED.last_visit THEN EXCLUDED.last_visit ELSE `+table+`.last_visit END`,
      h3Index, visits, lastVisit,
    )
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

func countRows(db *sql.DB, table string) (int, error) {
  var count int
  row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
  err := row.Scan(&count)
  return count, err
}

func main() {
  loadEnv()

  db, err := connectDB()
  if (err != nil ) {
    log.Fatal("Failed to connect to database:", err)
  }
  defer db.Close()

  // Define levels
  levels := []struct {
    level int
    table string
    nwLat float64
    nwLon float64
    seLat float64
    seLon float64
  }{
    {3, "h3_level_3", 0, 0, 0, 0},
    {4, "h3_level_4", NW_CORNER_LEVEL4_LAT, NW_CORNER_LEVEL4_LON, SE_CORNER_LEVEL4_LAT, SE_CORNER_LEVEL4_LON},
    {5, "h3_level_5", NW_CORNER_LEVEL5_LAT, NW_CORNER_LEVEL5_LON, SE_CORNER_LEVEL5_LAT, SE_CORNER_LEVEL5_LON},
    {6, "h3_level_6", NW_CORNER_LEVEL6_7_LAT, NW_CORNER_LEVEL6_7_LON, SE_CORNER_LEVEL6_7_LAT, SE_CORNER_LEVEL6_7_LON},
    {7, "h3_level_7", NW_CORNER_LEVEL6_7_LAT, NW_CORNER_LEVEL6_7_LON, SE_CORNER_LEVEL6_7_LON, SE_CORNER_LEVEL6_7_LON},
  }

  // Prompt for starting level to process
  var startLevel int
  for {
    fmt.Println("Enter the starting level (3-7): ")
    _, err := fmt.Scanln(&startLevel)
    if err == nil && startLevel >= 3 && startLevel <= 7 {
      break
    }
    fmt.Println("Invalid input. Please enter a level between 3 and 7.")
  }

  for _, level := range levels {
    if level.level < startLevel {
      continue  // Skip levels less than the starting level
    }

    err = ensureLastVisitColumn(db, level.table)
    if err != nil {
      log.Fatalf("Failed to ensure last_visit column for table %s: %v", level.table, err)
    }

    beforeCount, err := countRows(db, level.table)
    if err != nil {
      log.Fatalf("Failed to count rows in table %s before processing: %v", level.table, err)
    }
    log.Printf("Number of rows in table %s before processing: %d", level.table, beforeCount)

    data, err := fetchDataForLevel(db, "cities_with_users", level.nwLat, level.nwLon, level.seLat, level.seLon)
    if err != nil {
      log.Fatalf("Failed to fetch data for level %d: %v", level.level, err)
    }

    aggregatedData := aggregateData(data, level.level)

    err = insertAggregatedData(db, level.table, aggregatedData)
    if err != nil {
      log.Fatalf("Failed to insert aggregated data for level %d: %v", level.level, err)
    }

    afterCount, err := countRows(db, level.table)
    if err != nil {
      log.Fatalf("Failed to count rows in table %s after processing: %v", level.table, err)
    }
    log.Printf("Number of rows in table %s after processing: %d", level.table, afterCount)
  }

  log.Println("Successfully aggregated and updated visits for selected levels.")
}