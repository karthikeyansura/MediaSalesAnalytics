# ----------------------------------------------------------
# Title: Part A / Configure Cloud Database Analytics
# Course Name: CS5200 Database Management Systems
# Author: Sai Karthikeyan, Sura
# Semester: Spring 2025
# ----------------------------------------------------------

# Function to install packages on demand
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(!installed_packages)) {
    install.packages(packages[!installed_packages])
  }
}

# Function to load required packages
loadRequiredPackages <- function(packages) {
  for (package in packages) {
    suppressMessages(library(package, character.only = TRUE))
  }
}

# Function to release Aiven connections if threshold is exceeded
releaseAivenConnections <- function(threshold = 15) {
  active_cons <- dbListConnections(RMySQL::MySQL())
  if (length(active_cons) >= threshold) {
    cat("[INFO] Threshold reached. Disconnecting existing MySQL connections...\n")
    for (con in active_cons) {
      dbDisconnect(con)
    }
    cat("[INFO] Existing MySQL connections closed.\n")
  }
}

# Function to connect to the cloud MySQL database
connectAndCheckDatabase <- function() {
  
  releaseAivenConnections()
  
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_4zUOM58G58RIBn3nQqg"
  dbHost <- "dbserver-cs5200-media-sales-analytics.b.aivencloud.com"
  dbPort <- 17041
  con <- tryCatch(
    {
      dbConnect(
        RMySQL::MySQL(),
        user = dbUser,
        password = dbPassword,
        dbname = dbName,
        host = dbHost,
        port = dbPort
      )
    },
    error = function(e) {
      return(e$message)
    }
  )
  return(con)
}

# Main
main <- function() {
  installPackagesOnDemand(c("DBI", "RMySQL", "RSQLite"))
  loadRequiredPackages(c("DBI", "RMySQL", "RSQLite"))
  
  # Connect to films and music SQLite databases
  film_db <- dbConnect(RSQLite::SQLite(), "film-sales.db")
  music_db <- dbConnect(RSQLite::SQLite(), "music-sales.db")
  
  # Inspect Operational Database Schemas
  cat("Film Tables:\n"); print(dbListTables(film_db))
  cat("Music Tables:\n"); print(dbListTables(music_db))
  cat("Sample film.payment:\n"); print(dbGetQuery(film_db, "SELECT * FROM payment LIMIT 5"))
  cat("Sample music.invoices:\n"); print(dbGetQuery(music_db, "SELECT * FROM invoices LIMIT 5"))
  
  # Connect to Cloud MySQL database
  con <- connectAndCheckDatabase()
  if (is.character(con)) {
    cat("[ERROR] MySQL connection failed:\n", con, "\n")
  } else {
    cat("[SUCCESS] Connected to MySQL database.\n")
    
    dbExecute(con, "DROP TABLE IF EXISTS test_connection")
    dbExecute(con, "
      CREATE TABLE test_connection (
        id INT PRIMARY KEY,
        label VARCHAR(50) NOT NULL
      )")
    dbExecute(con, "INSERT INTO test_connection VALUES (1, 'Connected')")
    print(dbGetQuery(con, "SELECT * FROM test_connection"))
    dbExecute(con, "DROP TABLE test_connection")
    
    dbDisconnect(con)
    cat("[INFO] MySQL connection closed.\n")
  }
  
  # Disconnect local SQLite DBs
  dbDisconnect(film_db)
  dbDisconnect(music_db)
  cat("[INFO] SQLite connections closed.\n")
}

main()
