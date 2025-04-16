# title: "Part B / Create Analytics Datamart"
# author: "Sai Karthikeyan, Sura"
# date: "Spring 2025"

options(warn = -1)

# Install Required Packages
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

# Load Required Packages
loadRequiredPackages <- function(packages) {
  # load required packages
  for (package in packages) {
    suppressMessages({
      library(package, character.only = TRUE)
    })
  }
}

# Close Db Connections Over Threshold
closeDbConnectionsOverThreshold <- function() {
  threshold <- 10 # Aiven allows 16 open connections, threshold is set to 10
  currentOpenConnections <- dbListConnections(MySQL())
  if (length(currentOpenConnections) > threshold) {
    for (conn in currentOpenConnections) {
      dbDisconnect(conn)
    }
  }
}

# Connect to MySQL Database
connectToMySQLDatabase <- function() {
  # db credentials
  dbName <- ""
  dbUser <- ""
  dbPassword <- ""
  dbHost <- ""
  dbPort <- 
  
  tryCatch({
    closeDbConnectionsOverThreshold()
    dbCon <- dbConnect(
      RMySQL::MySQL(),
      user = dbUser,
      password = dbPassword,
      dbname = dbName,
      host = dbHost,
      port = dbPort
    )
    cat("Connected to database successfully.\n")
    return(dbCon)
  }, error = function(err) {
    cat("Error connecting to database:", err$message, "\n")
    stop("Database connection failed.")
  })
}

# Connect to SQLite Database
connectToSQLiteDb <- function(dbPath) {
  tryCatch({
    dbCon <- dbConnect(RSQLite::SQLite(), dbname = dbPath)
    cat("Connected to", dbPath, "SQLite database successfully.\n")
    return(dbCon)
  }, error = function(err) {
    cat("Error connecting to SQLite database:", err$message, "\n")
    stop("SQLite database connection failed.")
  })
}

# Print Line Separator
printLine <- function() {
  cat("--------------------------------------------------\n")
}

# Insert Data in Batches
insertInBatches <- function(dbCon, batchSize = 500, query, values) {
  numBatches <- ceiling(length(values) / batchSize)
  for (i in 1:numBatches) {
    startIdx <- (i - 1) * batchSize + 1
    endIdx <- min(i * batchSize, length(values))
    batchValues <- values[startIdx:endIdx]
    completeQuery <- paste(query, paste(batchValues, collapse = ","))
    #print(query) # for debugging
    dbExecute(dbCon, completeQuery)
  }
}

# Extract Data from Source
extractDataFromSource <- function(dbCon, query) {
  # extract data from source database
  data <- dbGetQuery(dbCon, query)
  return(data)
}

# SQL Query to Extract Film Location Data
getFilmLocationQuery <- function() {
  return("
    SELECT 
      a.address_id AS source_id,
      'FILM' AS source_system,
      a.address,
      c.city,
      NULL AS state_province,
      a.postal_code,
      co.country,
      NULL AS region,
      a.last_update
    FROM address a
    JOIN city c ON a.city_id = c.city_id
    JOIN country co ON c.country_id = co.country_id
  ")
}

# SQL Query to Extract Music Location Data
getMusicLocationQuery <- function() {
  return("
     SELECT 
      CustomerId AS source_id,
      'MUSIC' AS source_system,
      Address AS address,
      City AS city,
      State AS state_province,
      PostalCode AS postal_code,
      Country AS country,
      NULL AS region,
      CURRENT_TIMESTAMP AS last_update
    FROM customers
    WHERE Address IS NOT NULL
  ")
}

# ETL Pipeline for Location Data
locationDataETL <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  # extract location data from both source databases
  filmLocationData <- extractDataFromSource(filmSalesDbCon, getFilmLocationQuery())
  musicLocationData <- extractDataFromSource(musicSalesDbCon, getMusicLocationQuery())
  allLocations <- rbind(filmLocationData, musicLocationData) # combine data
  
  # transform the data
  allLocations$state_province[is.na(allLocations$state_province)] <- NA
  allLocations$postal_code[is.na(allLocations$postal_code)] <- NA
  allLocations$region[is.na(allLocations$region)] <- NA
  
  # insert query
  insertQuery <- paste0("INSERT INTO dim_location ",
                        "(source_id, source_system, address, city, state_province, 
                          postal_code, country, region) ", "
                        VALUES ")
  
  # format values for batch insert
  values <- apply(allLocations, 1, function(row) {
    formatSqlValue <- function(val) {
      if (is.na(val)) {
        return("NULL")
      } else {
        # escape single quotes in the string value
        escaped_val <- gsub("'", "''", as.character(val))
        return(paste0("'", escaped_val, "'"))
      }
    }
    
    # construct the value string with proper NULL handling
    sprintf("(%s, %s, %s, %s, %s, %s, %s, %s)",
            formatSqlValue(row["source_id"]),
            formatSqlValue(row["source_system"]),
            formatSqlValue(row["address"]),
            formatSqlValue(row["city"]),
            formatSqlValue(row["state_province"]),
            formatSqlValue(row["postal_code"]),
            formatSqlValue(row["country"]),
            formatSqlValue(row["region"]))
  })
  
  # insert data in batches
  insertInBatches(analyticsDbCon, batchSize = 500, insertQuery, values)
  
  cat("ETL Successful: dim_location table loaded into analytical db.\n")
}

# SQL Query to Extract Film Customer Data
getFilmCustomerQuery <- function() {
  return("
    SELECT 
      customer_id AS source_customer_id,
      'FILM' AS source_system,
      first_name,
      last_name,
      email,
      active,
      create_date
    FROM customer
  ")
}

# SQL Query to Extract Music Customer Data
getMusicCustomerQuery <- function() {
  return("
    SELECT 
      CustomerId AS source_customer_id,
      'MUSIC' AS source_system,
      FirstName AS first_name,
      LastName AS last_name,
      Email AS email,
      1 AS active,
      CURRENT_TIMESTAMP AS create_date
    FROM customers
  ")
}

# ETL Pipeline for Customer Data
customerDataETL <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  # extract customer data from both source databases
  filmCustomerData <- extractDataFromSource(filmSalesDbCon, getFilmCustomerQuery())
  musicCustomerData <- extractDataFromSource(musicSalesDbCon, getMusicCustomerQuery())
  allCustomers <- rbind(filmCustomerData, musicCustomerData) # combine data
  
  # transform the data
  allCustomers$email[is.na(allCustomers$email)] <- NA
  allCustomers$create_date[is.na(allCustomers$create_date)] <- NA

  # insert query
  insertQuery <- paste0("INSERT INTO dim_customer ",
                        "(source_customer_id, source_system, first_name, 
                          last_name, email, active, create_date) ", 
                        "VALUES ")
  
  # format values for batch insert
  values <- apply(allCustomers, 1, function(row) {
    formatSqlValue <- function(val) {
      if (is.na(val)) {
        return("NULL")
      } else {
        # escape single quotes in the string value
        escaped_val <- gsub("'", "''", as.character(val))
        return(paste0("'", escaped_val, "'"))
      }
    }
    
    # construct the value string with proper NULL handling
    sprintf("(%s, %s, %s, %s, %s, %s, %s)",
            formatSqlValue(row["source_customer_id"]),
            formatSqlValue(row["source_system"]),
            formatSqlValue(row["first_name"]),
            formatSqlValue(row["last_name"]),
            formatSqlValue(row["email"]),
            formatSqlValue(row["active"]),
            formatSqlValue(row["create_date"]))
  })
  
  # insert data in batches
  insertInBatches(analyticsDbCon, batchSize = 500, insertQuery, values)
  
  cat("ETL Successful: dim_customer table loaded into analytical db.\n")
}

# SQL Query to Extract Film Product Data
getFilmProductQuery <- function() {
  return("
     SELECT 
      f.film_id AS source_product_id,
      'FILM' AS source_system,
      'FILM' AS product_type,
      f.title,
      c.name AS category_genre,
      NULL AS artist_actor,
      f.release_year,
      l.name AS language,
      NULL AS media_type,
      f.rental_rate AS unit_price,
      f.length AS duration
    FROM film f
    LEFT JOIN film_category fc ON f.film_id = fc.film_id
    LEFT JOIN category c ON fc.category_id = c.category_id
    LEFT JOIN language l ON f.language_id = l.language_id
  ")
}

# SQL Query to Extract Music Product Data
getMusicProductQuery <- function() {
  return("
    SELECT 
      t.TrackId AS source_product_id,
      'MUSIC' AS source_system,
      'TRACK' AS product_type,
      t.Name AS title,
      g.Name AS category_genre,
      ar.Name AS artist_actor,
      NULL AS release_year,
      NULL AS language,
      mt.Name AS media_type,
      t.UnitPrice AS unit_price,
      t.Milliseconds AS duration
    FROM tracks t
    LEFT JOIN genres g ON t.GenreId = g.GenreId
    LEFT JOIN albums al ON t.AlbumId = al.AlbumId
    LEFT JOIN artists ar ON al.ArtistId = ar.ArtistId
    LEFT JOIN media_types mt ON t.MediaTypeId = mt.MediaTypeId
  ")
}

# ETL Pipeline for Product Data
productDataETL <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  # extract product data from both source databases
  filmProductData <- extractDataFromSource(filmSalesDbCon, getFilmProductQuery())
  musicProductData <- extractDataFromSource(musicSalesDbCon, getMusicProductQuery())
  allProducts <- rbind(filmProductData, musicProductData) # combine data
  
  # transform the data
  allProducts$category_genre[is.na(allProducts$category_genre)] <- NA
  allProducts$artist_actor[is.na(allProducts$artist_actor)] <- NA
  allProducts$release_year[is.na(allProducts$release_year)] <- NA
  allProducts$language[is.na(allProducts$language)] <- NA
  allProducts$media_type[is.na(allProducts$media_type)] <- NA
  
  # insert query
  insertQuery <- paste0("INSERT INTO dim_product ",
                        "(source_product_id, source_system, product_type, 
                          title, category_genre, artist_actor, release_year, 
                          language, media_type, unit_price, duration) ", 
                        "VALUES ")
  
  # format values for batch insert
  values <- apply(allProducts, 1, function(row) {
    formatSqlValue <- function(val) {
      if (is.na(val)) {
        return("NULL")
      } else {
        # escape single quotes in the string value
        escaped_val <- gsub("'", "''", as.character(val))
        return(paste0("'", escaped_val, "'"))
      }
    }
    
    # construct the value string with proper NULL handling
    sprintf("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            formatSqlValue(row["source_product_id"]),
            formatSqlValue(row["source_system"]),
            formatSqlValue(row["product_type"]),
            formatSqlValue(row["title"]),
            formatSqlValue(row["category_genre"]),
            formatSqlValue(row["artist_actor"]),
            formatSqlValue(row["release_year"]),
            formatSqlValue(row["language"]),
            formatSqlValue(row["media_type"]),
            formatSqlValue(row["unit_price"]),
            formatSqlValue(row["duration"]))
  })
  
  # insert data in batches
  insertInBatches(analyticsDbCon, batchSize = 500, insertQuery, values)
  
  cat("ETL Successful: dim_product table loaded into analytical db.\n")
}

# ETL Pipeline for Time Data
timeDataETL <- function(analyticsDbCon) {
  # generate time data for several years
  startDate <- as.Date("2000-01-01")
  endDate <- as.Date("2013-12-31")
  dateSeq <- seq(startDate, endDate, by="day")
  
  # produce date values
  timeData <- data.frame(
    time_key = as.integer(format(dateSeq, "%Y%m%d")),
    full_date = format(dateSeq, "%Y-%m-%d"),
    day_of_week = weekdays(dateSeq),
    day_num_in_month = as.integer(format(dateSeq, "%d")),
    day_num_in_year = as.integer(format(dateSeq, "%j")),
    month_num = as.integer(format(dateSeq, "%m")),
    month_name = month.abb[as.integer(format(dateSeq, "%m"))],
    quarter = ceiling(as.integer(format(dateSeq, "%m")) / 3),
    year = as.integer(format(dateSeq, "%Y")),
    weekend_flag = as.integer(weekdays(dateSeq) %in% c("Saturday", "Sunday")),
    holiday_flag = 0
  )
  
  # insert query
  insertQuery <- paste0("INSERT INTO dim_time ",
                        "(time_key, full_date, day_of_week, day_num_in_month, 
                          day_num_in_year, month_num, month_name, quarter, 
                          year, weekend_flag, holiday_flag) ", 
                        "VALUES ")
  
  # format values for batch insert
  values <- apply(timeData, 1, function(row) {
    sprintf("(%s, '%s', '%s', %s, %s, %s, '%s', %s, %s, %s, %s)",
            as.integer(row["time_key"]), 
            row["full_date"], 
            row["day_of_week"], 
            as.integer(row["day_num_in_month"]), 
            as.integer(row["day_num_in_year"]), 
            as.integer(row["month_num"]), 
            row["month_name"], 
            as.integer(row["quarter"]), 
            as.integer(row["year"]), 
            as.integer(row["weekend_flag"]), 
            as.integer(row["holiday_flag"]))
  })
  
  # insert data in batches
  insertInBatches(analyticsDbCon, batchSize = 500, insertQuery, values)
  
  cat("ETL Successful: dim_time table loaded into analytical db.\n")
}

getFilmSalesQuery <- function() {
  return("
    SELECT 
      p.payment_id AS source_transaction_id,
      'FILM' AS source_system,
      DATE(p.payment_date) AS transaction_date,
      p.customer_id,
      i.film_id AS product_id,
      co.country,
      1 AS quantity,
      p.amount AS amount
    FROM payment p
    JOIN rental r ON p.rental_id = r.rental_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN customer c ON p.customer_id = c.customer_id
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
  ")
}

getMusicSalesQuery <- function() {
  return("
    SELECT 
      ii.InvoiceLineId AS source_transaction_id,
      'MUSIC' AS source_system,
      DATE(i.InvoiceDate) AS transaction_date,
      i.CustomerId AS customer_id,
      ii.TrackId AS product_id,
      i.BillingCountry AS country,
      ii.Quantity AS quantity,
      ii.UnitPrice AS amount
    FROM invoice_items ii
    JOIN invoices i ON ii.InvoiceId = i.InvoiceId
  ")
}

salesDataETL <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon, lookups) {
  # extract sales data from both source databases
  filmSalesData <- extractDataFromSource(filmSalesDbCon, getFilmSalesQuery())
  musicSalesData <- extractDataFromSource(musicSalesDbCon, getMusicSalesQuery())
  
  # transform film sales
  filmSalesData$time_key <- as.integer(format(as.Date(filmSalesData$transaction_date), "%Y%m%d"))
  
  # Create lookup keys
  filmCustomerKeys <- paste(filmSalesData$customer_id, filmSalesData$source_system, sep = "-")
  filmProductKeys <- paste(filmSalesData$product_id, filmSalesData$source_system, sep = "-")
  filmLocationKeys <- paste(filmSalesData$country, filmSalesData$source_system, sep = "-")
  
  # Map dimension keys using lookups
  filmSalesData$customer_key <- lookups$customerLookup[filmCustomerKeys]
  filmSalesData$product_key <- lookups$productLookup[filmProductKeys]
  filmSalesData$location_key <- lookups$locationLookup[filmLocationKeys]
  
  # Filter out rows with NA keys
  filmSalesData <- filmSalesData[!is.na(filmSalesData$customer_key) & 
                                   !is.na(filmSalesData$product_key) & 
                                   !is.na(filmSalesData$location_key), ]
  
  # transform music sales
  musicSalesData$time_key <- as.integer(format(as.Date(musicSalesData$transaction_date), "%Y%m%d"))
  
  # Create lookup keys
  musicCustomerKeys <- paste(musicSalesData$customer_id, musicSalesData$source_system, sep = "-")
  musicProductKeys <- paste(musicSalesData$product_id, musicSalesData$source_system, sep = "-")
  musicLocationKeys <- paste(musicSalesData$country, musicSalesData$source_system, sep = "-")
  
  # Map dimension keys using lookups
  musicSalesData$customer_key <- lookups$customerLookup[musicCustomerKeys]
  musicSalesData$product_key <- lookups$productLookup[musicProductKeys]
  musicSalesData$location_key <- lookups$locationLookup[musicLocationKeys]
  
  # Filter out rows with NA keys
  musicSalesData <- musicSalesData[!is.na(musicSalesData$customer_key) & 
                                     !is.na(musicSalesData$product_key) & 
                                     !is.na(musicSalesData$location_key), ]
  
  # Select only needed columns
  filmSalesSubset <- filmSalesData[, c("time_key", "customer_key", "product_key", "location_key", 
                                       "source_system", "source_transaction_id", "transaction_date", 
                                       "quantity", "amount")]
  
  musicSalesSubset <- musicSalesData[, c("time_key", "customer_key", "product_key", "location_key", 
                                         "source_system", "source_transaction_id", "transaction_date", 
                                         "quantity", "amount")]
  
  allSales <- rbind(filmSalesSubset, musicSalesSubset)
  
  # Ensure date format is correct
  allSales$transaction_date <- format(as.Date(allSales$transaction_date), "%Y-%m-%d")
  
  # insert query
  insertQuery <- paste0("INSERT INTO sales_facts ",
                        "(time_key, customer_key, product_key, location_key, 
                          source_system, source_transaction_id, transaction_date, 
                          quantity, amount) ", 
                        "VALUES ")
  
  # format values for batch insert
  values <- apply(allSales, 1, function(row) {
    sprintf("(%d, %d, %d, %d, '%s', '%s', '%s', %d, %f)",
            as.integer(row["time_key"]), 
            as.integer(row["customer_key"]), 
            as.integer(row["product_key"]), 
            as.integer(row["location_key"]),
            row["source_system"], 
            row["source_transaction_id"], 
            row["transaction_date"],
            as.integer(row["quantity"]), 
            as.numeric(row["amount"]))
  })
  
  # insert data in batches
  insertInBatches(analyticsDbCon, batchSize = 500, insertQuery, values)
  
  cat("ETL Successful: sales_facts table loaded into analytical db.\n")
}

timeAggregatedFactsETL <- function(analyticsDbCon) {
  
  # Aggregation query for day level
  dayQuery <- "
    INSERT INTO time_agg_facts (
      time_key, time_level, country, source_system,
      total_revenue, avg_revenue_per_transaction, 
      total_units_sold, avg_units_per_transaction,
      min_units_per_transaction, max_units_per_transaction,
      min_revenue_per_transaction, max_revenue_per_transaction,
      customer_count, transaction_count
    )
    SELECT 
      sf.time_key,
      'DAY' AS time_level,
      dl.country,
      sf.source_system,
      SUM(sf.amount) AS total_revenue,
      AVG(sf.amount) AS avg_revenue_per_transaction,
      SUM(sf.quantity) AS total_units_sold,
      AVG(sf.quantity) AS avg_units_per_transaction,
      MIN(sf.quantity) AS min_units_per_transaction,
      MAX(sf.quantity) AS max_units_per_transaction,
      MIN(sf.amount) AS min_revenue_per_transaction,
      MAX(sf.amount) AS max_revenue_per_transaction,
      COUNT(DISTINCT sf.customer_key) AS customer_count,
      COUNT(*) AS transaction_count
    FROM sales_facts sf
    JOIN dim_location dl ON sf.location_key = dl.location_key
    GROUP BY sf.time_key, dl.country, sf.source_system
  "
  dbExecute(analyticsDbCon, dayQuery)
  
  # Aggregation query for month level
  monthQuery <- "
    INSERT INTO time_agg_facts (
      time_key, time_level, country, source_system,
      total_revenue, avg_revenue_per_transaction, 
      total_units_sold, avg_units_per_transaction,
      min_units_per_transaction, max_units_per_transaction,
      min_revenue_per_transaction, max_revenue_per_transaction,
      customer_count, transaction_count
    )
    SELECT 
      (dt.year * 100 + dt.month_num) AS time_key,
      'MONTH' AS time_level,
      dl.country,
      sf.source_system,
      SUM(sf.amount) AS total_revenue,
      AVG(sf.amount) AS avg_revenue_per_transaction,
      SUM(sf.quantity) AS total_units_sold,
      AVG(sf.quantity) AS avg_units_per_transaction,
      MIN(sf.quantity) AS min_units_per_transaction,
      MAX(sf.quantity) AS max_units_per_transaction,
      MIN(sf.amount) AS min_revenue_per_transaction,
      MAX(sf.amount) AS max_revenue_per_transaction,
      COUNT(DISTINCT sf.customer_key) AS customer_count,
      COUNT(*) AS transaction_count
    FROM sales_facts sf
    JOIN dim_time dt ON sf.time_key = dt.time_key
    JOIN dim_location dl ON sf.location_key = dl.location_key
    GROUP BY dt.year, dt.month_num, dl.country, sf.source_system
  "
  dbExecute(analyticsDbCon, monthQuery)
  
  # Aggregation query for quarter level
  quarterQuery <- "
    INSERT INTO time_agg_facts (
      time_key, time_level, country, source_system,
      total_revenue, avg_revenue_per_transaction, 
      total_units_sold, avg_units_per_transaction,
      min_units_per_transaction, max_units_per_transaction,
      min_revenue_per_transaction, max_revenue_per_transaction,
      customer_count, transaction_count
    )
    SELECT 
      (dt.year * 10 + dt.quarter) AS time_key,
      'QUARTER' AS time_level,
      dl.country,
      sf.source_system,
      SUM(sf.amount) AS total_revenue,
      AVG(sf.amount) AS avg_revenue_per_transaction,
      SUM(sf.quantity) AS total_units_sold,
      AVG(sf.quantity) AS avg_units_per_transaction,
      MIN(sf.quantity) AS min_units_per_transaction,
      MAX(sf.quantity) AS max_units_per_transaction,
      MIN(sf.amount) AS min_revenue_per_transaction,
      MAX(sf.amount) AS max_revenue_per_transaction,
      COUNT(DISTINCT sf.customer_key) AS customer_count,
      COUNT(*) AS transaction_count
    FROM sales_facts sf
    JOIN dim_time dt ON sf.time_key = dt.time_key
    JOIN dim_location dl ON sf.location_key = dl.location_key
    GROUP BY dt.year, dt.quarter, dl.country, sf.source_system
  "
  dbExecute(analyticsDbCon, quarterQuery)
  
  # Aggregation query for year level
  yearQuery <- "
    INSERT INTO time_agg_facts (
      time_key, time_level, country, source_system,
      total_revenue, avg_revenue_per_transaction, 
      total_units_sold, avg_units_per_transaction,
      min_units_per_transaction, max_units_per_transaction,
      min_revenue_per_transaction, max_revenue_per_transaction,
      customer_count, transaction_count
    )
    SELECT 
      dt.year AS time_key,
      'YEAR' AS time_level,
      dl.country,
      sf.source_system,
      SUM(sf.amount) AS total_revenue,
      AVG(sf.amount) AS avg_revenue_per_transaction,
      SUM(sf.quantity) AS total_units_sold,
      AVG(sf.quantity) AS avg_units_per_transaction,
      MIN(sf.quantity) AS min_units_per_transaction,
      MAX(sf.quantity) AS max_units_per_transaction,
      MIN(sf.amount) AS min_revenue_per_transaction,
      MAX(sf.amount) AS max_revenue_per_transaction,
      COUNT(DISTINCT sf.customer_key) AS customer_count,
      COUNT(*) AS transaction_count
    FROM sales_facts sf
    JOIN dim_time dt ON sf.time_key = dt.time_key
    JOIN dim_location dl ON sf.location_key = dl.location_key
    GROUP BY dt.year, dl.country, sf.source_system
  "
  dbExecute(analyticsDbCon, yearQuery)
  
  cat("ETL Successful: time_agg_facts table loaded into analytical db.\n")
}

# Create Lookup Mappings
createLookupMappings <- function(mysqlCon) {
  cat("Creating lookup mappings...\n")
  
  # Customer mappings
  customerMapping <- dbGetQuery(mysqlCon, 
                                "SELECT customer_key, source_customer_id, source_system FROM dim_customer")
  customerLookup <- setNames(
    as.numeric(customerMapping$customer_key), 
    trimws(paste(customerMapping$source_customer_id, customerMapping$source_system, sep = "-"))
  )
  
  # Product mappings
  productMapping <- dbGetQuery(mysqlCon, 
                               "SELECT product_key, source_product_id, source_system FROM dim_product")
  productLookup <- setNames(
    as.numeric(productMapping$product_key), 
    trimws(paste(productMapping$source_product_id, productMapping$source_system, sep = "-"))
  )
  
  # Location mappings
  locationMapping <- dbGetQuery(mysqlCon, 
                                "SELECT location_key, country, source_system, source_id FROM dim_location")
  locationLookup <- setNames(
    as.numeric(locationMapping$location_key),
    trimws(paste(locationMapping$country, locationMapping$source_system, sep = "-"))
  )
  
  return(list(
    customerLookup = customerLookup,
    productLookup = productLookup,
    locationLookup = locationLookup
  ))
}

verifyDimensionTableLoading <- function(analyticsDbCon) {
  cat("Verify dimension table loading: \n")
  for (table in c("dim_customer", "dim_product", "dim_location", "dim_time")) {
    count <- dbGetQuery(analyticsDbCon, paste0("SELECT COUNT(*) AS count FROM ", table))
    cat(table, ": ", count$count, "\n")
  }
}

verifyFactTableLoading <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  cat("\n Verify fact table loading: \n")
  
  # Get distinct countries from source databases
  filmCountries <- dbGetQuery(filmSalesDbCon, "SELECT DISTINCT country FROM country")
  musicCountries <- dbGetQuery(musicSalesDbCon, "SELECT DISTINCT Country AS country FROM customers WHERE Country IS NOT NULL")
  
  # Combine and deduplicate source countries
  combinedSourceCountries <- unique(rbind(filmCountries, musicCountries))
  
  # Get distinct countries from the datamart
  dwCountries <- dbGetQuery(analyticsDbCon, "SELECT DISTINCT country FROM dim_location WHERE country IS NOT NULL")
  
  cat("[Validation] Countries in FILM + MUSIC sources:", nrow(combinedSourceCountries), "\n")
  cat("[Validation] Countries in Data Warehouse:", nrow(dwCountries), "\n")
}

# Validate Fact Tables
validateFacts <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  # Revenue match - FILM
  filmRevenueSource <- dbGetQuery(filmSalesDbCon, "
    SELECT SUM(amount) AS total_revenue FROM payment
  ")
  filmRevenueDW <- dbGetQuery(analyticsDbCon, "
    SELECT SUM(amount) AS total_revenue FROM sales_facts WHERE source_system = 'FILM'
  ")
  cat("[Validation] Film Revenue (Source vs DW):", 
      round(filmRevenueSource$total_revenue, 2), "vs", round(filmRevenueDW$total_revenue, 2), "\n")
  
  # Revenue match - MUSIC
  musicRevenueSource <- dbGetQuery(musicSalesDbCon, "
    SELECT SUM(UnitPrice * Quantity) AS total_revenue FROM invoice_items
  ")
  musicRevenueDW <- dbGetQuery(analyticsDbCon, "
    SELECT SUM(amount) AS total_revenue FROM sales_facts WHERE source_system = 'MUSIC'
  ")
  cat("[Validation] Music Revenue (Source vs DW):", 
      round(musicRevenueSource$total_revenue, 2), "vs", round(musicRevenueDW$total_revenue, 2), "\n")
}

# Execute Custom ETL Process
executeCustomETL <- function(filmSalesDbCon, musicSalesDbCon, analyticsDbCon) {
  cat("Executing ETL process...\n")
  
  # perform ETL for dimension tables
  locationDataETL(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  customerDataETL(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  productDataETL(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  timeDataETL(analyticsDbCon)
  
  # create lookup mappings for dimension keys
  lookups <- createLookupMappings(analyticsDbCon)
  
  # perform ETL for fact tables
  salesDataETL(filmSalesDbCon, musicSalesDbCon, analyticsDbCon, lookups)
  timeAggregatedFactsETL(analyticsDbCon)
  printLine()
  
  # Validation
  cat("Validating...\n")
  verifyDimensionTableLoading(analyticsDbCon)
  verifyFactTableLoading(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  validateFacts(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  printLine()
}

# Main Method
main <- function() {
  # required packages
  packages <- c("RMySQL", "DBI", "RSQLite")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to databases
  analyticsDbCon <- connectToMySQLDatabase()
  filmSalesDbCon <- connectToSQLiteDb("film-sales.db")
  musicSalesDbCon <- connectToSQLiteDb("music-sales.db")
  
  # execute ETL
  executeCustomETL(filmSalesDbCon, musicSalesDbCon, analyticsDbCon)
  
  # disconnect from database
  dbDisconnect(analyticsDbCon)
}

# execute the script
main()
