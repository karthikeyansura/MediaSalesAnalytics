#*****************************************
#* Title: "Extract, Transform and Load Data for Media Distributors Analytics"
#* Author: "Your Name"
#* Date: "Spring 2025"
#*****************************************

#*****************************************
#* Install and Load Required Packages
#*****************************************
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

loadRequiredPackages <- function(packages) {
  for (package in packages) {
    suppressMessages({
      library(package, character.only = TRUE)
    })
  }
}

#*****************************************
#* Database Connection Functions
#*****************************************
connectToMySQLDatabase <- function() {
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_4zUOM58G58RIBn3nQqg"
  dbHost <- "dbserver-cs5200-media-sales-analytics.b.aivencloud.com"
  dbPort <- 17041
  
  # Close any existing MySQL connections
  currentOpenConnections <- dbListConnections(MySQL())
  if (length(currentOpenConnections) > 0) {
    for (conn in currentOpenConnections) {
      dbDisconnect(conn)
    }
  }
  
  dbCon <- dbConnect(
    RMySQL::MySQL(),
    user = dbUser,
    password = dbPassword,
    dbname = dbName,
    host = dbHost,
    port = dbPort
  )
  
  cat("[INFO] Connected to MySQL database successfully.\n")
  return(dbCon)
}

connectToSourceDatabases <- function() {
  # Connect to Film database
  filmCon <- dbConnect(RSQLite::SQLite(), "film-sales.db")
  cat("[INFO] Connected to Film SQLite database successfully.\n")
  
  # Connect to Music database
  musicCon <- dbConnect(RSQLite::SQLite(), "music-sales.db")
  cat("[INFO] Connected to Music SQLite database successfully.\n")
  
  return(list(filmCon = filmCon, musicCon = musicCon))
}

#*****************************************
#* Execute SQL Functions
#*****************************************
executeQuery <- function(dbCon, sqlQuery, silent = TRUE) {
  tryCatch({
    dbGetQuery(dbCon, sqlQuery)
  }, error = function(err) {
    if (!silent) cat("[ERROR] SQL query failed:", err$message, "\n")
    return(NULL)
  })
}

executeSQL <- function(dbCon, sqlStatement, silent = TRUE) {
  tryCatch({
    dbExecute(dbCon, sqlStatement)
  }, error = function(err) {
    if (!silent) cat("[ERROR] SQL execution failed:", err$message, "\n")
    return(NULL)
  })
}

#*****************************************
#* Check Required Tables
#*****************************************
checkRequiredTables <- function(dbCon) {
  cat("[INFO] Checking for required tables...\n")
  
  requiredTables <- c(
    "dim_time", "dim_location", "dim_product_type", "dim_product", "dim_customer",
    "fact_sales", "fact_sales_obt", "fact_time_agg"
  )
  
  for (table in requiredTables) {
    tableExistsQuery <- sprintf("
      SELECT COUNT(*) as count 
      FROM information_schema.tables 
      WHERE table_schema = DATABASE() 
      AND table_name = '%s'
    ", table)
    
    result <- executeQuery(dbCon, tableExistsQuery)
    
    if (is.null(result) || result$count[1] == 0) {
      cat("[ERROR] Required table", table, "does not exist. Please run createStarSchema.R first.\n")
      return(FALSE)
    }
  }
  
  return(TRUE)
}

#*****************************************
#* Load Time Dimension
#*****************************************
loadTimeDimension <- function(mysqlCon, filmCon, musicCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM dim_time")
  
  # Extract film payment dates
  filmDates <- executeQuery(filmCon, "SELECT DISTINCT date(payment_date) as date_value FROM payment")
  
  # Extract music invoice dates
  musicDates <- executeQuery(musicCon, "SELECT DISTINCT date(InvoiceDate) as date_value FROM invoices")
  
  # Combine and deduplicate dates
  allDates <- unique(c(filmDates$date_value, musicDates$date_value))
  allDates <- allDates[!is.na(allDates)]
  
  # Prepare insert values for batch processing
  valuesList <- character()
  
  for (dateStr in allDates) {
    date <- as.Date(dateStr)
    if (!is.na(date)) {
      timeKey <- as.numeric(format(date, "%Y%m%d"))
      dayOfWeek <- weekdays(date)
      dayNum <- as.numeric(format(date, "%d"))
      dayOfYear <- as.numeric(format(date, "%j"))
      monthNum <- as.numeric(format(date, "%m"))
      monthName <- months(date)
      quarter <- ceiling(monthNum / 3)
      year <- as.numeric(format(date, "%Y"))
      isWeekend <- (weekdays(date) %in% c("Saturday", "Sunday"))
      isHoliday <- FALSE
      
      valuesList <- c(valuesList, 
                      sprintf("(%d, '%s', '%s', %d, %d, %d, '%s', %d, %d, %d, %d)",
                              timeKey, date, dayOfWeek, dayNum, dayOfYear, 
                              monthNum, monthName, quarter, year, 
                              as.numeric(isWeekend), as.numeric(isHoliday)))
    }
  }
  
  # Insert in batches
  batchSize <- 50
  totalValues <- length(valuesList)
  
  for (i in seq(1, totalValues, by = batchSize)) {
    end <- min(i + batchSize - 1, totalValues)
    batch <- valuesList[i:end]
    
    if (length(batch) > 0) {
      insertSQL <- paste0("INSERT INTO dim_time (time_key, full_date, day_of_week, day_num_in_month, ",
                          "day_num_in_year, month_num, month_name, quarter, year, weekend_flag, holiday_flag) VALUES ", 
                          paste(batch, collapse = ", "))
      executeSQL(mysqlCon, insertSQL)
    }
  }
  
  # Count loaded records
  countResult <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_time")
  cat("[INFO] Loaded time dimension records: ", countResult$count, "\n")
}

#*****************************************
#* Load Location Dimension
#*****************************************
loadLocationDimension <- function(mysqlCon, filmCon, musicCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM dim_location")
  
  # Extract film countries
  filmCountries <- executeQuery(filmCon, "SELECT country_id as source_id, country FROM country")
  filmCountries$source_system <- "FILM"
  
  # Extract music countries
  musicCountries <- executeQuery(musicCon, "
    SELECT NULL as source_id, Country as country
    FROM customers 
    WHERE Country IS NOT NULL AND Country != ''
    UNION
    SELECT NULL as source_id, BillingCountry as country
    FROM invoices 
    WHERE BillingCountry IS NOT NULL AND BillingCountry != ''
  ")
  musicCountries$source_system <- "MUSIC"
  
  # Combine and deduplicate
  allCountries <- rbind(filmCountries, musicCountries)
  uniqueCountries <- allCountries[!duplicated(allCountries$country), ]
  
  # Assign regions
  uniqueCountries$region <- "Other"
  
  # North America
  naCountries <- c("United States", "Canada", "Mexico")
  uniqueCountries$region[uniqueCountries$country %in% naCountries] <- "North America"
  
  # Europe
  euCountries <- c("United Kingdom", "France", "Germany", "Italy", "Spain", "Portugal", 
                   "Netherlands", "Belgium", "Switzerland", "Austria", "Denmark", "Norway", 
                   "Sweden", "Finland", "Ireland", "Greece", "Poland", "Czech Republic")
  uniqueCountries$region[uniqueCountries$country %in% euCountries] <- "Europe"
  
  # Asia
  asiaCountries <- c("China", "Japan", "India", "South Korea", "Thailand", "Indonesia",
                     "Philippines", "Vietnam", "Malaysia", "Singapore")
  uniqueCountries$region[uniqueCountries$country %in% asiaCountries] <- "Asia"
  
  # Insert records
  for (i in 1:nrow(uniqueCountries)) {
    sourceId <- uniqueCountries$source_id[i]
    if (is.na(sourceId)) sourceId <- "NULL" else sourceId <- paste0("'", sourceId, "'")
    
    country <- gsub("'", "''", uniqueCountries$country[i])
    sourceSystem <- uniqueCountries$source_system[i]
    region <- uniqueCountries$region[i]
    
    insertSQL <- sprintf("INSERT INTO dim_location (source_id, source_system, country, region) VALUES (%s, '%s', '%s', '%s')",
                         sourceId, sourceSystem, country, region)
    executeSQL(mysqlCon, insertSQL)
  }
  
  # Count loaded records
  countResult <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_location")
  cat("[INFO] Loaded location dimension records: ", countResult$count, "\n")
}

#*****************************************
#* Load Customer Dimension
#*****************************************
loadCustomerDimension <- function(mysqlCon, filmCon, musicCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM dim_customer")
  
  # Get location keys for lookup
  locationKeys <- executeQuery(mysqlCon, "SELECT location_key, country, source_system FROM dim_location")
  
  # Extract film customers
  filmCustomers <- executeQuery(filmCon, "
    SELECT 
      c.customer_id, 
      c.first_name, 
      c.last_name, 
      c.email,
      c.active,
      c.create_date,
      co.country
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
  ")
  filmCustomers$source_system <- "FILM"
  
  # Extract music customers
  musicCustomers <- executeQuery(musicCon, "
    SELECT 
      CustomerId as customer_id, 
      FirstName as first_name, 
      LastName as last_name, 
      Email as email,
      1 as active,
      NULL as create_date,
      Country as country
    FROM customers
    WHERE Country IS NOT NULL
  ")
  musicCustomers$source_system <- "MUSIC"
  
  # Process film customers
  filmValues <- character()
  for (i in 1:nrow(filmCustomers)) {
    customerId <- filmCustomers$customer_id[i]
    firstName <- gsub("'", "''", filmCustomers$first_name[i])
    lastName <- gsub("'", "''", filmCustomers$last_name[i])
    email <- ifelse(is.na(filmCustomers$email[i]), "NULL", 
                    paste0("'", gsub("'", "''", filmCustomers$email[i]), "'"))
    active <- ifelse(is.na(filmCustomers$active[i]), "NULL", filmCustomers$active[i])
    createDate <- ifelse(is.na(filmCustomers$create_date[i]), "NULL", 
                         paste0("'", filmCustomers$create_date[i], "'"))
    country <- filmCustomers$country[i]
    sourceSystem <- filmCustomers$source_system[i]
    
    # Find location key
    locationKey <- locationKeys$location_key[
      locationKeys$country == country & locationKeys$source_system == sourceSystem]
    
    if (length(locationKey) > 0) {
      filmValues <- c(filmValues, 
                      sprintf("('%s', '%s', '%s', '%s', %s, %s, %d, %s, CURRENT_TIMESTAMP)",
                              customerId, sourceSystem, firstName, lastName, 
                              email, active, locationKey[1], createDate))
    }
  }
  
  # Process music customers
  musicValues <- character()
  for (i in 1:nrow(musicCustomers)) {
    customerId <- musicCustomers$customer_id[i]
    firstName <- gsub("'", "''", musicCustomers$first_name[i])
    lastName <- gsub("'", "''", musicCustomers$last_name[i])
    email <- ifelse(is.na(musicCustomers$email[i]), "NULL", 
                    paste0("'", gsub("'", "''", musicCustomers$email[i]), "'"))
    active <- ifelse(is.na(musicCustomers$active[i]), "NULL", musicCustomers$active[i])
    createDate <- ifelse(is.na(musicCustomers$create_date[i]), "NULL", 
                         paste0("'", musicCustomers$create_date[i], "'"))
    country <- musicCustomers$country[i]
    sourceSystem <- musicCustomers$source_system[i]
    
    # Find location key
    locationKey <- locationKeys$location_key[
      locationKeys$country == country & locationKeys$source_system == sourceSystem]
    
    if (length(locationKey) > 0) {
      musicValues <- c(musicValues, 
                       sprintf("('%s', '%s', '%s', '%s', %s, %s, %d, %s, CURRENT_TIMESTAMP)",
                               customerId, sourceSystem, firstName, lastName, 
                               email, active, locationKey[1], createDate))
    }
  }
  
  # Insert film customers in batches
  if (length(filmValues) > 0) {
    for (i in seq(1, length(filmValues), by = 50)) {
      end <- min(i + 49, length(filmValues))
      insertSQL <- paste0("INSERT INTO dim_customer (source_customer_id, source_system, first_name, last_name, ",
                          "email, active, location_key, create_date, last_update) VALUES ", 
                          paste(filmValues[i:end], collapse = ", "))
      executeSQL(mysqlCon, insertSQL)
    }
  }
  
  # Insert music customers in batches
  if (length(musicValues) > 0) {
    for (i in seq(1, length(musicValues), by = 50)) {
      end <- min(i + 49, length(musicValues))
      insertSQL <- paste0("INSERT INTO dim_customer (source_customer_id, source_system, first_name, last_name, ",
                          "email, active, location_key, create_date, last_update) VALUES ", 
                          paste(musicValues[i:end], collapse = ", "))
      executeSQL(mysqlCon, insertSQL)
    }
  }
  
  # Count loaded records
  filmCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_customer WHERE source_system = 'FILM'")
  musicCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_customer WHERE source_system = 'MUSIC'")
  cat("[INFO] Loaded customer dimension records: ", filmCount$count + musicCount$count, "\n")
}

#*****************************************
#* Load Product Dimension
#*****************************************
loadProductDimension <- function(mysqlCon, filmCon, musicCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM dim_product")
  
  # Extract film products
  filmProducts <- executeQuery(filmCon, "
    SELECT 
      f.film_id as product_id,
      f.title,
      c.name as category,
      NULL as artist,
      f.release_year,
      l.name as language,
      NULL as media_type,
      f.rental_rate as unit_price,
      f.length as duration,
      f.last_update
    FROM film f
    LEFT JOIN film_category fc ON f.film_id = fc.film_id
    LEFT JOIN category c ON fc.category_id = c.category_id
    LEFT JOIN language l ON f.language_id = l.language_id
  ")
  filmProducts$source_system <- "FILM"
  filmProducts$product_type_key <- 1
  
  # Extract music products - fixed query without ReleaseDate
  musicProducts <- executeQuery(musicCon, "
    SELECT 
      t.TrackId as product_id,
      t.Name as title,
      g.Name as category,
      ar.Name as artist,
      NULL as release_year,
      NULL as language,
      mt.Name as media_type,
      t.UnitPrice as unit_price,
      t.Milliseconds / 1000 as duration,
      NULL as last_update
    FROM tracks t
    LEFT JOIN albums al ON t.AlbumId = al.AlbumId
    LEFT JOIN artists ar ON al.ArtistId = ar.ArtistId
    LEFT JOIN genres g ON t.GenreId = g.GenreId
    LEFT JOIN media_types mt ON t.MediaTypeId = mt.MediaTypeId
  ")
  musicProducts$source_system <- "MUSIC"
  musicProducts$product_type_key <- 2
  
  # Insert in batches
  insertProductBatch <- function(products, batchSize = 50) {
    for (i in seq(1, nrow(products), by = batchSize)) {
      end <- min(i + batchSize - 1, nrow(products))
      batch <- products[i:end, ]
      
      valuesList <- character()
      
      for (j in 1:nrow(batch)) {
        productId <- batch$product_id[j]
        title <- gsub("'", "''", batch$title[j])
        category <- ifelse(is.na(batch$category[j]), "NULL", 
                           paste0("'", gsub("'", "''", batch$category[j]), "'"))
        artist <- ifelse(is.na(batch$artist[j]), "NULL", 
                         paste0("'", gsub("'", "''", batch$artist[j]), "'"))
        releaseYear <- ifelse(is.na(batch$release_year[j]), "NULL", batch$release_year[j])
        language <- ifelse(is.na(batch$language[j]), "NULL", 
                           paste0("'", gsub("'", "''", batch$language[j]), "'"))
        mediaType <- ifelse(is.na(batch$media_type[j]), "NULL", 
                            paste0("'", gsub("'", "''", batch$media_type[j]), "'"))
        unitPrice <- ifelse(is.na(batch$unit_price[j]), "NULL", batch$unit_price[j])
        duration <- ifelse(is.na(batch$duration[j]), "NULL", batch$duration[j])
        sourceSystem <- batch$source_system[j]
        productTypeKey <- batch$product_type_key[j]
        
        valuesList <- c(valuesList, 
                        sprintf("('%s', '%s', %d, '%s', %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)",
                                productId, sourceSystem, productTypeKey, title, 
                                category, artist, releaseYear, language, 
                                mediaType, unitPrice, duration))
      }
      
      if (length(valuesList) > 0) {
        insertSQL <- paste0("INSERT INTO dim_product (source_product_id, source_system, product_type_key, ",
                            "title, category_genre, artist_actor, release_year, language, media_type, ",
                            "unit_price, duration, last_update) VALUES ", 
                            paste(valuesList, collapse = ", "))
        executeSQL(mysqlCon, insertSQL)
      }
    }
  }
  
  # Insert film products
  insertProductBatch(filmProducts)
  
  # Insert music products
  insertProductBatch(musicProducts)
  
  # Count loaded records
  productCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_product")
  cat("[INFO] Loaded product dimension records: ", productCount$count, "\n")
}

#*****************************************
#* Load Sales Facts
#*****************************************
loadSalesFacts <- function(mysqlCon, filmCon, musicCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM fact_sales")
  executeSQL(mysqlCon, "DELETE FROM fact_sales_obt")
  
  # Get dimension lookups
  timeKeys <- executeQuery(mysqlCon, "SELECT time_key, full_date FROM dim_time")
  customerKeys <- executeQuery(mysqlCon, "SELECT customer_key, source_customer_id, source_system FROM dim_customer")
  productKeys <- executeQuery(mysqlCon, "SELECT product_key, source_product_id, source_system FROM dim_product")
  locationKeys <- executeQuery(mysqlCon, "SELECT location_key, country, source_system FROM dim_location")
  
  # Process film sales
  offset <- 0
  batchSize <- 500
  filmCount <- 0
  
  repeat {
    # Get batch of film sales
    filmSales <- executeQuery(filmCon, sprintf("
      SELECT 
        p.payment_id,
        date(p.payment_date) as payment_date,
        p.payment_date as transaction_datetime,
        p.customer_id,
        i.film_id,
        co.country,
        p.amount,
        1 as quantity
      FROM payment p
      LEFT JOIN rental r ON p.rental_id = r.rental_id
      LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
      JOIN customer c ON p.customer_id = c.customer_id
      JOIN address a ON c.address_id = a.address_id
      JOIN city ci ON a.city_id = ci.city_id
      JOIN country co ON ci.country_id = co.country_id
      ORDER BY p.payment_id
      LIMIT %d OFFSET %d
    ", batchSize, offset))
    
    # If no more records, break
    if (nrow(filmSales) == 0) break
    
    # Process batch
    valuesFact <- character()
    valuesOBT <- character()
    
    for (i in 1:nrow(filmSales)) {
      paymentDate <- as.Date(filmSales$payment_date[i])
      customerId <- filmSales$customer_id[i]
      productId <- filmSales$film_id[i]
      country <- filmSales$country[i]
      
      # Skip if missing data
      if (is.na(paymentDate) || is.na(customerId) || is.na(country)) next
      
      # Get dimension keys
      timeKey <- timeKeys$time_key[timeKeys$full_date == paymentDate]
      customerKey <- customerKeys$customer_key[
        customerKeys$source_customer_id == customerId & customerKeys$source_system == "FILM"]
      productKey <- productKeys$product_key[
        productKeys$source_product_id == productId & productKeys$source_system == "FILM"]
      locationKey <- locationKeys$location_key[
        locationKeys$country == country & locationKeys$source_system == "FILM"]
      
      # Skip if missing keys
      if (length(timeKey) == 0 || length(customerKey) == 0 || 
          length(locationKey) == 0) next
      
      # Skip if product doesn't exist
      if (length(productKey) == 0) {
        # For film sales we can use a NULL product ID
        if (!is.na(productId)) {
          productKey <- productKeys$product_key[
            productKeys$source_system == "FILM"][1]
        } else {
          next
        }
      }
      
      # Add to fact_sales values
      valuesFact <- c(valuesFact, 
                      sprintf("(%d, %d, %d, %d, '%s', '%s', '%s', %d, %f)",
                              timeKey[1], customerKey[1], productKey[1], locationKey[1], 
                              "FILM", filmSales$payment_id[i], filmSales$transaction_datetime[i],
                              filmSales$quantity[i], filmSales$amount[i]))
      
      # Add to fact_sales_obt values
      valuesOBT <- c(valuesOBT,
                     sprintf("('%s', '%s', '%s', %d, %d, %d, '%s', 'Customer', '%s', 'Product', '%s', '%s', 'Region', %d, %f, '%s')",
                             paymentDate, 
                             weekdays(paymentDate),
                             months(paymentDate), 
                             as.numeric(format(paymentDate, "%m")),
                             ceiling(as.numeric(format(paymentDate, "%m")) / 3),
                             as.numeric(format(paymentDate, "%Y")),
                             customerId, 
                             productId, 
                             "Film",
                             gsub("'", "''", country), 
                             filmSales$quantity[i], 
                             filmSales$amount[i], 
                             "FILM"))
    }
    
    # Insert batch
    if (length(valuesFact) > 0) {
      factSQL <- paste0("INSERT INTO fact_sales (time_key, customer_key, product_key, location_key, ",
                        "source_system, source_transaction_id, transaction_date, quantity, amount) VALUES ", 
                        paste(valuesFact, collapse = ", "))
      executeSQL(mysqlCon, factSQL)
      
      obtSQL <- paste0("INSERT INTO fact_sales_obt (transaction_date, day_of_week, month_name, ",
                       "month_num, quarter, year, customer_id, customer_name, product_id, ",
                       "product_title, product_type, country, region, quantity, amount, source_system) VALUES ", 
                       paste(valuesOBT, collapse = ", "))
      executeSQL(mysqlCon, obtSQL)
      
      filmCount <- filmCount + length(valuesFact)
    }
    
    # Move to next batch
    offset <- offset + batchSize
  }
  
  # Process music sales
  offset <- 0
  musicCount <- 0
  
  repeat {
    # Get batch of music sales
    musicSales <- executeQuery(musicCon, sprintf("
      SELECT 
        ii.InvoiceLineId as payment_id,
        date(i.InvoiceDate) as payment_date,
        i.InvoiceDate as transaction_datetime,
        i.CustomerId as customer_id,
        ii.TrackId as track_id,
        i.BillingCountry as country,
        ii.UnitPrice * ii.Quantity as amount,
        ii.Quantity as quantity
      FROM invoice_items ii
      JOIN invoices i ON ii.InvoiceId = i.InvoiceId
      ORDER BY ii.InvoiceLineId
      LIMIT %d OFFSET %d
    ", batchSize, offset))
    
    # If no more records, break
    if (nrow(musicSales) == 0) break
    
    # Process batch
    valuesFact <- character()
    valuesOBT <- character()
    
    for (i in 1:nrow(musicSales)) {
      paymentDate <- as.Date(musicSales$payment_date[i])
      customerId <- musicSales$customer_id[i]
      productId <- musicSales$track_id[i]
      country <- musicSales$country[i]
      
      # Skip if missing data
      if (is.na(paymentDate) || is.na(customerId) || is.na(country)) next
      
      # Get dimension keys
      timeKey <- timeKeys$time_key[timeKeys$full_date == paymentDate]
      customerKey <- customerKeys$customer_key[
        customerKeys$source_customer_id == customerId & customerKeys$source_system == "MUSIC"]
      productKey <- productKeys$product_key[
        productKeys$source_product_id == productId & productKeys$source_system == "MUSIC"]
      locationKey <- locationKeys$location_key[
        locationKeys$country == country & locationKeys$source_system == "MUSIC"]
      
      # Skip if missing keys
      if (length(timeKey) == 0 || length(customerKey) == 0 || 
          length(locationKey) == 0 || length(productKey) == 0) next
      
      # Add to fact_sales values
      valuesFact <- c(valuesFact, 
                      sprintf("(%d, %d, %d, %d, '%s', '%s', '%s', %d, %f)",
                              timeKey[1], customerKey[1], productKey[1], locationKey[1], 
                              "MUSIC", musicSales$payment_id[i], musicSales$transaction_datetime[i],
                              musicSales$quantity[i], musicSales$amount[i]))
      
      # Add to fact_sales_obt values
      valuesOBT <- c(valuesOBT,
                     sprintf("('%s', '%s', '%s', %d, %d, %d, '%s', 'Customer', '%s', 'Product', '%s', '%s', 'Region', %d, %f, '%s')",
                             paymentDate, 
                             weekdays(paymentDate),
                             months(paymentDate), 
                             as.numeric(format(paymentDate, "%m")),
                             ceiling(as.numeric(format(paymentDate, "%m")) / 3),
                             as.numeric(format(paymentDate, "%Y")),
                             customerId, 
                             productId, 
                             "Music",
                             gsub("'", "''", country), 
                             musicSales$quantity[i], 
                             musicSales$amount[i], 
                             "MUSIC"))
    }
    
    # Insert batch
    if (length(valuesFact) > 0) {
      factSQL <- paste0("INSERT INTO fact_sales (time_key, customer_key, product_key, location_key, ",
                        "source_system, source_transaction_id, transaction_date, quantity, amount) VALUES ", 
                        paste(valuesFact, collapse = ", "))
      executeSQL(mysqlCon, factSQL)
      
      obtSQL <- paste0("INSERT INTO fact_sales_obt (transaction_date, day_of_week, month_name, ",
                       "month_num, quarter, year, customer_id, customer_name, product_id, ",
                       "product_title, product_type, country, region, quantity, amount, source_system) VALUES ", 
                       paste(valuesOBT, collapse = ", "))
      executeSQL(mysqlCon, obtSQL)
      
      musicCount <- musicCount + length(valuesFact)
    }
    
    # Move to next batch
    offset <- offset + batchSize
  }
  
  # Report total loaded
  cat("[INFO] Loaded sales facts records: ", filmCount + musicCount, "\n")
}

#*****************************************
#* Load Time Aggregation Facts
#*****************************************
loadTimeAggFacts <- function(mysqlCon) {
  # Clear existing data
  executeSQL(mysqlCon, "DELETE FROM fact_time_agg")
  
  # Insert aggregates for each time level
  timeLevels <- list(
    list(id = 1, name = "DAY"),
    list(id = 2, name = "MONTH", groupBy = "dt.year, dt.month_num"),
    list(id = 3, name = "QUARTER", groupBy = "dt.year, dt.quarter"),
    list(id = 4, name = "YEAR", groupBy = "dt.year")
  )
  
  # Load aggregates for each time level
  for (level in timeLevels) {
    levelName <- level$name
    levelId <- level$id
    groupBy <- if (levelName == "DAY") "dt.time_key" else level$groupBy
    
    aggregateSQL <- sprintf("
      INSERT INTO fact_time_agg (
        time_key, time_level_id, time_level, country, product_type, source_system,
        total_revenue, avg_revenue_per_transaction, total_units_sold, 
        avg_units_per_transaction, min_units_per_transaction, max_units_per_transaction,
        min_revenue_per_transaction, max_revenue_per_transaction, 
        customer_count, transaction_count
      )
      SELECT 
        MIN(dt.time_key) as time_key, 
        %d as time_level_id,
        '%s' as time_level, 
        dl.country, 
        CASE dp.product_type_key 
          WHEN 1 THEN 'Film'
          WHEN 2 THEN 'Music'
          ELSE 'Unknown'
        END as product_type,
        fs.source_system,
        SUM(fs.amount) as total_revenue, 
        AVG(fs.amount) as avg_revenue_per_transaction, 
        SUM(fs.quantity) as total_units_sold,
        AVG(fs.quantity) as avg_units_per_transaction, 
        MIN(fs.quantity) as min_units_per_transaction, 
        MAX(fs.quantity) as max_units_per_transaction,
        MIN(fs.amount) as min_revenue_per_transaction, 
        MAX(fs.amount) as max_revenue_per_transaction,
        COUNT(DISTINCT fs.customer_key) as customer_count, 
        COUNT(*) as transaction_count
      FROM 
        fact_sales fs
      JOIN dim_time dt ON fs.time_key = dt.time_key
      JOIN dim_location dl ON fs.location_key = dl.location_key
      JOIN dim_product dp ON fs.product_key = dp.product_key
      GROUP BY %s, dl.country, dp.product_type_key, fs.source_system
    ", levelId, levelName, groupBy)
    
    executeSQL(mysqlCon, aggregateSQL)
  }
  
  # Count loaded records
  aggCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM fact_time_agg")
  cat("[INFO] Loaded time aggregation fact records: ", aggCount$count, "\n")
}

#*****************************************
#* Main function
#*****************************************
main <- function() {
  # Install and load required packages
  installPackagesOnDemand(c("RMySQL", "RSQLite", "DBI"))
  loadRequiredPackages(c("RMySQL", "RSQLite", "DBI"))
  
  cat("[INFO] Loading Media Distributors analytics data...\n")
  
  # Connect to databases
  mysqlCon <- connectToMySQLDatabase()
  
  # Check required tables
  if (!checkRequiredTables(mysqlCon)) {
    cat("[ERROR] Star schema tables not found. Run createStarSchema.R first.\n")
    dbDisconnect(mysqlCon)
    return()
  }
  
  # Connect to source databases
  sources <- connectToSourceDatabases()
  filmCon <- sources$filmCon
  musicCon <- sources$musicCon
  
  cat("[INFO] Successfully connected to all databases\n")
  
  # Execute ETL process in transaction
  tryCatch({
    executeSQL(mysqlCon, "START TRANSACTION")
    cat("[INFO] Starting ETL process...\n")
    
    # Load dimensions
    loadTimeDimension(mysqlCon, filmCon, musicCon)
    loadLocationDimension(mysqlCon, filmCon, musicCon)
    loadCustomerDimension(mysqlCon, filmCon, musicCon)
    loadProductDimension(mysqlCon, filmCon, musicCon)
    
    # Load facts
    loadSalesFacts(mysqlCon, filmCon, musicCon)
    loadTimeAggFacts(mysqlCon)
    
    # Commit transaction
    executeSQL(mysqlCon, "COMMIT")
    cat("[INFO] ETL process completed successfully.\n")
    
  }, error = function(err) {
    executeSQL(mysqlCon, "ROLLBACK")
    cat("[ERROR] ETL process failed:", err$message, "\n")
  })
  
  # Close connections
  dbDisconnect(mysqlCon)
  dbDisconnect(filmCon)
  dbDisconnect(musicCon)
}

# Execute the main function
main()