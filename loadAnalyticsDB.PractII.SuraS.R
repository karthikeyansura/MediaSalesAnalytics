# Title: Part C / Extract, Transform and Load Data
# Course Name: CS5200 Database Management Systems
# Author: Your Name
# Semester: Spring 2025

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
connectToMySQL <- function() {
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

# Function to connect to the Film Sales SQLite database
connectToFilmDB <- function() {
  con <- tryCatch(
    {
      dbConnect(RSQLite::SQLite(), "film-sales.db")
    },
    error = function(e) {
      return(e$message)
    }
  )
  return(con)
}

# Function to connect to the Music Sales SQLite database
connectToMusicDB <- function() {
  con <- tryCatch(
    {
      dbConnect(RSQLite::SQLite(), "music-sales.db")
    },
    error = function(e) {
      return(e$message)
    }
  )
  return(con)
}

# Function to execute SQL query with error handling
executeQuery <- function(con, sqlQuery, silent = FALSE) {
  result <- tryCatch(
    {
      if (!silent) cat("[SQL] Executing query:", sqlQuery, "\n")
      dbGetQuery(con, sqlQuery)
    },
    error = function(e) {
      cat("[ERROR] SQL query failed:", e$message, "\n")
      cat("[SQL] Failed query:", sqlQuery, "\n")
      return(NULL)
    }
  )
  if (!is.null(result) && !silent) {
    cat("[SQL] Query successful. Rows returned:", nrow(result), "\n")
  }
  return(result)
}

# Function to execute SQL statement with error handling
executeSQL <- function(con, sqlStatement, silent = FALSE) {
  result <- tryCatch(
    {
      if (!silent) cat("[SQL] Executing:", sqlStatement, "\n")
      dbExecute(con, sqlStatement)
    },
    error = function(e) {
      cat("[ERROR] SQL execution failed:", e$message, "\n")
      cat("[SQL] Failed statement:", sqlStatement, "\n")
      return(NULL)
    }
  )
  if (!is.null(result) && !silent) {
    cat("[SQL] Execution successful. Rows affected:", result, "\n")
  }
  return(result)
}

# Function to check if required tables exist
checkRequiredTables <- function(con) {
  cat("[INFO] Checking for required tables...\n")
  
  requiredTables <- c("dim_time", "dim_country", "dim_product_type", 
                      "dim_customer", "fact_sales", "fact_sales_obt")
  
  allTablesExist <- TRUE
  
  for (table in requiredTables) {
    tableExistsQuery <- sprintf("
      SELECT COUNT(*) as count 
      FROM information_schema.tables 
      WHERE table_schema = 'defaultdb' 
      AND table_name = '%s'
    ", table)
    
    result <- executeQuery(con, tableExistsQuery, silent = TRUE)
    
    if (is.null(result) || result$count[1] == 0) {
      cat("[ERROR] Required table", table, "does not exist. Please run createStarSchema.R first.\n")
      allTablesExist <- FALSE
    }
  }
  
  return(allTablesExist)
}

# Function to extract unique dates from both databases and load into dim_time
loadTimeDimension <- function(filmCon, musicCon, mysqlCon) {
  cat("[INFO] Loading time dimension...\n")
  
  # Extract payment dates from Film Sales
  filmDates <- executeQuery(filmCon, "
    SELECT DISTINCT date(payment_date) as date_value 
    FROM payment 
    ORDER BY date_value
  ")
  
  # Extract invoice dates from Music Sales
  musicDates <- executeQuery(musicCon, "
    SELECT DISTINCT date(InvoiceDate) as date_value 
    FROM invoices 
    ORDER BY date_value
  ")
  
  # Combine all dates
  allDates <- unique(c(filmDates$date_value, musicDates$date_value))
  
  # Clear existing time dimension data
  executeSQL(mysqlCon, "DELETE FROM dim_time;")
  
  # Insert dates in batches (more efficient than row-by-row)
  batchSize <- 100
  totalDates <- length(allDates)
  
  for (i in seq(1, totalDates, by = batchSize)) {
    endIdx <- min(i + batchSize - 1, totalDates)
    batch <- allDates[i:endIdx]
    
    if (length(batch) > 0) {
      # Prepare batch insert values
      valuesList <- character(length(batch))
      
      for (j in 1:length(batch)) {
        date <- as.Date(batch[j])
        if (!is.na(date)) {
          timeKey <- as.numeric(format(date, "%Y%m%d"))
          dayOfWeek <- weekdays(date)
          dayOfMonth <- as.numeric(format(date, "%d"))
          monthName <- months(date)
          monthNumber <- as.numeric(format(date, "%m"))
          quarter <- ceiling(monthNumber / 3)
          year <- as.numeric(format(date, "%Y"))
          
          valuesList[j] <- sprintf("(%d, '%s', '%s', %d, '%s', %d, %d, %d)",
                                   timeKey, date, dayOfWeek, dayOfMonth, 
                                   monthName, monthNumber, quarter, year)
        }
      }
      
      # Execute batch insert
      insertSQL <- paste0("INSERT INTO dim_time (time_key, date, day_of_week, day_of_month, month_name, month_number, quarter, year) VALUES ", 
                          paste(valuesList, collapse = ", "))
      executeSQL(mysqlCon, insertSQL)
    }
  }
  
  # Verify loading
  countResult <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_time")
  cat("[INFO] Loaded", countResult$count, "time dimension records.\n")
}

# Function to extract countries from both databases and load into dim_country
loadCountryDimension <- function(filmCon, musicCon, mysqlCon) {
  cat("[INFO] Loading country dimension...\n")
  
  # Extract countries from Film Sales
  filmCountries <- executeQuery(filmCon, "
    SELECT DISTINCT country, country_id 
    FROM country 
    ORDER BY country
  ")
  
  # Extract countries from Music Sales
  musicCountries <- executeQuery(musicCon, "
    SELECT DISTINCT Country 
    FROM customers 
    WHERE Country IS NOT NULL AND Country != '' 
    UNION 
    SELECT DISTINCT BillingCountry 
    FROM invoices 
    WHERE BillingCountry IS NOT NULL AND BillingCountry != '' 
    ORDER BY Country
  ")
  
  # Clear existing country dimension data
  executeSQL(mysqlCon, "DELETE FROM dim_country;")
  
  # Combine all countries
  allCountries <- unique(c(filmCountries$country, musicCountries$Country))
  allCountries <- allCountries[!is.na(allCountries) & allCountries != ""]
  
  # Batch insert countries
  batchSize <- 50
  totalCountries <- length(allCountries)
  
  for (i in seq(1, totalCountries, by = batchSize)) {
    endIdx <- min(i + batchSize - 1, totalCountries)
    batch <- allCountries[i:endIdx]
    
    if (length(batch) > 0) {
      # Prepare batch insert values
      valuesList <- character(length(batch))
      
      for (j in 1:length(batch)) {
        country <- batch[j]
        valuesList[j] <- sprintf("('%s')", gsub("'", "''", country))
      }
      
      # Execute batch insert
      insertSQL <- paste0("INSERT INTO dim_country (country_name) VALUES ", 
                          paste(valuesList, collapse = ", "))
      executeSQL(mysqlCon, insertSQL)
    }
  }
  
  # Verify loading
  countResult <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_country")
  cat("[INFO] Loaded", countResult$count, "country dimension records.\n")
}

# Function to load all customer data with batch processing
loadCustomerDimension <- function(filmCon, musicCon, mysqlCon) {
  cat("[INFO] Loading customer dimension...\n")
  
  # Clear existing customer dimension data
  executeSQL(mysqlCon, "DELETE FROM dim_customer;")
  
  # Extract film customers with country information
  filmCustomers <- executeQuery(filmCon, "
    SELECT 
      c.customer_id, 
      c.first_name, 
      c.last_name, 
      c.email, 
      co.country 
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country co ON ci.country_id = co.country_id
  ")
  
  # Get all countries and their keys
  countryData <- executeQuery(mysqlCon, "SELECT country_key, country_name FROM dim_country")
  
  # Create a lookup table for country keys
  countryLookup <- setNames(countryData$country_key, countryData$country_name)
  
  # Process film customers in batches
  cat("[INFO] Loading film customers...\n")
  batchSize <- 50
  totalFilmCustomers <- nrow(filmCustomers)
  
  for (i in seq(1, totalFilmCustomers, by = batchSize)) {
    endIdx <- min(i + batchSize - 1, totalFilmCustomers)
    batch <- filmCustomers[i:endIdx, ]
    
    if (nrow(batch) > 0) {
      # Prepare batch insert values
      valuesList <- character(0)
      
      for (j in 1:nrow(batch)) {
        sourceId <- batch$customer_id[j]
        firstName <- batch$first_name[j]
        lastName <- batch$last_name[j]
        email <- batch$email[j]
        country <- batch$country[j]
        
        # Get country key from lookup
        countryKey <- countryLookup[country]
        
        if (!is.na(countryKey)) {
          valuesList <- c(valuesList, sprintf("(%d, %d, '%s', '%s', '%s', %d)",
                                              sourceId, 
                                              1, # product_type_key for film
                                              gsub("'", "''", firstName), 
                                              gsub("'", "''", lastName), 
                                              gsub("'", "''", email),
                                              countryKey))
        }
      }
      
      if (length(valuesList) > 0) {
        # Execute batch insert
        insertSQL <- paste0("INSERT INTO dim_customer (source_id, product_type_key, first_name, last_name, email, country_key) VALUES ", 
                            paste(valuesList, collapse = ", "))
        executeSQL(mysqlCon, insertSQL)
      }
    }
  }
  
  # Extract music customers with country information
  musicCustomers <- executeQuery(musicCon, "
    SELECT 
      CustomerId, 
      FirstName, 
      LastName, 
      Email, 
      Country 
    FROM customers
    WHERE Country IS NOT NULL
  ")
  
  # Process music customers in batches
  cat("[INFO] Loading music customers...\n")
  totalMusicCustomers <- nrow(musicCustomers)
  
  for (i in seq(1, totalMusicCustomers, by = batchSize)) {
    endIdx <- min(i + batchSize - 1, totalMusicCustomers)
    batch <- musicCustomers[i:endIdx, ]
    
    if (nrow(batch) > 0) {
      # Prepare batch insert values
      valuesList <- character(0)
      
      for (j in 1:nrow(batch)) {
        sourceId <- batch$CustomerId[j]
        firstName <- batch$FirstName[j]
        lastName <- batch$LastName[j]
        email <- batch$Email[j]
        country <- batch$Country[j]
        
        # Get country key from lookup
        countryKey <- countryLookup[country]
        
        if (!is.na(countryKey)) {
          valuesList <- c(valuesList, sprintf("(%d, %d, '%s', '%s', '%s', %d)",
                                              sourceId, 
                                              2, # product_type_key for music
                                              gsub("'", "''", firstName), 
                                              gsub("'", "''", lastName), 
                                              gsub("'", "''", email),
                                              countryKey))
        }
      }
      
      if (length(valuesList) > 0) {
        # Execute batch insert
        insertSQL <- paste0("INSERT INTO dim_customer (source_id, product_type_key, first_name, last_name, email, country_key) VALUES ", 
                            paste(valuesList, collapse = ", "))
        executeSQL(mysqlCon, insertSQL)
      }
    }
  }
  
  # Verify loading
  countResult <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_customer")
  cat("[INFO] Loaded", countResult$count, "customer dimension records.\n")
}

# Function to load film sales data into fact tables with batch inserts
loadFilmSales <- function(filmCon, mysqlCon) {
  cat("[INFO] Loading film sales facts...\n")
  
  # Get all dimension lookups to avoid repeated queries
  timeData <- executeQuery(mysqlCon, "SELECT time_key, date FROM dim_time")
  timeLookup <- setNames(timeData$time_key, as.character(timeData$date))
  
  countryData <- executeQuery(mysqlCon, "SELECT country_key, country_name FROM dim_country")
  countryLookup <- setNames(countryData$country_key, countryData$country_name)
  
  customerData <- executeQuery(mysqlCon, "SELECT customer_key, source_id, first_name, last_name, email, country_key, product_type_key FROM dim_customer WHERE product_type_key = 1")
  customerLookup <- setNames(customerData$customer_key, customerData$source_id)
  
  # Create a lookup dataframe for customer info
  customerInfoLookup <- data.frame(
    source_id = customerData$source_id,
    customer_key = customerData$customer_key,
    first_name = customerData$first_name,
    last_name = customerData$last_name,
    email = customerData$email,
    country_key = customerData$country_key,
    stringsAsFactors = FALSE
  )
  
  # Process film sales in batches, limiting records to prevent memory overload
  offset <- 0
  batchSize <- 1000
  hasMoreRows <- TRUE
  
  while (hasMoreRows) {
    # Extract film payment data with joins to get all needed information
    filmSalesSQL <- sprintf("
      SELECT 
        p.payment_id,
        date(p.payment_date) as payment_date,
        p.customer_id,
        co.country,
        p.amount,
        1 as units_sold  -- Assuming 1 unit per payment for films
      FROM payment p
      JOIN customer c ON p.customer_id = c.customer_id
      JOIN address a ON c.address_id = a.address_id
      JOIN city ci ON a.city_id = ci.city_id
      JOIN country co ON ci.country_id = co.country_id
      ORDER BY p.payment_id
      LIMIT %d OFFSET %d
    ", batchSize, offset)
    
    filmSales <- executeQuery(filmCon, filmSalesSQL)
    
    if (nrow(filmSales) == 0) {
      hasMoreRows <- FALSE
      next
    }
    
    cat("[INFO] Processing batch of", nrow(filmSales), "film sales records from offset", offset, "\n")
    
    # Prepare batch data for fact_sales and fact_sales_obt tables
    factBatchValues <- character(0)
    obtBatchValues <- character(0)
    
    for (i in 1:nrow(filmSales)) {
      paymentDate <- as.Date(filmSales$payment_date[i])
      customerId <- filmSales$customer_id[i]
      country <- filmSales$country[i]
      amount <- filmSales$amount[i]
      unitsSold <- filmSales$units_sold[i]
      
      # Skip if missing data
      if (is.na(paymentDate) || is.na(customerId) || is.na(country)) {
        next
      }
      
      # Get time key from lookup
      timeKey <- timeLookup[as.character(paymentDate)]
      if (is.na(timeKey)) {
        next
      }
      
      # Get country key from lookup
      countryKey <- countryLookup[country]
      if (is.na(countryKey)) {
        next
      }
      
      # Get customer key from lookup
      customerKey <- customerLookup[customerId]
      if (is.na(customerKey)) {
        next
      }
      
      # Add to fact_sales batch
      factBatchValues <- c(factBatchValues, 
                           sprintf("(%d, %d, %d, %d, %d, %f)",
                                   timeKey, customerKey, countryKey, 1, unitsSold, amount))
      
      # Get customer info
      customerInfo <- customerInfoLookup[customerInfoLookup$source_id == customerId, ]
      if (nrow(customerInfo) == 0) {
        next
      }
      
      # Get country name
      countryName <- countryData$country_name[countryData$country_key == countryKey]
      if (length(countryName) == 0) {
        next
      }
      
      # Calculate time dimension values
      dayOfWeek <- weekdays(paymentDate)
      dayOfMonth <- as.numeric(format(paymentDate, "%d"))
      monthName <- months(paymentDate)
      monthNumber <- as.numeric(format(paymentDate, "%m"))
      quarter <- ceiling(monthNumber / 3)
      year <- as.numeric(format(paymentDate, "%Y"))
      
      # Add to fact_sales_obt batch
      obtBatchValues <- c(obtBatchValues,
                          sprintf("('%s', '%s', %d, '%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', %d, %f)",
                                  paymentDate, 
                                  dayOfWeek, 
                                  dayOfMonth, 
                                  monthName, 
                                  monthNumber, 
                                  quarter, 
                                  year,
                                  customerId, 
                                  gsub("'", "''", customerInfo$first_name[1]), 
                                  gsub("'", "''", customerInfo$last_name[1]), 
                                  gsub("'", "''", customerInfo$email[1]),
                                  gsub("'", "''", countryName[1]), 
                                  "Film", 
                                  unitsSold, 
                                  amount))
    }
    
    # Insert fact_sales batch
    if (length(factBatchValues) > 0) {
      cat("[INFO] Inserting batch of", length(factBatchValues), "records into fact_sales\n")
      factInsertSQL <- paste0("INSERT INTO fact_sales (time_key, customer_key, country_key, product_type_key, units_sold, revenue) VALUES ", 
                              paste(factBatchValues, collapse = ", "))
      executeSQL(mysqlCon, factInsertSQL)
    }
    
    # Insert fact_sales_obt batch
    if (length(obtBatchValues) > 0) {
      cat("[INFO] Inserting batch of", length(obtBatchValues), "records into fact_sales_obt\n")
      obtInsertSQL <- paste0("INSERT INTO fact_sales_obt (date, day_of_week, day_of_month, month_name, month_number, quarter, year, customer_id, customer_first_name, customer_last_name, customer_email, country_name, product_type, units_sold, revenue) VALUES ", 
                             paste(obtBatchValues, collapse = ", "))
      executeSQL(mysqlCon, obtInsertSQL)
    }
    
    # Move to next batch
    offset <- offset + batchSize
    cat("[INFO] Completed processing", offset, "film sales records.\n")
  }
}

# Function to load music sales data into fact tables with batch inserts
loadMusicSales <- function(musicCon, mysqlCon) {
  cat("[INFO] Loading music sales facts...\n")
  
  # Get all dimension lookups to avoid repeated queries
  timeData <- executeQuery(mysqlCon, "SELECT time_key, date FROM dim_time")
  timeLookup <- setNames(timeData$time_key, as.character(timeData$date))
  
  countryData <- executeQuery(mysqlCon, "SELECT country_key, country_name FROM dim_country")
  countryLookup <- setNames(countryData$country_key, countryData$country_name)
  
  customerData <- executeQuery(mysqlCon, "SELECT customer_key, source_id, first_name, last_name, email, country_key, product_type_key FROM dim_customer WHERE product_type_key = 2")
  customerLookup <- setNames(customerData$customer_key, customerData$source_id)
  
  # Create a lookup dataframe for customer info
  customerInfoLookup <- data.frame(
    source_id = customerData$source_id,
    customer_key = customerData$customer_key,
    first_name = customerData$first_name,
    last_name = customerData$last_name,
    email = customerData$email,
    country_key = customerData$country_key,
    stringsAsFactors = FALSE
  )
  
  # Process music sales in batches, limiting records to prevent memory overload
  offset <- 0
  batchSize <- 1000
  hasMoreRows <- TRUE
  
  while (hasMoreRows) {
    # Extract music sales data with joins to get all needed information
    musicSalesSQL <- sprintf("
      SELECT 
        ii.InvoiceLineId,
        date(i.InvoiceDate) as invoice_date,
        i.CustomerId,
        i.BillingCountry as country,
        ii.UnitPrice * ii.Quantity as amount,
        ii.Quantity as units_sold
      FROM invoice_items ii
      JOIN invoices i ON ii.InvoiceId = i.InvoiceId
      ORDER BY ii.InvoiceLineId
      LIMIT %d OFFSET %d
    ", batchSize, offset)
    
    musicSales <- executeQuery(musicCon, musicSalesSQL)
    
    if (nrow(musicSales) == 0) {
      hasMoreRows <- FALSE
      next
    }
    
    cat("[INFO] Processing batch of", nrow(musicSales), "music sales records from offset", offset, "\n")
    
    # Prepare batch data for fact_sales and fact_sales_obt tables
    factBatchValues <- character(0)
    obtBatchValues <- character(0)
    
    for (i in 1:nrow(musicSales)) {
      invoiceDate <- as.Date(musicSales$invoice_date[i])
      customerId <- musicSales$CustomerId[i]
      country <- musicSales$country[i]
      amount <- musicSales$amount[i]
      unitsSold <- musicSales$units_sold[i]
      
      # Skip if missing data
      if (is.na(invoiceDate) || is.na(customerId) || is.na(country)) {
        next
      }
      
      # Get time key from lookup
      timeKey <- timeLookup[as.character(invoiceDate)]
      if (is.na(timeKey)) {
        next
      }
      
      # Get country key from lookup
      countryKey <- countryLookup[country]
      if (is.na(countryKey)) {
        next
      }
      
      # Get customer key from lookup
      customerKey <- customerLookup[customerId]
      if (is.na(customerKey)) {
        next
      }
      
      # Add to fact_sales batch
      factBatchValues <- c(factBatchValues, 
                           sprintf("(%d, %d, %d, %d, %d, %f)",
                                   timeKey, customerKey, countryKey, 2, unitsSold, amount))
      
      # Get customer info
      customerInfo <- customerInfoLookup[customerInfoLookup$source_id == customerId, ]
      if (nrow(customerInfo) == 0) {
        next
      }
      
      # Get country name
      countryName <- countryData$country_name[countryData$country_key == countryKey]
      if (length(countryName) == 0) {
        next
      }
      
      # Calculate time dimension values
      dayOfWeek <- weekdays(invoiceDate)
      dayOfMonth <- as.numeric(format(invoiceDate, "%d"))
      monthName <- months(invoiceDate)
      monthNumber <- as.numeric(format(invoiceDate, "%m"))
      quarter <- ceiling(monthNumber / 3)
      year <- as.numeric(format(invoiceDate, "%Y"))
      
      # Add to fact_sales_obt batch
      obtBatchValues <- c(obtBatchValues,
                          sprintf("('%s', '%s', %d, '%s', %d, %d, %d, %d, '%s', '%s', '%s', '%s', '%s', %d, %f)",
                                  invoiceDate, 
                                  dayOfWeek, 
                                  dayOfMonth, 
                                  monthName, 
                                  monthNumber, 
                                  quarter, 
                                  year,
                                  customerId, 
                                  gsub("'", "''", customerInfo$first_name[1]), 
                                  gsub("'", "''", customerInfo$last_name[1]), 
                                  gsub("'", "''", customerInfo$email[1]),
                                  gsub("'", "''", countryName[1]), 
                                  "Music", 
                                  unitsSold, 
                                  amount))
    }
    
    # Insert fact_sales batch
    if (length(factBatchValues) > 0) {
      cat("[INFO] Inserting batch of", length(factBatchValues), "records into fact_sales\n")
      factInsertSQL <- paste0("INSERT INTO fact_sales (time_key, customer_key, country_key, product_type_key, units_sold, revenue) VALUES ", 
                              paste(factBatchValues, collapse = ", "))
      executeSQL(mysqlCon, factInsertSQL)
    }
    
    # Insert fact_sales_obt batch
    if (length(obtBatchValues) > 0) {
      cat("[INFO] Inserting batch of", length(obtBatchValues), "records into fact_sales_obt\n")
      obtInsertSQL <- paste0("INSERT INTO fact_sales_obt (date, day_of_week, day_of_month, month_name, month_number, quarter, year, customer_id, customer_first_name, customer_last_name, customer_email, country_name, product_type, units_sold, revenue) VALUES ", 
                             paste(obtBatchValues, collapse = ", "))
      executeSQL(mysqlCon, obtInsertSQL)
    }
    
    # Move to next batch
    offset <- offset + batchSize
    cat("[INFO] Completed processing", offset, "music sales records.\n")
  }
}

# Function to clear fact tables
clearFactTables <- function(mysqlCon) {
  cat("[INFO] Clearing fact tables...\n")
  executeSQL(mysqlCon, "DELETE FROM fact_sales;")
  executeSQL(mysqlCon, "DELETE FROM fact_sales_obt;")
}

# Function to load all sales facts
loadSalesFacts <- function(filmCon, musicCon, mysqlCon) {
  cat("[INFO] Loading sales facts...\n")
  
  # Clear existing fact data
  clearFactTables(mysqlCon)
  
  # Load film and music sales
  loadFilmSales(filmCon, mysqlCon)
  loadMusicSales(musicCon, mysqlCon)
  
  # Verify loading
  salesCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM fact_sales")
  obtCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM fact_sales_obt")
  cat("[INFO] Loaded", salesCount$count, "sales fact records and", obtCount$count, "OBT records.\n")
}

# Function to validate facts
validateFacts <- function(filmCon, musicCon, mysqlCon) {
  cat("[INFO] Validating fact tables...\n")
  
  # Validate film customers count
  filmCustomersCount <- executeQuery(filmCon, "SELECT COUNT(DISTINCT customer_id) as count FROM customer")
  dwFilmCustomersCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_customer WHERE product_type_key = 1")
  
  cat("[VALIDATION] Film customers in source:", filmCustomersCount$count, "\n")
  cat("[VALIDATION] Film customers in datamart:", dwFilmCustomersCount$count, "\n")
  
  # Validate music customers count
  musicCustomersCount <- executeQuery(musicCon, "SELECT COUNT(DISTINCT CustomerId) as count FROM customers")
  dwMusicCustomersCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_customer WHERE product_type_key = 2")
  
  cat("[VALIDATION] Music customers in source:", musicCustomersCount$count, "\n")
  cat("[VALIDATION] Music customers in datamart:", dwMusicCustomersCount$count, "\n")
  
  # Validate film sales amount
  filmSalesAmount <- executeQuery(filmCon, "SELECT SUM(amount) as total FROM payment")
  dwFilmSalesAmount <- executeQuery(mysqlCon, "SELECT SUM(revenue) as total FROM fact_sales WHERE product_type_key = 1")
  
  cat("[VALIDATION] Film sales amount in source:", filmSalesAmount$total, "\n")
  cat("[VALIDATION] Film sales amount in datamart:", dwFilmSalesAmount$total, "\n")
  
  # Validate music sales amount
  musicSalesAmount <- executeQuery(musicCon, "
    SELECT SUM(UnitPrice * Quantity) as total 
    FROM invoice_items
  ")
  dwMusicSalesAmount <- executeQuery(mysqlCon, "SELECT SUM(revenue) as total FROM fact_sales WHERE product_type_key = 2")
  
  cat("[VALIDATION] Music sales amount in source:", musicSalesAmount$total, "\n")
  cat("[VALIDATION] Music sales amount in datamart:", dwMusicSalesAmount$total, "\n")
  
  # Validate country counts
  filmCountryCount <- executeQuery(filmCon, "SELECT COUNT(*) as count FROM country")
  musicCountryCount <- executeQuery(musicCon, "SELECT COUNT(DISTINCT Country) as count FROM customers WHERE Country IS NOT NULL")
  dwCountryCount <- executeQuery(mysqlCon, "SELECT COUNT(*) as count FROM dim_country")
  
  cat("[VALIDATION] Film countries in source:", filmCountryCount$count, "\n")
  cat("[VALIDATION] Music countries in source:", musicCountryCount$count, "\n")
  cat("[VALIDATION] Countries in datamart:", dwCountryCount$count, "\n")
}

# Main
main <- function() {
  installPackagesOnDemand(c("RMySQL", "RSQLite", "DBI"))
  loadRequiredPackages(c("RMySQL", "RSQLite", "DBI"))
  
  # Connect to databases
  mysqlCon <- connectToMySQL()
  if (is.character(mysqlCon)) {
    cat("[ERROR] MySQL connection failed: ", mysqlCon, "\n")
    return()
  }
  
  # Check if required tables exist
  if (!checkRequiredTables(mysqlCon)) {
    cat("[ERROR] Missing required tables. Please run createStarSchema.R first.\n")
    dbDisconnect(mysqlCon)
    return()
  }
  
  filmCon <- connectToFilmDB()
  if (is.character(filmCon)) {
    cat("[ERROR] Film DB connection failed: ", filmCon, "\n")
    dbDisconnect(mysqlCon)
    return()
  }
  
  musicCon <- connectToMusicDB()
  if (is.character(musicCon)) {
    cat("[ERROR] Music DB connection failed: ", musicCon, "\n")
    dbDisconnect(mysqlCon)
    dbDisconnect(filmCon)
    return()
  }
  
  cat("[INFO] Connected to all databases.\n")
  
  # Load dimensions
  loadTimeDimension(filmCon, musicCon, mysqlCon)
  loadCountryDimension(filmCon, musicCon, mysqlCon)
  loadCustomerDimension(filmCon, musicCon, mysqlCon)
  
  # Load facts
  loadSalesFacts(filmCon, musicCon, mysqlCon)
  
  # Validate facts
  validateFacts(filmCon, musicCon, mysqlCon)
  
  # Close connections
  dbDisconnect(mysqlCon)
  dbDisconnect(filmCon)
  dbDisconnect(musicCon)
  cat("[INFO] All database connections closed.\n")
}

main()