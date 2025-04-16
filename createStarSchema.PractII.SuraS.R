# title: "Part B / Create Analytics Datamart"
# author: "Sai Karthikeyan, Sura"
# date: "Spring 2025"

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

# Print Line Separator
printLine <- function() {
  cat("--------------------------------------------------\n")
}

# Drop Table If Exists
dropTable <- function(dbCon, tableName) {
  # drop table if exists
  sqlQueryToDropTable <- paste("DROP TABLE IF EXISTS", tableName, ";")
  dbExecute(dbCon, sqlQueryToDropTable)
  cat("Dropped table:", tableName, "\n")
}

# Drop All Tables
dropAllTables <- function(dbCon) {
  # drop all fact tables
  cat("Dropping fact tables...\n")
  
  factTables <- c("sales_facts", "time_agg_facts")
  
  for (table in factTables) {
    dropTable(dbCon, table)
  }
  
  # drop all dimension tables
  cat("Dropping dimension tables...\n")
  
  dimensionTables <- c("dim_product", "dim_customer", "dim_time", "dim_location", "dim_prouct_type")
  
  for (table in dimensionTables) {
    dropTable(dbCon, table)
  }
  
  printLine()
}

createProductDimensionTable <- function(dbCon) {
  # SQL query to create product dimension table
  sqlQuery <- "
    CREATE TABLE dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    source_product_id VARCHAR(20) NOT NULL,
    source_system VARCHAR(10) NOT NULL,
    product_type VARCHAR(10) NOT NULL,
    title VARCHAR(255) NOT NULL,
    category_genre VARCHAR(25),
    artist_actor VARCHAR(100),
    release_year SMALLINT,
    language VARCHAR(20),
    media_type VARCHAR(50),
    unit_price DECIMAL(10,2),
    duration INT,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  "
  
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  dim_product\n")
}

createCustomerDimensionTable <- function(dbCon) {
  # SQL query to create customer dimension table
  sqlQuery <- "
    CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    source_customer_id VARCHAR(20) NOT NULL,
    source_system VARCHAR(10) NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    email VARCHAR(60),
    active BOOLEAN DEFAULT TRUE,
    create_date DATETIME,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  "
  
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  dim_customer\n")
}

createTimeDimensionTable <- function(dbCon) {
  # SQL query to create time dimension table
  sqlQuery <- "
    CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    day_num_in_month TINYINT NOT NULL,
    day_num_in_year SMALLINT NOT NULL,
    month_num TINYINT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter TINYINT NOT NULL,
    year SMALLINT NOT NULL,
    weekend_flag BOOLEAN NOT NULL,
    holiday_flag BOOLEAN DEFAULT FALSE
    );
  "
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  dim_time\n")
}

createLocationDimensionTable <- function(dbCon) {
  # SQL query to create location dimension table
  sqlQuery <- "
    CREATE TABLE dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(20),
    source_system VARCHAR(10) NOT NULL,
    address VARCHAR(70),
    city VARCHAR(50),
    state_province VARCHAR(40),
    postal_code VARCHAR(10),
    country VARCHAR(50) NOT NULL,
    region VARCHAR(30),
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
  "
  
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  dim_location\n")
}

createDimensionTables <- function(dbCon) {
  # create dimension tables
  cat("Creating dimension tables...\n")
  
  # create dim_product table
  createProductDimensionTable(dbCon)
  
  # create dim_customer table
  createCustomerDimensionTable(dbCon)
  
  # create dim_time table
  createTimeDimensionTable(dbCon)
  
  # create dim_location table
  createLocationDimensionTable(dbCon)
  
  printLine()
}

createSalesFactTable <- function(dbCon) {
  # SQL query to create sales fact table
  sqlQuery <- "
    CREATE TABLE sales_facts (
    sales_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    time_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    location_key INT NOT NULL,
    
    source_system VARCHAR(10) NOT NULL,
    source_transaction_id VARCHAR(20) NOT NULL,
    transaction_date DATETIME NOT NULL,
    
    quantity INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
    );
  "
  
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  sales_facts\n")
}

createTimeAggFactTable <- function(dbCon) {
  # SQL query to create time_agg_facts table
  sqlQuery <- "
    CREATE TABLE time_agg_facts (
    time_agg_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    time_key INT NOT NULL,
    time_level VARCHAR(10) NOT NULL,
    country VARCHAR(50) NOT NULL,
    source_system VARCHAR(10) NOT NULL,
    
    total_revenue DECIMAL(14,2) NOT NULL,
    avg_revenue_per_transaction DECIMAL(12,2) NOT NULL,
    total_units_sold INT NOT NULL,
    avg_units_per_transaction DECIMAL(10,2) NOT NULL,
    min_units_per_transaction INT NOT NULL,
    max_units_per_transaction INT NOT NULL,
    min_revenue_per_transaction DECIMAL(10,2) NOT NULL,
    max_revenue_per_transaction DECIMAL(10,2) NOT NULL,
    customer_count INT NOT NULL,
    transaction_count INT NOT NULL,
    
    UNIQUE INDEX idx_time_agg_unique (time_key, time_level, country, source_system)
    );
  "
  
  # execute SQL query
  dbExecute(dbCon, sqlQuery)
  cat("Created table:  time_agg_facts\n")
}

createFactTables <- function(dbCon) {
  # create fact tables
  cat("Creating fact tables...\n")
  
  # create sales_facts table
  createSalesFactTable(dbCon)
  
  # create time_agg_facts table
  createTimeAggFactTable(dbCon)
  
  printLine()
}

# Main Method
main <- function() {
  # required packages
  packages <- c("RMySQL")
  
  # install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # connect to database
  mySqlDbCon <- connectToMySQLDatabase()
  
  # drop all tables
  dropAllTables(mySqlDbCon)
  
  # create dimension tables
  createDimensionTables(mySqlDbCon)
  
  # create fact tables
  createFactTables(mySqlDbCon)
  
  # disconnect from database
  dbDisconnect(mySqlDbCon)
}

# execute the script
main()
