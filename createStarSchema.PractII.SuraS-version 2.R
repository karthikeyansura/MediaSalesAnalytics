#*****************************************
#* Title: "Create Analytics Star Schema for Media Distributors, Inc."
#* Author: "Your Name"
#* Date: "Spring 2025"
#*****************************************

#*****************************************
#* Install Required Packages
#*****************************************
installPackagesOnDemand <- function(packages) {
  installed_packages <- packages %in% rownames(installed.packages())
  if (any(installed_packages == FALSE)) {
    install.packages(packages[!installed_packages])
  }
}

#*****************************************
#* Load Required Packages
#*****************************************
loadRequiredPackages <- function(packages) {
  for (package in packages) {
    suppressMessages({
      library(package, character.only = TRUE)
    })
  }
}

#*****************************************
#* Close Db Connections Over Threshold
#*****************************************
closeDbConnectionsOverThreshold <- function() {
  threshold <- 10
  currentOpenConnections <- dbListConnections(MySQL())
  if (length(currentOpenConnections) > threshold) {
    for (conn in currentOpenConnections) {
      dbDisconnect(conn)
    }
  }
}

#*****************************************
#* Connect to MySQL Database
#*****************************************
connectToMySQLDatabase <- function() {
  dbName <- "defaultdb"
  dbUser <- "avnadmin"
  dbPassword <- "AVNS_4zUOM58G58RIBn3nQqg"
  dbHost <- "dbserver-cs5200-media-sales-analytics.b.aivencloud.com"
  dbPort <- 17041
  
  cat("[INFO] Connecting to MySQL database...\n")
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
    dbExecute(dbCon, paste("USE", dbName, ";"))
    cat("[INFO] Connected to MySQL database successfully.\n")
    return(dbCon)
  }, error = function(err) {
    cat("[ERROR] Database connection failed:", err$message, "\n")
    stop("Database connection failed.")
  })
}

#*****************************************
#* Print Line Separator
#*****************************************
printLine <- function() {
  cat("--------------------------------------------------\n")
}

#*****************************************
#* Execute SQL with Error Handling
#*****************************************
executeSQL <- function(dbCon, sqlStatement, silent = TRUE) {
  result <- tryCatch({
    dbExecute(dbCon, sqlStatement)
  }, error = function(err) {
    if (!silent) {
      cat("[ERROR] SQL execution failed:", err$message, "\n")
    }
    return(NULL)
  })
  return(result)
}

#*****************************************
#* Execute SQL Query with Error Handling
#*****************************************
executeQuery <- function(dbCon, sqlQuery, silent = TRUE) {
  result <- tryCatch({
    dbGetQuery(dbCon, sqlQuery)
  }, error = function(err) {
    if (!silent) {
      cat("[ERROR] SQL query failed:", err$message, "\n")
    }
    return(NULL)
  })
  return(result)
}

#*****************************************
#* Drop Table If Exists
#*****************************************
dropTable <- function(dbCon, tableName) {
  sqlQueryToDropTable <- paste("DROP TABLE IF EXISTS", tableName, ";")
  executeSQL(dbCon, sqlQueryToDropTable, silent = TRUE)
}

#*****************************************
#* Drop View If Exists
#*****************************************
dropView <- function(dbCon, viewName) {
  sqlQueryToDropView <- paste("DROP VIEW IF EXISTS", viewName, ";")
  executeSQL(dbCon, sqlQueryToDropView, silent = TRUE)
}

#*****************************************
#* Drop All Tables and Views
#*****************************************
dropAllTables <- function(dbCon) {
  cat("[INFO] Removing existing schema objects...\n")
  executeSQL(dbCon, "SET FOREIGN_KEY_CHECKS = 0;", silent = TRUE)
  
  views <- c("view_revenue_analysis", "view_units_analysis", "view_customer_analysis")
  for (view in views) {
    dropView(dbCon, view)
  }
  
  factTables <- c("fact_time_agg", "fact_sales_obt", "fact_sales")
  for (table in factTables) {
    dropTable(dbCon, table)
  }
  
  dimensionTables <- c("dim_customer", "dim_product", "dim_product_type", "dim_location", "dim_time")
  for (table in dimensionTables) {
    dropTable(dbCon, table)
  }
  
  executeSQL(dbCon, "SET FOREIGN_KEY_CHECKS = 1;", silent = TRUE)
  cat("[INFO] Dropped existing tables and views.\n")
}

#*****************************************
#* Create Time Dimension Table
#*****************************************
createTimeDimensionTable <- function(dbCon) {
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
      holiday_flag BOOLEAN DEFAULT FALSE,
      INDEX idx_year_quarter (year, quarter),
      INDEX idx_date (full_date)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Location Dimension Table
#*****************************************
createLocationDimensionTable <- function(dbCon) {
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
      last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE INDEX idx_country_source (country, source_system, source_id),
      INDEX idx_country (country)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Product Type Dimension Table
#*****************************************
createProductTypeDimensionTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE dim_product_type (
      product_type_key INT PRIMARY KEY,
      product_type VARCHAR(20) NOT NULL,
      description VARCHAR(255),
      source_system VARCHAR(10) NOT NULL,
      UNIQUE INDEX idx_product_type (product_type, source_system)
    );
  "
  executeSQL(dbCon, sqlQuery)
  
  insertProductTypesSQL <- "
    INSERT INTO dim_product_type (product_type_key, product_type, description, source_system) VALUES
    (1, 'Film', 'Film and video products', 'FILM'),
    (2, 'Music', 'Music and audio products', 'MUSIC');
  "
  executeSQL(dbCon, insertProductTypesSQL)
}

#*****************************************
#* Create Product Dimension Table
#*****************************************
createProductDimensionTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE dim_product (
      product_key INT AUTO_INCREMENT PRIMARY KEY,
      source_product_id VARCHAR(20) NOT NULL,
      source_system VARCHAR(10) NOT NULL,
      product_type_key INT NOT NULL,
      title VARCHAR(255) NOT NULL,
      category_genre VARCHAR(25),
      artist_actor VARCHAR(100),
      release_year SMALLINT,
      language VARCHAR(20),
      media_type VARCHAR(50),
      unit_price DECIMAL(10,2),
      duration INT,
      last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (product_type_key) REFERENCES dim_product_type(product_type_key),
      UNIQUE INDEX idx_source_product (source_product_id, source_system),
      INDEX idx_product_type (product_type_key)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Customer Dimension Table
#*****************************************
createCustomerDimensionTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE dim_customer (
      customer_key INT AUTO_INCREMENT PRIMARY KEY,
      source_customer_id VARCHAR(20) NOT NULL,
      source_system VARCHAR(10) NOT NULL,
      first_name VARCHAR(45) NOT NULL,
      last_name VARCHAR(45) NOT NULL,
      email VARCHAR(60),
      location_key INT,
      active BOOLEAN DEFAULT TRUE,
      create_date DATETIME,
      last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      UNIQUE INDEX idx_source_customer (source_customer_id, source_system),
      INDEX idx_location (location_key),
      FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Sales Fact Table
#*****************************************
createSalesFactTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE fact_sales (
      sales_fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
      time_key INT NOT NULL,
      customer_key INT NOT NULL,
      product_key INT NOT NULL,
      location_key INT NOT NULL,
      
      -- Source information
      source_system VARCHAR(10) NOT NULL,
      source_transaction_id VARCHAR(20) NOT NULL,
      transaction_date DATETIME NOT NULL,
      
      -- Facts/measures
      quantity INT NOT NULL,
      amount DECIMAL(10,2) NOT NULL,
      
      -- Foreign keys
      FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
      FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
      FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
      FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
      
      -- Indexes for faster queries
      INDEX idx_time (time_key),
      INDEX idx_location_time (location_key, time_key),
      INDEX idx_product_time (product_key, time_key),
      INDEX idx_source_transaction (source_system, source_transaction_id),
      INDEX idx_year_range (time_key)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Sales OBT Fact Table
#*****************************************
createSalesOBTFactTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE fact_sales_obt (
      sales_obt_id BIGINT AUTO_INCREMENT PRIMARY KEY,
      
      -- Time dimension fields
      transaction_date DATE NOT NULL,
      day_of_week VARCHAR(10),
      month_name VARCHAR(10),
      month_num TINYINT,
      quarter TINYINT,
      year SMALLINT,
      
      -- Customer dimension fields
      customer_id VARCHAR(20) NOT NULL,
      customer_name VARCHAR(100),
      
      -- Product dimension fields
      product_id VARCHAR(20) NOT NULL,
      product_title VARCHAR(255),
      product_type VARCHAR(20),
      
      -- Location dimension fields
      country VARCHAR(50) NOT NULL,
      region VARCHAR(30),
      
      -- Facts/measures
      quantity INT NOT NULL,
      amount DECIMAL(10,2) NOT NULL,
      source_system VARCHAR(10) NOT NULL,
      
      -- Indexes for faster queries
      INDEX idx_date (transaction_date),
      INDEX idx_year_quarter (year, quarter),
      INDEX idx_country_product (country, product_type),
      INDEX idx_country_year (country, year),
      INDEX idx_year (year)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create Time Aggregation Fact Table
#*****************************************
createTimeAggFactTable <- function(dbCon) {
  sqlQuery <- "
    CREATE TABLE fact_time_agg (
      time_agg_id BIGINT AUTO_INCREMENT PRIMARY KEY,
      
      -- Dimension references
      time_key INT NOT NULL,
      time_level_id TINYINT NOT NULL, -- 1=DAY, 2=MONTH, 3=QUARTER, 4=YEAR
      time_level VARCHAR(10) NOT NULL, -- 'DAY', 'MONTH', 'QUARTER', 'YEAR'
      country VARCHAR(50) NOT NULL,
      product_type VARCHAR(20) NOT NULL,
      source_system VARCHAR(10) NOT NULL,
      
      -- Aggregated measures
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
      
      -- Composite unique constraint and indexes
      UNIQUE INDEX idx_time_agg_unique (time_key, time_level_id, country, product_type, source_system),
      INDEX idx_country_product (country, product_type),
      INDEX idx_time_level (time_level_id, time_key),
      INDEX idx_time_level_str (time_level)
    );
  "
  executeSQL(dbCon, sqlQuery)
}

#*****************************************
#* Create All Dimension Tables
#*****************************************
createDimensionTables <- function(dbCon) {
  cat("[INFO] Creating dimension tables...\n")
  createTimeDimensionTable(dbCon)
  createLocationDimensionTable(dbCon)
  createProductTypeDimensionTable(dbCon)
  createProductDimensionTable(dbCon)
  createCustomerDimensionTable(dbCon)
  cat("[INFO] Created dimension tables.\n")
  cat("- Dimension tables: dim_time, dim_location, dim_product_type, dim_product, dim_customer\n")
}

#*****************************************
#* Create All Fact Tables
#*****************************************
createFactTables <- function(dbCon) {
  cat("[INFO] Creating fact tables...\n")
  createSalesFactTable(dbCon)
  createSalesOBTFactTable(dbCon)
  createTimeAggFactTable(dbCon)
  cat("[INFO] Created fact tables.\n")
  cat("- Fact tables: fact_sales, fact_sales_obt, fact_time_agg\n")
}

#*****************************************
#* Create Additional Views
#*****************************************
createAnalyticalViews <- function(dbCon) {
  cat("[INFO] Creating analytical views...\n")
  
  # Create revenue analysis view
  revenueViewSQL <- "
    CREATE OR REPLACE VIEW view_revenue_analysis AS
    SELECT 
      dt.year,
      dt.quarter,
      dt.month_name,
      dl.country,
      dl.region,
      dpt.product_type,
      SUM(fs.amount) AS total_revenue,
      AVG(fs.amount) AS avg_revenue_per_transaction,
      COUNT(DISTINCT fs.customer_key) AS customer_count
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    JOIN dim_location dl ON fs.location_key = dl.location_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_product_type dpt ON dp.product_type_key = dpt.product_type_key
    GROUP BY 
      dt.year,
      dt.quarter,
      dt.month_name,
      dl.country,
      dl.region,
      dpt.product_type
    WITH ROLLUP;
  "
  executeSQL(dbCon, revenueViewSQL)
  
  # Create units sold analysis view
  unitsViewSQL <- "
    CREATE OR REPLACE VIEW view_units_analysis AS
    SELECT 
      dt.year,
      dt.quarter,
      dt.month_name,
      dl.country,
      dl.region,
      dpt.product_type,
      SUM(fs.quantity) AS total_units,
      AVG(fs.quantity) AS avg_units_per_transaction,
      MIN(fs.quantity) AS min_units,
      MAX(fs.quantity) AS max_units,
      COUNT(DISTINCT fs.customer_key) AS customer_count
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    JOIN dim_location dl ON fs.location_key = dl.location_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_product_type dpt ON dp.product_type_key = dpt.product_type_key
    GROUP BY 
      dt.year,
      dt.quarter,
      dt.month_name,
      dl.country,
      dl.region,
      dpt.product_type
    WITH ROLLUP;
  "
  executeSQL(dbCon, unitsViewSQL)
  
  # Create customer analysis view
  customerViewSQL <- "
    CREATE OR REPLACE VIEW view_customer_analysis AS
    SELECT 
      dl.country,
      dl.region,
      dpt.product_type,
      COUNT(DISTINCT dc.customer_key) AS customer_count,
      SUM(fs.amount) AS total_revenue,
      SUM(fs.quantity) AS total_units,
      SUM(fs.amount) / COUNT(DISTINCT dc.customer_key) AS revenue_per_customer,
      SUM(fs.quantity) / COUNT(DISTINCT dc.customer_key) AS units_per_customer
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_key = dc.customer_key
    JOIN dim_location dl ON fs.location_key = dl.location_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    JOIN dim_product_type dpt ON dp.product_type_key = dpt.product_type_key
    GROUP BY 
      dl.country,
      dl.region,
      dpt.product_type
    WITH ROLLUP;
  "
  executeSQL(dbCon, customerViewSQL)
  
  cat("[INFO] Created analytical views.\n")
  cat("- Analytical views: view_revenue_analysis, view_units_analysis, view_customer_analysis\n")
}

#*****************************************
#* Main Method
#*****************************************
main <- function() {
  # Required packages
  packages <- c("RMySQL", "DBI")
  
  # Install and load required packages
  installPackagesOnDemand(packages)
  loadRequiredPackages(packages)
  
  # Connect to database
  mySqlDbCon <- connectToMySQLDatabase()
  
  # Drop all existing tables
  dropAllTables(mySqlDbCon)
  
  # Create dimension tables
  createDimensionTables(mySqlDbCon)
  
  # Create fact tables
  createFactTables(mySqlDbCon)
  
  # Create analytical views
  createAnalyticalViews(mySqlDbCon)
  
  # Disconnect from database
  dbDisconnect(mySqlDbCon)
  cat("[INFO] Star schema created successfully.\n")
}

# Execute the script
main()