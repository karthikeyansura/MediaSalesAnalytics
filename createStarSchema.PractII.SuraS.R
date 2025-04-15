# Title: Part B / Create Analytics Datamart - FIXED
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

# Function to create time dimension table
createTimeDimension <- function(con) {
  cat("[INFO] Creating time dimension table...\n")
  
  # Drop existing dimension if exists
  executeSQL(con, "DROP TABLE IF EXISTS dim_time;")
  
  # Create time dimension table
  sql <- "
  CREATE TABLE dim_time (
    time_key INT PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month_name VARCHAR(10),
    month_number INT,
    quarter INT,
    year INT,
    UNIQUE INDEX idx_date (date),
    INDEX idx_year_quarter (year, quarter)
  );
  "
  executeSQL(con, sql)
}

# Function to create country dimension table
createCountryDimension <- function(con) {
  cat("[INFO] Creating country dimension table...\n")
  
  # Drop existing dimension if exists
  executeSQL(con, "DROP TABLE IF EXISTS dim_country;")
  
  # Create country dimension table
  sql <- "
  CREATE TABLE dim_country (
    country_key INT AUTO_INCREMENT PRIMARY KEY,
    country_name VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    UNIQUE INDEX idx_country_name (country_name)
  );
  "
  executeSQL(con, sql)
}

# Function to create product type dimension table
createProductTypeDimension <- function(con) {
  cat("[INFO] Creating product type dimension table...\n")
  
  # Drop existing dimension if exists
  executeSQL(con, "DROP TABLE IF EXISTS dim_product_type;")
  
  # Create product type dimension table
  sql <- "
  CREATE TABLE dim_product_type (
    product_type_key INT PRIMARY KEY,
    product_type VARCHAR(20) NOT NULL,
    description VARCHAR(255)
  );
  "
  executeSQL(con, sql)
  
  # Insert product types
  executeSQL(con, "INSERT INTO dim_product_type VALUES (1, 'Film', 'Film products');")
  executeSQL(con, "INSERT INTO dim_product_type VALUES (2, 'Music', 'Music products');")
}

# Function to create customer dimension table
createCustomerDimension <- function(con) {
  cat("[INFO] Creating customer dimension table...\n")
  
  # Drop existing dimension if exists
  executeSQL(con, "DROP TABLE IF EXISTS dim_customer;")
  
  # Create customer dimension table
  sql <- "
  CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT NOT NULL,
    product_type_key INT NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    country_key INT,
    UNIQUE INDEX idx_source_product (source_id, product_type_key),
    INDEX idx_country (country_key),
    FOREIGN KEY (product_type_key) REFERENCES dim_product_type(product_type_key),
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key)
  );
  "
  executeSQL(con, sql)
}

# Function to create sales fact table
createSalesFact <- function(con) {
  cat("[INFO] Creating sales fact table...\n")
  
  # Drop existing fact if exists
  executeSQL(con, "DROP TABLE IF EXISTS fact_sales;")
  
  # Create sales fact table
  sql <- "
  CREATE TABLE fact_sales (
    sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    time_key INT NOT NULL,
    customer_key INT NOT NULL,
    country_key INT NOT NULL,
    product_type_key INT NOT NULL,
    units_sold INT NOT NULL,
    revenue DECIMAL(10,2) NOT NULL,
    INDEX idx_time (time_key),
    INDEX idx_customer (customer_key),
    INDEX idx_country (country_key),
    INDEX idx_product_type (product_type_key),
    FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
    FOREIGN KEY (product_type_key) REFERENCES dim_product_type(product_type_key)
  );
  "
  executeSQL(con, sql)
  
  # Create monthly indexes to help with performance
  executeSQL(con, "CREATE INDEX idx_sales_time_country ON fact_sales (time_key, country_key);")
  executeSQL(con, "CREATE INDEX idx_sales_time_product ON fact_sales (time_key, product_type_key);")
}

# Function to create an OBT (One Big Table) fact table
createOBTFact <- function(con) {
  cat("[INFO] Creating One Big Table (OBT) fact table...\n")
  
  # Drop existing OBT if exists
  executeSQL(con, "DROP TABLE IF EXISTS fact_sales_obt;")
  
  # Create OBT fact table
  sql <- "
  CREATE TABLE fact_sales_obt (
    sales_obt_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    
    -- Time dimension fields
    date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_of_month INT,
    month_name VARCHAR(10),
    month_number INT,
    quarter INT,
    year INT,
    
    -- Customer dimension fields
    customer_id INT NOT NULL,
    customer_first_name VARCHAR(50),
    customer_last_name VARCHAR(50),
    customer_email VARCHAR(100),
    
    -- Country dimension fields
    country_name VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    
    -- Product type dimension field
    product_type VARCHAR(20) NOT NULL,
    
    -- Metrics
    units_sold INT NOT NULL,
    revenue DECIMAL(10,2) NOT NULL,
    
    -- Indexes for better performance
    INDEX idx_date (date),
    INDEX idx_year_quarter (year, quarter),
    INDEX idx_country (country_name),
    INDEX idx_product_type (product_type)
  );
  "
  executeSQL(con, sql)
  
  # Create composite indexes for common query patterns
  executeSQL(con, "CREATE INDEX idx_obt_country_product ON fact_sales_obt (country_name, product_type);")
  executeSQL(con, "CREATE INDEX idx_obt_year_quarter_product ON fact_sales_obt (year, quarter, product_type);")
}

# Function to create sales aggregation view by time
createSalesAggTimeView <- function(con) {
  cat("[INFO] Creating time-based sales aggregation view...\n")
  
  # Drop existing view if exists
  executeSQL(con, "DROP VIEW IF EXISTS view_sales_by_time;")
  
  # Create aggregated view by time
  sql <- "
  CREATE VIEW view_sales_by_time AS
  SELECT 
    dt.year,
    dt.quarter,
    dt.month_number,
    dt.month_name,
    dp.product_type,
    SUM(fs.units_sold) AS total_units,
    AVG(fs.units_sold) AS avg_units,
    MIN(fs.units_sold) AS min_units,
    MAX(fs.units_sold) AS max_units,
    SUM(fs.revenue) AS total_revenue,
    AVG(fs.revenue) AS avg_revenue,
    MIN(fs.revenue) AS min_revenue,
    MAX(fs.revenue) AS max_revenue,
    COUNT(DISTINCT fs.customer_key) AS customer_count
  FROM fact_sales fs
  JOIN dim_time dt ON fs.time_key = dt.time_key
  JOIN dim_product_type dp ON fs.product_type_key = dp.product_type_key
  GROUP BY dt.year, dt.quarter, dt.month_number, dt.month_name, dp.product_type
  WITH ROLLUP;
  "
  executeSQL(con, sql)
}

# Function to create sales aggregation view by country
createSalesAggCountryView <- function(con) {
  cat("[INFO] Creating country-based sales aggregation view...\n")
  
  # Drop existing view if exists
  executeSQL(con, "DROP VIEW IF EXISTS view_sales_by_country;")
  
  # Create aggregated view by country
  sql <- "
  CREATE VIEW view_sales_by_country AS
  SELECT 
    dc.country_name,
    dp.product_type,
    dt.year,
    dt.quarter,
    SUM(fs.units_sold) AS total_units,
    AVG(fs.units_sold) AS avg_units,
    MIN(fs.units_sold) AS min_units,
    MAX(fs.units_sold) AS max_units,
    SUM(fs.revenue) AS total_revenue,
    AVG(fs.revenue) AS avg_revenue,
    MIN(fs.revenue) AS min_revenue,
    MAX(fs.revenue) AS max_revenue,
    COUNT(DISTINCT fs.customer_key) AS customer_count
  FROM fact_sales fs
  JOIN dim_country dc ON fs.country_key = dc.country_key
  JOIN dim_product_type dp ON fs.product_type_key = dp.product_type_key
  JOIN dim_time dt ON fs.time_key = dt.time_key
  GROUP BY dc.country_name, dp.product_type, dt.year, dt.quarter
  WITH ROLLUP;
  "
  executeSQL(con, sql)
}

# Function to create customer count aggregation view
createCustomerCountView <- function(con) {
  cat("[INFO] Creating customer count aggregation view...\n")
  
  # Drop existing view if exists
  executeSQL(con, "DROP VIEW IF EXISTS view_customer_count;")
  
  # Create customer count aggregation view
  sql <- "
  CREATE VIEW view_customer_count AS
  SELECT 
    dc.country_name,
    dp.product_type,
    COUNT(DISTINCT c.customer_key) AS customer_count
  FROM dim_customer c
  JOIN dim_country dc ON c.country_key = dc.country_key
  JOIN dim_product_type dp ON c.product_type_key = dp.product_type_key
  GROUP BY dc.country_name, dp.product_type
  WITH ROLLUP;
  "
  executeSQL(con, sql)
}

# Main
main <- function() {
  installPackagesOnDemand(c("RMySQL", "DBI"))
  loadRequiredPackages(c("RMySQL", "DBI"))
  
  # Connect to cloud MySQL and initialize schema
  con <- connectAndCheckDatabase()
  if (is.character(con)) {
    cat("[ERROR] MySQL connection failed: ", con, "\n")
  } else {
    cat("[INFO] Connected to MySQL database.\n")
    
    # Create dimension tables
    createTimeDimension(con)
    createCountryDimension(con)
    createProductTypeDimension(con)
    createCustomerDimension(con)
    
    # Create fact tables
    createSalesFact(con)
    createOBTFact(con)
    
    # Create aggregation views
    createSalesAggTimeView(con)
    createSalesAggCountryView(con)
    createCustomerCountView(con)
    
    dbDisconnect(con)
    cat("[INFO] MySQL connection closed.\n")
  }
}

main()