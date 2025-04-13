installPackagesOnDemand <- function(packages) {
  missing <- packages[!(packages %in% rownames(installed.packages()))]
  if (length(missing)) install.packages(missing)
}

loadRequiredPackages <- function(packages) {
  lapply(packages, function(pkg) suppressMessages(library(pkg, character.only = TRUE)))
}

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

connectToCloudDatabase <- function() {
  
  releaseAivenConnections()
  
  dbConnect(RMySQL::MySQL(),
            user = "avnadmin",
            password = "AVNS_4zUOM58G58RIBn3nQqg",
            dbname = "defaultdb",
            host = "dbserver-cs5200-media-sales-analytics.b.aivencloud.com",
            port = 17041)
}

main <- function() {
  installPackagesOnDemand(c("DBI", "RMySQL"))
  loadRequiredPackages(c("DBI", "RMySQL"))
  
  con <- connectToCloudDatabase()
  
  tables <- dbGetQuery(con, "SHOW TABLES")[[1]]
  cat("[INFO] Found tables:\n"); print(tables)
  
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 0;")
  
  for (tbl in tables) {
    query <- sprintf("DROP TABLE IF EXISTS `%s`", tbl)
    dbExecute(con, query)
    cat("[INFO] Dropped table:", tbl, "\n")
  }
  
  dbExecute(con, "SET FOREIGN_KEY_CHECKS = 1;")
  dbDisconnect(con)
  cat("[SUCCESS] All tables dropped and MySQL connection closed.\n")
}

main()
