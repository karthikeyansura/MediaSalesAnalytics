installPackagesOnDemand <- function(packages) {
  missing <- packages[!(packages %in% rownames(installed.packages()))]
  if (length(missing)) install.packages(missing)
}

loadRequiredPackages <- function(packages) {
  lapply(packages, function(pkg) suppressMessages(library(pkg, character.only = TRUE)))
}

releaseAivenConnections <- function(threshold = 15) {
  active_cons <- DBI::dbListConnections(RMySQL::MySQL())
  if (length(active_cons) >= threshold) {
    cat("[INFO] Threshold reached. Disconnecting existing MySQL connections...\n")
    for (con in active_cons) {
      DBI::dbDisconnect(con)
    }
    cat("[INFO] Existing MySQL connections closed.\n")
  }
}

connectToCloudDatabase <- function() {
  releaseAivenConnections()
  
  DBI::dbConnect(RMySQL::MySQL(),
                 user = "",
                 password = "",
                 dbname = "",
                 host = "",
                 port = )
}

dropTablesAndViews <- function(con) {
  DBI::dbExecute(con, "SET FOREIGN_KEY_CHECKS = 0;")
  
  obj_info <- DBI::dbGetQuery(con, "SHOW FULL TABLES")
  colnames(obj_info) <- c("name", "type")
  
  if (nrow(obj_info) == 0) {
    cat("[INFO] No tables or views found.\n")
  } else {
    for (i in seq_len(nrow(obj_info))) {
      name <- obj_info$name[i]
      type <- obj_info$type[i]
      
      drop_stmt <- if (type == "VIEW") {
        sprintf("DROP VIEW IF EXISTS `%s`", name)
      } else {
        sprintf("DROP TABLE IF EXISTS `%s`", name)
      }
      
      DBI::dbExecute(con, drop_stmt)
      cat("[INFO] Dropped", type, ":", name, "\n")
    }
  }
  
  DBI::dbExecute(con, "SET FOREIGN_KEY_CHECKS = 1;")
}

main <- function() {
  installPackagesOnDemand(c("DBI", "RMySQL"))
  loadRequiredPackages(c("DBI", "RMySQL"))
  
  con <- connectToCloudDatabase()
  
  dropTablesAndViews(con)
  
  DBI::dbDisconnect(con)
  cat("[SUCCESS] Database cleaned up and connection closed.\n")
}

main()
