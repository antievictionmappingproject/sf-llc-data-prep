library(RPostgres)
library(dplyr)
library(janitor)

## Loading required package: DBI
pw <- {
  "password"
}

con <- dbConnect(RPostgres::Postgres(), dbname = "azaddb",
                 host = "localhost", port = 5432,
                 user = "azad", password = pw, bigint = "numeric")

data_llc <- readr::read_csv('~/R_Proj/data/LPMASTER.csv')

'%!in%' <- function(x,y)!('%in%'(x,y))
data_llc <- readr::read_csv('/home/azad/data/LPMASTER.csv')

remove_1 <- c("ADDR", "SAME", ")", "SAME", ")",   "EXEC",     "70)",     "46)",  "20634)",      "1)",    "348)",      "ES",   "9282)", "EX", "0)", "E", "2)",  "X",    "181)",   "2723)", 
              "6)", "208)",  "6", "RIEZ",  "PIER", "9", "GP1", "GP",  "OREXCO",  "SAMECA",    "476)", "EZ", "N",  "11", "LEE" ,  "1348)", "278)", "9)", "EC", "RC )",
              "`", "1",   "36)",  "3665 H", "3X",    "446)",     "48)", "CEO", "WZ", ".",  "% KTBS",     "`EX",    "170)",   "2798)",    "282)",  "3 RIEZ", "EX"   ,"FLOOR",  "92199)", "TOWER")

remove_2 <- c("EEDDG", "POB 9", "GGGGG", 	"RR 2", "S", "AA", "2", "NONE", )

data_llc <- (data_llc %>% filter(ca_address %!in% remove_1))
data_llc <- (data_llc %>% filter(ca_address %!in% remove_2))

remove_others <- (data_llc %>% filter(nchar( ca_address) < 6))$ca_address
data_llc <- (data_llc %>% filter(ca_address %!in% remove_others))

data_llc <- data_llc %>% mutate(year = substr(file_date, 1, 4))



##### matches_ca_18_18 ####


data_ll_ca_18 <- split((data_llc %>% filter(year == "2018")), (0:nrow(data_llc %>% filter(year == "2018")) %/% (nrow(data_llc %>% filter(year == "2018")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_18[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   -- phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', calif_address, calif_city, calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con, "DROP TABLE IF EXISTS llc_ca2018;")
dbSendQuery(con,  "CREATE TABLE llc_ca2018 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")


# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_18[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2018_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

data_ll_ca_18 <- NULL


##### matches_ca_17_17 ####


data_ll_ca_17 <- split((data_llc %>% filter(year == "2017")), (0:nrow(data_llc %>% filter(year == "2017")) %/% (nrow(data_llc %>% filter(year == "2017")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_17[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2017 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_17[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2017_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

data_ll_ca_17 <- NULL

##### matches_ca_16_16 ####

data_ll_ca_16 <- split((data_llc %>% filter(year == "2016")), (0:nrow(data_llc %>% filter(year == "2016")) %/% (nrow(data_llc %>% filter(year == "2016")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_16[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2016 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_16[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2016_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

data_ll_ca_16 <- NULL


##### matches_ca_15_15 ####

data_ll_ca_15 <- split((data_llc %>% filter(year == "2015")), (0:nrow(data_llc %>% filter(year == "2015")) %/% (nrow(data_llc %>% filter(year == "2015")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_15[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2015 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_15[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2015_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

data_ll_ca_15 <- NULL

##### matches_ca_14_14 ####


data_ll_ca_14 <- split((data_llc %>% filter(year == "2014")), (0:nrow(data_llc %>% filter(year == "2014")) %/% (nrow(data_llc %>% filter(year == "2014")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_14[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2014 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_14[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2014_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);")

data_ll_ca_14 <- NULL

##### matches_ca_13_13 ####

data_ll_ca_13 <- split((data_llc %>% filter(year == "2013")), (0:nrow(data_llc %>% filter(year == "2013")) %/% (nrow(data_llc %>% filter(year == "2013")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_13[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2013 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_13[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2013_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_13 <- NULL

##### matches_ca_12_12 ####

data_ll_ca_12 <- split((data_llc %>% filter(year == "2012")), (0:nrow(data_llc %>% filter(year == "2012")) %/% (nrow(data_llc %>% filter(year == "2012")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_12[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2012 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_12[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2012_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_12 <- NULL

##### matches_ca_11_11 ####

data_ll_ca_11 <- split((data_llc %>% filter(year == "2011")), (0:nrow(data_llc %>% filter(year == "2011")) %/% (nrow(data_llc %>% filter(year == "2011")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_11[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2011 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_11[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2011_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_11 <- NULL

##### matches_ca_10_10 ####

data_ll_ca_10 <- split((data_llc %>% filter(year == "2010")), (0:nrow(data_llc %>% filter(year == "2010")) %/% (nrow(data_llc %>% filter(year == "2010")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_10[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2010 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_10[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2010_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_10 <- NULL

##### matches_ca_09_09 ####

data_ll_ca_09 <- split((data_llc %>% filter(year == "2009")), (0:nrow(data_llc %>% filter(year == "2009")) %/% (nrow(data_llc %>% filter(year == "2009")) / 2) ))


dbWriteTable(con, "llc_data_ca", data_ll_ca_09[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2009 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_09[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2009_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_09 <- NULL

##### matches_ca_08_08 ####

data_ll_ca_08 <- split((data_llc %>% filter(year == "2008")), (0:nrow(data_llc %>% filter(year == "2008")) %/% (nrow(data_llc %>% filter(year == "2008")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_08[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2008 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_08[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2008_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_08 <- NULL

##### matches_ca_07_07 ####

data_ll_ca_07 <- split((data_llc %>% filter(year == "2007")), (0:nrow(data_llc %>% filter(year == "2007")) %/% (nrow(data_llc %>% filter(year == "2007")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_07[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2007 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_07[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2007_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_07 <- NULL

##### matches_ca_06_06 ####

data_ll_ca_06 <- split((data_llc %>% filter(year == "2006")), (0:nrow(data_llc %>% filter(year == "2006")) %/% (nrow(data_llc %>% filter(year == "2006")) / 2) ))

dbWriteTable(con, "llc_data_ca", data_ll_ca_06[[1]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2006 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

# ca 2
dbWriteTable(con, "llc_data_ca", data_ll_ca_06[[2]] %>% select(file_number,
                                                               calif_address, 
                                                               calif_city, 
                                                               calif_zip) %>% filter(!is.na(calif_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_ca1;")
dbSendQuery(con, "CREATE TABLE llc_data_ca1 AS
                                   SELECT
                                   file_number,
                                   calif_address, 
                                   calif_city, 
                                   calif_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', concat_ws(' ', calif_address, calif_city), 'CA', calif_zip )))) AS tsq
                                   FROM llc_data_ca;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_ca1_idx ON llc_data_ca1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_ca2006_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_ca1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_ca1 ON (assessors_address.ts @@ llc_data_ca1.tsq);") 

data_ll_ca_06 <- NULL

#### pull back in

llc_ca18_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2018")

llc_ca18_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2018_2")
llc_ca17_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2017")

llc_ca17_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2017_2")

llc_ca16_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2016")

llc_ca16_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2016_2")

llc_ca15_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2015")

llc_ca15_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2015_2")

llc_ca14_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2014")

llc_ca14_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2014_2")

llc_ca13_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2013")

llc_ca13_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2013_2")

llc_ca12_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2012")

llc_ca12_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2012_2")

llc_ca11_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2011")

llc_ca11_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2011_2")

llc_ca10_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2010")

llc_ca10_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2010_2")

llc_ca09_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2009")

llc_ca09_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2009_2")

llc_ca08_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2008")

llc_ca08_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2008_2")

llc_ca07_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2007")

llc_ca07_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2007_2")

llc_ca06_1 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2006")

llc_ca06_2 <- dbGetQuery(con, "SELECT * FROM
           llc_ca2006_2")

Pattern1 <- grep("llc_ca",names(.GlobalEnv),value=TRUE)
listpat <- list(Pattern1)
Pattern1_list <- do.call(list, mget(Pattern1))
data_ca <- do.call( bind_rows, Pattern1_list )
data_ca1 <- data_ca %>% distinct(document_number, file_number, .keep_all = TRUE)
readr::write_csv(data_ca1, "~/R_Proj/data_out/var_ca.csv")

## clear out database
dbRemoveTable(con, "llc_ca2006")
dbRemoveTable(con, "llc_ca2006_2")
dbRemoveTable(con, "llc_ca2007")
dbRemoveTable(con, "llc_ca2007_2")
dbRemoveTable(con, "llc_ca2008")
dbRemoveTable(con, "llc_ca2008_2" )
dbRemoveTable(con, "llc_ca2009")
dbRemoveTable(con, "llc_ca2009_2" )
dbRemoveTable(con, "llc_ca2010")
dbRemoveTable(con, "llc_ca2010_2" )
dbRemoveTable(con, "llc_ca2011")
dbRemoveTable(con, "llc_ca2011_2" )
dbRemoveTable(con, "llc_ca2012")
dbRemoveTable(con, "llc_ca2012_2" )
dbRemoveTable(con, "llc_ca2013")
dbRemoveTable(con, "llc_ca2013_2" )
dbRemoveTable(con, "llc_ca2014")
dbRemoveTable(con, "llc_ca2014_2" )
dbRemoveTable(con, "llc_ca2015")
dbRemoveTable(con, "llc_ca2015_2" )
dbRemoveTable(con, "llc_ca2016")
dbRemoveTable(con, "llc_ca2016_2" )
dbRemoveTable(con, "llc_ca2017")
dbRemoveTable(con, "llc_ca2017_2" )
dbRemoveTable(con, "llc_ca2018")
dbRemoveTable(con, "llc_ca2018_2" )