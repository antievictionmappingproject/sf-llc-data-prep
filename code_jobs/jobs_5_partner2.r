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

data_llc <- data_llc %>% mutate(agent_address = case_when(
  !is.na(agent_address_2) ~ agent_address_2,
  is.na(agent_address_2) ~ agent_address_1,
  TRUE ~ agent_address_1
))


'%!in%' <- function(x,y)!('%in%'(x,y))
data_llc <- readr::read_csv('/home/azad/data/LPMASTER.csv')

remove_1 <- c("ADDR", "SAME", ")", "SAME", ")",   "EXEC",     "70)",     "46)",  "20634)",      "1)",    "348)",      "ES",   "9282)", "EX", "0)", "E", "2)",  "X",    "181)",   "2723)", 
              "6)", "208)",  "6", "RIEZ",  "PIER", "9", "GP1", "GP",  "OREXCO",  "SAMECA",    "476)", "EZ", "N",  "11", "LEE" ,  "1348)", "278)", "9)", "EC", "RC )",
              "`", "1",   "36)",  "3665 H", "3X",    "446)",     "48)", "CEO", "WZ", ".",  "% KTBS",     "`EX",    "170)",   "2798)",    "282)",  "3 RIEZ", "EX"   ,"FLOOR",  "92199)", "TOWER")

remove_2 <- c("EEDDG", "POB 9", "GGGGG",  "RR 2", "S", "AA", "2", "NONE")

data_llc <- (data_llc %>% filter(partner_2_address %!in% remove_1))
data_llc <- (data_llc %>% filter(partner_2_address %!in% remove_2))

remove_others <- (data_llc %>% filter(nchar( partner_2_address) < 6))$partner_2_address
data_llc <- (data_llc %>% filter(partner_2_address %!in% remove_others))

data_llc <- data_llc %>% mutate(year = substr(file_date, 1, 4))

##### matches_partner_2_18_18 ####

data_ll_partner_2_18 <- split((data_llc %>% filter(year == "2018")), (0:nrow(data_llc %>% filter(year == "2018")) %/% (nrow(data_llc %>% filter(year == "2018")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_18[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22018 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_18[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22018_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_17_17 ####

data_ll_partner_2_17 <- split((data_llc %>% filter(year == "2017")), (0:nrow(data_llc %>% filter(year == "2017")) %/% (nrow(data_llc %>% filter(year == "2017")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_17[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22017 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_17[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22017_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 


##### matches_partner_2_16_16 ####

data_ll_partner_2_16 <- split((data_llc %>% filter(year == "2016")), (0:nrow(data_llc %>% filter(year == "2016")) %/% (nrow(data_llc %>% filter(year == "2016")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_16[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22016 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_16[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22016_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_15_15 ####

data_ll_partner_2_15 <- split((data_llc %>% filter(year == "2015")), (0:nrow(data_llc %>% filter(year == "2015")) %/% (nrow(data_llc %>% filter(year == "2015")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_15[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22015 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_15[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22015_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_14_14 ####

data_ll_partner_2_14 <- split((data_llc %>% filter(year == "2014")), (0:nrow(data_llc %>% filter(year == "2014")) %/% (nrow(data_llc %>% filter(year == "2014")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_14[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22014 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_14[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22014_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_13_13 ####

data_ll_partner_2_13 <- split((data_llc %>% filter(year == "2013")), (0:nrow(data_llc %>% filter(year == "2013")) %/% (nrow(data_llc %>% filter(year == "2013")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_13[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22013 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_13[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22013_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_12_12 ####

data_ll_partner_2_12 <- split((data_llc %>% filter(year == "2012")), (0:nrow(data_llc %>% filter(year == "2012")) %/% (nrow(data_llc %>% filter(year == "2012")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_12[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22012 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_12[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22012_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_11_11 ####

data_ll_partner_2_11 <- split((data_llc %>% filter(year == "2011")), (0:nrow(data_llc %>% filter(year == "2011")) %/% (nrow(data_llc %>% filter(year == "2011")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_11[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22011 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_11[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22011_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_10_10 ####

data_ll_partner_2_10 <- split((data_llc %>% filter(year == "2010")), (0:nrow(data_llc %>% filter(year == "2010")) %/% (nrow(data_llc %>% filter(year == "2010")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_10[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22010 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_10[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22010_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_09_09 ####

data_ll_partner_2_09 <- split((data_llc %>% filter(year == "2009")), (0:nrow(data_llc %>% filter(year == "2009")) %/% (nrow(data_llc %>% filter(year == "2009")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_09[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22009 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_09[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22009_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_08_08 ####

data_ll_partner_2_08 <- split((data_llc %>% filter(year == "2008")), (0:nrow(data_llc %>% filter(year == "2008")) %/% (nrow(data_llc %>% filter(year == "2008")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_08[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22008 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_08[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22008_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_07_07 ####

data_ll_partner_2_07 <- split((data_llc %>% filter(year == "2007")), (0:nrow(data_llc %>% filter(year == "2007")) %/% (nrow(data_llc %>% filter(year == "2007")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_07[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22007 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_07[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22007_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

##### matches_partner_2_06_06 ####

data_ll_partner_2_06 <- split((data_llc %>% filter(year == "2006")), (0:nrow(data_llc %>% filter(year == "2006")) %/% (nrow(data_llc %>% filter(year == "2006")) / 2) ))

dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_06[[1]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22006 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 

# partner_2 2
dbWriteTable(con, "llc_data_partner_2", data_ll_partner_2_06[[2]] %>% select(file_number,
                                                                             partner_2_address, 
                                                                             partner_2_city, 
                                                                             partner_2_state,
                                                                             partner_2_zip) %>% filter(!is.na(partner_2_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_partner_21;")
dbSendQuery(con, "CREATE TABLE llc_data_partner_21 AS
                                   SELECT
                                   file_number,
                                   partner_2_address, 
                                   partner_2_city, 
                                   partner_2_state,
                                   partner_2_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', partner_2_address, partner_2_city, partner_2_state, partner_2_zip )))) AS tsq
                                   FROM llc_data_partner_2;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_partner_21_idx ON llc_data_partner_21 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_partner_22006_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_partner_21.file_number
                                   FROM assessors_address
                                   JOIN llc_data_partner_21 ON (assessors_address.ts @@ llc_data_partner_21.tsq);") 


llc_partner_218_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22018")

llc_partner_218_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22018_2")
llc_partner_217_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22017")

llc_partner_217_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22017_2")

llc_partner_216_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22016")

llc_partner_216_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22016_2")

llc_partner_215_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22015")

llc_partner_215_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22015_2")

llc_partner_214_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22014")

llc_partner_214_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22014_2")

llc_partner_213_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22013")

llc_partner_213_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22013_2")

llc_partner_212_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22012")

llc_partner_212_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22012_2")

llc_partner_211_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22011")

llc_partner_211_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22011_2")

llc_partner_210_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22010")

llc_partner_210_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22010_2")

llc_partner_209_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22009")

llc_partner_209_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22009_2")

llc_partner_208_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22008")

llc_partner_208_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22008_2")

llc_partner_207_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22007")

llc_partner_207_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22007_2")

llc_partner_206_1 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22006")

llc_partner_206_2 <- dbGetQuery(con, "SELECT * FROM
           llc_partner_22006_2")

Pattern1 <- grep("llc_partner_2",names(.GlobalEnv),value=TRUE)
listpat <- list(Pattern1)
Pattern1_list <- do.call(list, mget(Pattern1))
data_partner_2 <- do.call( bind_rows, Pattern1_list )
data_partner_21 <- data_partner_2 %>% distinct(document_number, file_number, .keep_all = TRUE)
readr::write_csv(data_partner_21, "~/R_Proj/data_out/var_partner_2.csv")

dbRemoveTable(con, "llc_partner_22006")
dbRemoveTable(con, "llc_partner_22006_2")
dbRemoveTable(con, "llc_partner_22007")
dbRemoveTable(con, "llc_partner_22007_2")
dbRemoveTable(con, "llc_partner_22008")
dbRemoveTable(con, "llc_partner_22008_2" )
dbRemoveTable(con, "llc_partner_22009")
dbRemoveTable(con, "llc_partner_22009_2" )
dbRemoveTable(con, "llc_partner_22010")
dbRemoveTable(con, "llc_partner_22010_2" )
dbRemoveTable(con, "llc_partner_22011")
dbRemoveTable(con, "llc_partner_22011_2" )
dbRemoveTable(con, "llc_partner_22012")
dbRemoveTable(con, "llc_partner_22012_2" )
dbRemoveTable(con, "llc_partner_22013")
dbRemoveTable(con, "llc_partner_22013_2" )
dbRemoveTable(con, "llc_partner_22014")
dbRemoveTable(con, "llc_partner_22014_2" )
dbRemoveTable(con, "llc_partner_22015")
dbRemoveTable(con, "llc_partner_22015_2" )
dbRemoveTable(con, "llc_partner_22016")
dbRemoveTable(con, "llc_partner_22016_2" )
dbRemoveTable(con, "llc_partner_22017")
dbRemoveTable(con, "llc_partner_22017_2" )
dbRemoveTable(con, "llc_partner_22018")
dbRemoveTable(con, "llc_partner_22018_2" )


