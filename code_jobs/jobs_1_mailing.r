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


'%!in%' <- function(x,y)!('%in%'(x,y))
data_llc <- readr::read_csv('/home/azad/data/LPMASTER.csv')

remove_1 <- c("ADDR", "SAME", ")", "SAME", ")",   "EXEC",     "70)",     "46)",  "20634)",      "1)",    "348)",      "ES",   "9282)", "EX", "0)", "E", "2)",  "X",    "181)",   "2723)", 
              "6)", "208)",  "6", "RIEZ",  "PIER", "9", "GP1", "GP",  "OREXCO",  "SAMECA",    "476)", "EZ", "N",  "11", "LEE" ,  "1348)", "278)", "9)", "EC", "RC )",
              "`", "1",   "36)",  "3665 H", "3X",    "446)",     "48)", "CEO", "WZ", ".",  "% KTBS",     "`EX",    "170)",   "2798)",    "282)",  "3 RIEZ", "EX"   ,"FLOOR",  "92199)", "TOWER")

remove_2 <- c("EEDDG", "POB 9", "GGGGG", 	"RR 2", "S", "AA", "2", "NONE" )

data_llc <- (data_llc %>% filter(mailing_address %!in% remove_1))
data_llc <- (data_llc %>% filter(mailing_address %!in% remove_2))

remove_others <- (data_llc %>% filter(nchar( mailing_address) < 6))$mailing_address
data_llc <- (data_llc %>% filter(mailing_address %!in% remove_others))

data_llc <- data_llc %>% mutate(year = substr(file_date, 1, 4))
## mailing address 18 - 18

data_ll_mail_18 <- split((data_llc %>% filter(year == "2018")), (0:nrow(data_llc %>% filter(year == "2018")) %/% (nrow(data_llc %>% filter(year == "2018")) / 2) ))
# data_llc2018[1]
dbWriteTable(con, "llc_data_mail", data_ll_mail_18[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2018 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_18[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2018_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_17_17 ####


data_ll_mail_17 <- split((data_llc %>% filter(year == "2017")), (0:nrow(data_llc %>% filter(year == "2017")) %/% (nrow(data_llc %>% filter(year == "2017")) / 2) ))
# data_llc2017[1]
dbWriteTable(con, "llc_data_mail", data_ll_mail_17[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2017 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_17[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2017_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_16_16 ####


data_ll_mail_16 <- split((data_llc %>% filter(year == "2016")), (0:nrow(data_llc %>% filter(year == "2016")) %/% (nrow(data_llc %>% filter(year == "2016")) / 2) ))
# data_llc2016[1]
dbWriteTable(con, "llc_data_mail", data_ll_mail_16[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2016 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_16[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2016_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_15_15 ####


data_ll_mail_15 <- split((data_llc %>% filter(year == "2015")), (0:nrow(data_llc %>% filter(year == "2015")) %/% (nrow(data_llc %>% filter(year == "2015")) / 2) ))
# data_llc2015[1]
dbWriteTable(con, "llc_data_mail", data_ll_mail_15[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2015 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_15[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2015_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_14_14 ####


data_ll_mail_14 <- split((data_llc %>% filter(year == "2014")), (0:nrow(data_llc %>% filter(year == "2014")) %/% (nrow(data_llc %>% filter(year == "2014")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_14[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2014 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_14[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2014_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_13_13 ####


data_ll_mail_13 <- split((data_llc %>% filter(year == "2013")), (0:nrow(data_llc %>% filter(year == "2013")) %/% (nrow(data_llc %>% filter(year == "2013")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_13[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2013 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_13[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2013_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_12_12 ####


data_ll_mail_12 <- split((data_llc %>% filter(year == "2012")), (0:nrow(data_llc %>% filter(year == "2012")) %/% (nrow(data_llc %>% filter(year == "2012")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_12[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2012 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_12[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2012_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 



##### matches_mailing_11_11 ####


data_ll_mail_11 <- split((data_llc %>% filter(year == "2011")), (0:nrow(data_llc %>% filter(year == "2011")) %/% (nrow(data_llc %>% filter(year == "2011")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_11[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2011 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_11[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2011_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_10_10 ####


data_ll_mail_10 <- split((data_llc %>% filter(year == "2010")), (0:nrow(data_llc %>% filter(year == "2010")) %/% (nrow(data_llc %>% filter(year == "2010")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_10[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2010 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_10[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2010_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_09_09 ####


data_ll_mail_09 <- split((data_llc %>% filter(year == "2009")), (0:nrow(data_llc %>% filter(year == "2009")) %/% (nrow(data_llc %>% filter(year == "2009")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_09[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2009 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_09[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2009_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_08_08 ####


data_ll_mail_08 <- split((data_llc %>% filter(year == "2008")), (0:nrow(data_llc %>% filter(year == "2008")) %/% (nrow(data_llc %>% filter(year == "2008")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_08[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2008 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_08[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2008_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_07_07 ####


data_ll_mail_07 <- split((data_llc %>% filter(year == "2007")), (0:nrow(data_llc %>% filter(year == "2007")) %/% (nrow(data_llc %>% filter(year == "2007")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_07[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2007 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_07[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2007_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


##### matches_mailing_06_06 ####


data_ll_mail_06 <- split((data_llc %>% filter(year == "2006")), (0:nrow(data_llc %>% filter(year == "2006")) %/% (nrow(data_llc %>% filter(year == "2006")) / 2) ))


dbWriteTable(con, "llc_data_mail", data_ll_mail_06[[1]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2006 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 

# mail 2
dbWriteTable(con, "llc_data_mail", data_ll_mail_06[[2]] %>% select(file_number,
                                                                   mailing_address, 
                                                                   mailing_city, 
                                                                   mailing_state,
                                                                   mailing_zip) %>% filter(!is.na(mailing_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_mail1;")
dbSendQuery(con, "CREATE TABLE llc_data_mail1 AS
                                   SELECT
                                   file_number,
                                   mailing_address, 
                                   mailing_city, 
                                   mailing_state,
                                   mailing_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_mail1_idx ON llc_data_mail1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_mailing2006_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_mail1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);") 


### pull all data

llc_mailing18_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2018")
# llc_mailing18_1a <- llc_mailing18_1 %>% distinct(document_number, file_number, .keep_all = TRUE)


llc_mailing18_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2018_2")
llc_mailing17_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2017")

llc_mailing17_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2017_2")

llc_mailing16_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2016")

llc_mailing16_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2016_2")

llc_mailing15_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2015")

llc_mailing15_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2015_2")

llc_mailing14_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2014")

llc_mailing14_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2014_2")

llc_mailing13_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2013")

llc_mailing13_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2013_2")


llc_mailing12_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2012")

llc_mailing12_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2012_2")


llc_mailing11_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2011")

llc_mailing11_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2011_2")

llc_mailing10_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2010")

llc_mailing10_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2010_2")

llc_mailing09_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2009")

llc_mailing09_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2009_2")

llc_mailing08_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2008")

llc_mailing08_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2008_2")

llc_mailing07_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2007")

llc_mailing07_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2007_2")

llc_mailing06_1 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2006")

llc_mailing06_2 <- dbGetQuery(con, "SELECT * FROM
           llc_mailing2006_2")

Pattern1 <- grep("llc_mailing",names(.GlobalEnv),value=TRUE)
listpat <- list(Pattern1)
Pattern1_list <- do.call(list, mget(Pattern1))
data_mailing <- do.call( bind_rows, Pattern1_list )
data_mailing1 <- data_mailing %>% distinct(document_number, file_number, .keep_all = TRUE)
readr::write_csv(data_mailing1, "~/R_Proj/data_out/var_mailing_2.csv")
# map2_df(Pattern1_list, names(Pattern1_list), ~ mutate(.x, ID = .y)) 
# args1 <- list("document_number", "file_number", ".keep_all = TRUE")
dbRemoveTable(con, "llc_mailing2006")
dbRemoveTable(con, "llc_mailing2006_2")
dbRemoveTable(con, "llc_mailing2007")
dbRemoveTable(con, "llc_mailing2007_2")
dbRemoveTable(con, "llc_mailing2008")
dbRemoveTable(con, "llc_mailing2008_2" )
dbRemoveTable(con, "llc_mailing2009")
dbRemoveTable(con, "llc_mailing2009_2" )
dbRemoveTable(con, "llc_mailing2010")
dbRemoveTable(con, "llc_mailing2010_2" )
dbRemoveTable(con, "llc_mailing2011")
dbRemoveTable(con, "llc_mailing2011_2" )
dbRemoveTable(con, "llc_mailing2012")
dbRemoveTable(con, "llc_mailing2012_2" )
dbRemoveTable(con, "llc_mailing2013")
dbRemoveTable(con, "llc_mailing2013_2" )
dbRemoveTable(con, "llc_mailing2014")
dbRemoveTable(con, "llc_mailing2014_2" )
dbRemoveTable(con, "llc_mailing2015")
dbRemoveTable(con, "llc_mailing2015_2" )
dbRemoveTable(con, "llc_mailing2016")
dbRemoveTable(con, "llc_mailing2016_2" )
dbRemoveTable(con, "llc_mailing2017")
dbRemoveTable(con, "llc_mailing2017_2" )
dbRemoveTable(con, "llc_mailing2018")
dbRemoveTable(con, "llc_mailing2018_2" )



