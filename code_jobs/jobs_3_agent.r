library(RPostgres)
library(dplyr)
library(janitor)

'%!in%' <- function(x,y)!('%in%'(x,y))

pw <- {
  "password"
}

con <- dbConnect(RPostgres::Postgres(), dbname = "azaddb",
                 host = "localhost", port = 5432,
                 user = "azad", password = pw, bigint = "numeric")

data_llc <- readr::read_csv('~/R_Proj/data/LPMASTER.csv')

remove_1 <- c("SAME", ")", "SAME", ")",   "EXEC",     "70)",     "46)",  "20634)",      "1)",    "348)",      "ES",   "9282)", "EX", "0)", "E", "2)",  "X",    "181)",   "2723)", 
"6)", "208)",  "6", "RIEZ",  "PIER", "9", "GP1", "GP",  "OREXCO",  "SAMECA",    "476)", "EZ", "N",  "11", "LEE" ,  "1348)", "278)", "9)", "EC", "RC )",
"`", "1",   "36)",  "3665 H", "3X",    "446)",     "48)", "CEO", "WZ", ".",  "% KTBS",     "`EX",    "170)",   "2798)",    "282)",  "3 RIEZ" )

remove_2 <- c("EX"   ,"FLOOR",  "92199)", "TOWER")

data_llc <- data_llc %>% filter(agent_address_1 %!in% remove_1, agent_address_2 %!in% remove_2)

data_llc <- data_llc %>% mutate(agent_address = case_when(
  is.na(agent_city) ~ NA_character_,
  !is.na(agent_address_2) ~ paste(agent_address_1, agent_address_2, sep = " "),
  is.na(agent_address_2) ~ agent_address_1,
  TRUE ~ agent_address_1
))

data_llc <- data_llc %>% mutate(year = substr(file_date, 1, 4))


# agent_name0 <- (data_llc %>% filter( nchar(agent_address) < 7)) %>% select(agent_address)
# filter_repeat_list0 <- names(summary( as.factor((agent_name0 )$agent_address) ))[(summary( as.factor((agent_name0)$agent_address) )) >2]
# 
# agent_name <- (data_llc %>% filter( nchar(agent_address_1) < 7)) %>% select(agent_address_1)
# filter_repeat_list <- names(summary( as.factor((agent_name )$agent_address_1) ))[(summary( as.factor((agent_name )$agent_address_1) )) >2]
# 
# agent_name_2 <- (data_llc %>% filter( nchar(agent_address_2) < 7)) %>% select(agent_address_2)
# filter_repeat_list_2 <- names(summary( as.factor((agent_name_2 )$agent_address_2) ))[(summary( as.factor((agent_name_2 )$agent_address_2) )) >2]
# 

filter_repeat_list1 <- as.data.frame(filter_repeat_list)
filter_repeat_list1 <- filter_repeat_list1 %>%  inner_join(data_llc, by= c("filter_repeat_list" = "agent_address_1"), keep = TRUE)  %>% select(filter_repeat_list, agent_names, file_number )

data_llc <- data_llc %>% filter(agent_address_1 %!in% filter_repeat_list)
data_llc <- data_llc %>% filter(agent_address != ".")
##### matches_agent_18_18 ####


data_ll_agent_18 <- split((data_llc %>% filter(year == "2018")), (0:nrow(data_llc %>% filter(year == "2018")) %/% (nrow(data_llc %>% filter(year == "2018")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_18[[1]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

# dbSendQuery(con, "DROP TABLE IF EXISTS llc_agent2018;")
dbSendQuery(con,  "CREATE TABLE llc_agent2018 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_18[[2]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2018_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

data_ll_agent_18 <- NULL


#llc_agent18_1 <- dbGetQuery(con, "SELECT * FROM
#           llc_agent2018")

#llc_agent18_2 <- dbGetQuery(con, "SELECT * FROM
#           llc_agent2018_2")



##### matches_agent_17_17 ####


data_ll_agent_17 <- split((data_llc %>% filter(year == "2017")), (0:nrow(data_llc %>% filter(year == "2017")) %/% (nrow(data_llc %>% filter(year == "2017")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_17[[1]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2017 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_17[[2]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2017_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

data_ll_agent_17 <- NULL


##### matches_agent_16_16 ####


data_ll_agent_16 <- split((data_llc %>% filter(year == "2016")), (0:nrow(data_llc %>% filter(year == "2016")) %/% (nrow(data_llc %>% filter(year == "2016")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_16[[1]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2016 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_16[[2]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2016_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

data_ll_agent_16 <- NULL


##### matches_agent_15_15 ####


data_ll_agent_15 <- split((data_llc %>% filter(year == "2015")), (0:nrow(data_llc %>% filter(year == "2015")) %/% (nrow(data_llc %>% filter(year == "2015")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_15[[1]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2015 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_15[[2]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2015_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

data_ll_agent_15 <- NULL


##### matches_agent_14_14 ####


data_ll_agent_14 <- split((data_llc %>% filter(year == "2014")), (0:nrow(data_llc %>% filter(year == "2014")) %/% (nrow(data_llc %>% filter(year == "2014")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_14[[1]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2014 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_14[[2]] %>% select(file_number,
                                                                     agent_address,
                                                                     agent_city,
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address,
                                   agent_city,
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2014_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);")

data_ll_agent_14 <- NULL

##### matches_agent_13_13 ####


data_ll_agent_13 <- split((data_llc %>% filter(year == "2013")), (0:nrow(data_llc %>% filter(year == "2013")) %/% (nrow(data_llc %>% filter(year == "2013")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_13[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2013 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_13[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2013_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_13 <- NULL


##### matches_agent_12_12 ####


data_ll_agent_12 <- split((data_llc %>% filter(year == "2012")), (0:nrow(data_llc %>% filter(year == "2012")) %/% (nrow(data_llc %>% filter(year == "2012")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_12[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2012 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_12[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2012_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_12 <- NULL

##### matches_agent_11_11 ####


data_ll_agent_11 <- split((data_llc %>% filter(year == "2011")), (0:nrow(data_llc %>% filter(year == "2011")) %/% (nrow(data_llc %>% filter(year == "2011")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_11[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2011 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_11[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2011_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_11 <- NULL

##### matches_agent_10_10 ####


data_ll_agent_10 <- split((data_llc %>% filter(year == "2010")), (0:nrow(data_llc %>% filter(year == "2010")) %/% (nrow(data_llc %>% filter(year == "2010")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_10[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2010 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_10[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2010_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_10 <- NULL

##### matches_agent_09_09 ####

data_ll_agent_09 <- split((data_llc %>% filter(year == "2009")), (0:nrow(data_llc %>% filter(year == "2009")) %/% (nrow(data_llc %>% filter(year == "2009")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_09[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2009 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_09[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2009_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_09 <- NULL


##### matches_agent_08_08 ####


data_ll_agent_08 <- split((data_llc %>% filter(year == "2008")), (0:nrow(data_llc %>% filter(year == "2008")) %/% (nrow(data_llc %>% filter(year == "2008")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_08[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2008 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_08[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2008_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_08 <- NULL


##### matches_agent_07_07 ####


data_ll_agent_07 <- split((data_llc %>% filter(year == "2007")), (0:nrow(data_llc %>% filter(year == "2007")) %/% (nrow(data_llc %>% filter(year == "2007")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_07[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2007 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_07[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2007_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_07 <- NULL


##### matches_agent_06_06 ####


data_ll_agent_06 <- split((data_llc %>% filter(year == "2006")), (0:nrow(data_llc %>% filter(year == "2006")) %/% (nrow(data_llc %>% filter(year == "2006")) / 2) ))


dbWriteTable(con, "llc_data_agent", data_ll_agent_06[[1]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2006 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

# agent 2
dbWriteTable(con, "llc_data_agent", data_ll_agent_06[[2]] %>% select(file_number,
                                                                     agent_address, 
                                                                     agent_city, 
                                                                     agent_state,
                                                                     agent_zip) %>% filter(!is.na(agent_address)), overwrite = TRUE, row.names = FALSE)

dbSendQuery(con, "DROP TABLE IF EXISTS llc_data_agent1;")
dbSendQuery(con, "CREATE TABLE llc_data_agent1 AS
                                   SELECT
                                   file_number,
                                   agent_address, 
                                   agent_city, 
                                   agent_state,
                                   agent_zip,
                                   phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', agent_address, agent_city, agent_state, agent_zip )))) AS tsq
                                   FROM llc_data_agent;
                                   ")
dbSendQuery(con, "CREATE INDEX llc_data_agent1_idx ON llc_data_agent1 USING GIST (tsq);" )

dbSendQuery(con,  "CREATE TABLE llc_agent2006_2 AS SELECT
                                   assessors_address.*,
                                   llc_data_agent1.file_number
                                   FROM assessors_address
                                   JOIN llc_data_agent1 ON (assessors_address.ts @@ llc_data_agent1.tsq);") 

data_ll_agent_06 <- NULL


#### pull back in

llc_agent18_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2018") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent18_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2018_2") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent17_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2017") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent17_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2017_2") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent16_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2016") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent16_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2016_2") %>% distinct(document_number, file_number, .keep_all = TRUE)

llc_agent15_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2015") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent15_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2015_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent14_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2014") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent14_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2014_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent13_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2013") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent13_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2013_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent12_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2012") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent12_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2012_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)


llc_agent11_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2011") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent11_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2011_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent10_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2010") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent10_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2010_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent09_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2009") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent09_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2009_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent08_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2008") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent08_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2008_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent07_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2007") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent07_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2007_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent06_1 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2006") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

llc_agent06_2 <- dbGetQuery(con, "SELECT * FROM
           llc_agent2006_2") %>%  filter(file_number %!in% filter_repeat_list1$file_number)

Pattern1 <- grep("llc_agent",names(.GlobalEnv),value=TRUE)
listpat <- list(Pattern1)
Pattern1_list <- do.call(list, mget(Pattern1))
data_agent <- do.call( bind_rows, Pattern1_list )

readr::write_csv(data_agent1, "~/R_Proj/data_out/var_agent06-09.csv")


### level checks
# data_agent1 <- NULL
# levs <- summary(as.factor(data_agent$file_number))
# View(levs)


# dbRemoveTable(con, "llc_agent2006")
# dbRemoveTable(con, "llc_agent2006_2")
# dbRemoveTable(con, "llc_agent2007")
# dbRemoveTable(con, "llc_agent2007_2")
# dbRemoveTable(con, "llc_agent2008")
# dbRemoveTable(con, "llc_agent2008_2" )
# dbRemoveTable(con, "llc_agent2009")
# dbRemoveTable(con, "llc_agent2009_2" )
# dbRemoveTable(con, "llc_agent2010")
# dbRemoveTable(con, "llc_agent2010_2" )
# dbRemoveTable(con, "llc_agent2011")
# dbRemoveTable(con, "llc_agent2011_2" )
# dbRemoveTable(con, "llc_agent2012")
# dbRemoveTable(con, "llc_agent2012_2" )
# dbRemoveTable(con, "llc_agent2013")
# dbRemoveTable(con, "llc_agent2013_2" )
# dbRemoveTable(con, "llc_agent2014")
# dbRemoveTable(con, "llc_agent2014_2" )
# dbRemoveTable(con, "llc_agent2015")
# dbRemoveTable(con, "llc_agent2015_2" )
# dbRemoveTable(con, "llc_agent2016")
# dbRemoveTable(con, "llc_agent2016_2" )
# dbRemoveTable(con, "llc_agent2017")
# dbRemoveTable(con, "llc_agent2017_2" )
# dbRemoveTable(con, "llc_agent2018")
# dbRemoveTable(con, "llc_agent2018_2" )


