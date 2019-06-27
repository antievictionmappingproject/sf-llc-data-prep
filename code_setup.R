# library('RPostgreSQL')
# devtools::install_github("r-dbi/RPostgres")
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

## assessors ####
data_ass <- data.table::fread("/home/azad/data/name_addresses.csv")
names(data_ass) <- c("Document_Number", "Buyer", "First_Owner_Full_Name", "Parcel_Number", "First_Owner_Name", "Legal_Description", "Legal_Description_Part2", "Mailing_Address", "Mailing_Address_City", "Mailing_Address_Zip", "Non_Money_Sale_Date", "Owner_Absent_Occupied", "Ownership_Description", "Previous_Buyer", "Previous_Sale_Date", "Previous_Seller", "Sale_Date", "Second_Owner_Full_Name", "Second_Owner_Name", "Seller", "Site_Address", "Site_Address_City_State", "Use_Code")

data_ass <- data_ass %>% clean_names()
data_ass <- data_ass %>% select(-legal_description, -legal_description_part2)

# dbGetQuery(con, "TRUNCATE TABLE assessors_data")
dbWriteTable(con, "assessors_data", data_ass, overwrite = TRUE, row.names = FALSE)
dbSendQuery(con, "ALTER TABLE assessors_data ADD COLUMN pk SERIAL PRIMARY KEY;")
dbSendQuery(con, "DROP INDEX IF EXISTS assessors_data_idx;")
dbSendQuery(con, "CREATE INDEX assessors_data_idx on assessors_data (pk);")

dbSendQuery(con,"DROP TABLE IF EXISTS addresses_normalized;" )
dbSendQuery(con,
            "CREATE TABLE addresses_normalized AS 
            SELECT pk, 
            na,
            to_tsvector('simple', na) AS ts 
            FROM ( 
            SELECT pk, 
            unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_address_city, 'CA', mailing_address_zip))) AS na 
            FROM assessors_data ) 
            AS subq1;")
dbSendQuery(con, "DROP INDEX IF EXISTS addresses_normalized_idx;" )
dbSendQuery(con, "CREATE INDEX addresses_normalized_idx ON addresses_normalized USING GIN (ts);" )


dbSendQuery(con, "DROP TABLE IF EXISTS assessors_address;")
dbSendQuery(con,            
                  "CREATE TABLE assessors_address AS 
                  SELECT
                  assessors_data.document_number,
                  addresses_normalized.*
                  FROM assessors_data
                  JOIN addresses_normalized using (pk);") 

dbSendQuery(con, "DROP INDEX IF EXISTS assessors_address_idx;" )
dbSendQuery(con, "CREATE INDEX assessors_address_idx ON assessors_address USING GIST (ts);" )
