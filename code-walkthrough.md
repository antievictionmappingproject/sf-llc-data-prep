# Address Normalization Walk Through

### Basic Issue

We have two main files with addresses that we want to join - <u>parcel / ownership data</u> with relatively clean assessor's data (with addresses), and <u>llc data</u> - with much more dirty addresses that we want to normalize, and join.



## Pseudo Code 

#### code_setup.R

We first load the assessor's address table into the database as **assessors_data**, then we create a second table **addresses_normalized** that creates <u>na</u> a normalized address field using *postal_normalize* function. That field is re-joined to the assessors data using the <u>document_number</u> (passing note, this document number may not be a correct field to join across).

### code_jobs

Each of the script files in **code_join** directory works over various columns in the Business Record Entity llc file that have address fields. These are not necessarily available for each llc.



| Field    | What it is                                                   |
| -------- | ------------------------------------------------------------ |
| Mailing  | Address of LLC                                               |
| CA       | California recording address of an entity that is based outside of the state |
| Agent    | Listing agent / lawyer legally required for the LLC filing   |
| Partner1 | First partner listed in the filing document                  |
| Partner2 | Second partner listed in the filing document *               |

*-  Only the first listed two partners in the filed data are recorded in the data set

Each file runs through data in the BRE data for each year, creates a tokenized address field that gets used in a later table



```sql
DROP TABLE IF EXISTS llc_data_mail1;
CREATE TABLE llc_data_mail1 AS
SELECT
file_number,
mailing_address, 
mailing_city, 
mailing_state,
mailing_zip,
phraseto_tsquery('simple', unnest(postal_normalize(concat_ws(', ', mailing_address, mailing_city, mailing_state, mailing_zip )))) AS tsq
                                   FROM llc_data_mail;
                                   ");
                                   
```



```plsql
CREATE TABLE llc_mailing 
AS SELECT                                   assessors_address.*,                           llc_data_mail1.file_number
FROM 
assessors_address
JOIN llc_data_mail1 ON (assessors_address.ts @@ llc_data_mail1.tsq);
```

