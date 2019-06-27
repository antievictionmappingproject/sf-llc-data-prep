# LLC Network Lookup

# AEMP-OWN-IT PROPERTY  / LLC SUMMARY

 

## Overview

We are working on an add-on tool to  help connect owners actively or passively obfuscate, properties ownership through record naming - in particular connecting LLC names and properties to individuals and groups. 

 

## Problem:

Previous AEMP work has used record searches and database records to research individual landlords to determine and verify their property ownership.

 Since there is no specific database with unique identifiers for unique individuals this research is often labor and time intensive, but necessary do for individual bad-actor land lords and their associates.

## Basic Issues

The lack of unique identifiers make connecting particular individuals "John Smith" to all of their properties difficult.  The names recorded in assessors data can vary wildly based on how they were recorded (*"John Smith"*, *"J Smith"*) sometimes business (<i>"J Smith Property Co"</i> or spousal partners <i>"John and Jane Smith"/ "Jane and John Smith"</i>)  are included in the records, different addresses are used, or LLCs ("Smith Property Investors LLC") are used instead of individual names.  Often, determining large corporate owners are often easier since they use regular naming conventions - changing an LLC or ownership name once a year along with their investment patterns. 

 While these can be discerned on an individual basis by human search (see AEMP SF network analysis of bad landlords) doing it across the data.



## What We Are Doing To Make It Easier

To help activists and researchers, our work does several new things to make it easier to track owners, properties and 

### Name and Address Normalization



Addresses: Addresses can also be dirty but are the only way to link property ownership records from Assessors data to California State LLC record databases. While the Assessors addresses are "clean," LLC records are not verified and are simply transcribed from LLC documents in whatever form that they were hand-written. Joining address data is normally incredibly dirty, but we use a more complicated parsing tool to help match similar addresses. 

This allows us to match LLC partners and their agents to assessors records allowing the creation and tracking of networks of owners and their incorporations at city and state-wide scales/

 

## Work

Even with the above database linking tools and shortcuts, AEMP's work here can only make it easier for people to look at *possible* LLC and property correlations for searches. Human discernment and verification is still required to get a good sense if the matches make sense for large numbers of properties and owners.

Two pieces of open source tools can help: *Open Refine* and *Libpostal*

### OpenRefine and Property Records

The first piece allows for linking different but similar names to property  ownership within the assessors database to help link similar entity names to multiple properties. Ie if a LLC owned multiple properties, registering them under similar but not exactly the same names, and different registered mailing addresses.

If the owners registered their two homes using the same home mailing address, or the same ownership name they would be easily captured with a join or exact lookup.

| Property            | Owner                  | Owner Address              |
| ------------------- | ---------------------- | -------------------------- |
| 123 Mailing Address | J Smith Property Co    | 124 Home Address St        |
| 122 Mailing Address | John Smith Property Co | 124 Second Home Address St |

But since their names are slightly different, they can be discerned using human eyes knowing what to look ofr

Names: In the above examples - we use tools from OpenRefine to link and connect slightly different but similar names. Ie from the examples above *"John Smith",* *"J Smith"*, and even the couple names can be parsed together. (*"John Smith"*, *"J Smith"*) sometimes business (<i>"J Smith Property Co"</i> or spousal partners <i>"John and Jane Smith"/ "Jane and John Smith"</i>), this can be done manually through refine's factor classifier and search (do-able for individual searches), or programmatically through bindings for Python or R (for entire city-level searches).

### LLC Purchases - Address Normalization and Joins

What happens when the same person or group uses hides behind an LLC or many different LLCs to make purchases? 



| Property            | Owner (Per Assessor) | Owner (Hidden) | Purchase Address    |
| ------------------- | -------------------- | -------------- | ------------------- |
| 123 Mailing Address | MithS LLC            | J Smith        | 124 Lawyers Address |

| Entity    | Mailing Name | Mailing Address    |
| --------- | ------------ | ------------------ |
| MithS LLC | John Smith   | 124 Home Address S |

 How do we uncover or partially uncover LLCs? The second piece uses libpostal's address program which blows out fuzzy matcher to allow for non-perfect address joins.

<https://github.com/openvenues/libpostal/> 

<https://github.com/pramsey/pgsql-postal>  



## BRE Entity Data

| LLC_file_number | assessor_doc_id | parcel_id   | property_address                                         | mailing_match | ca_address_match | agent_match | partner1_match | partner2_match |
| --------------- | --------------- | ----------- | -------------------------------------------------------- | ------------- | ---------------- | ----------- | -------------- | -------------- |
| 201100310031    | 1115594         | 001-00-0001 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 824421          | 001-00-0002 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1537237         | 001-00-0003 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 950912          | 001-00-0004 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1448349         | 001-00-0005 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 711767          | 001-00-0006 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1781264         | 001-00-0007 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 521886          | 001-00-0008 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1369882         | 001-00-0009 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1619584         | 001-00-0010 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1481828         | 001-00-0011 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 556906          | 001-00-0012 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 556905          | 001-00-0013 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 1104601         | 001-00-0014 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 944123          | 001-00-0015 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310031    | 681547          | 001-00-0016 | 4700 merlin planta encino coahuila   91436               | 1             | 0                | 0           | 0              |                |
| 201100310035    | 1791047         | 001-00-0017 | 3621 windspun doctor huntington   beach california 92649 | 0             | 1                | 1           | 0              |                |
| 201100310035    | 2900547         | 001-00-0018 | 3621 windspun doctor huntington   beach california 92649 | 0             | 1                | 1           | 0              |                |





The mailing address (and name), 



The california address if the mailing address is out of state, and agent name/address, 



partner1  / partner2 - The first 2 partner names and addresses (if there are more  than 2 there is an indicator saying there are, but you have to go to  the original filing to hand copy the additional names out)

 

agent name - if the llc used an agent as as the intermediary.