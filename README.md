# eBird Statistical Report Generator

This code generates a PDF (via latex) of an Annual eBird Statistical Report for a county, state, or country.
Before use, you must request and download an eBird dataset for the region in question.

## Getting Started

Requirements:

-   Python 3.x
-   PostgreSQL
-   eBird data for the region in question, access requests and downloads at https://ebird.org/data/download

## To load data

-   Install PostgreSQL
-   Download data
-   Look in the file `ebird-create-db-and-load-ebd.sql` and edit to suit. Especially, you'll want to change this line to suit:
    `DECLARE ebird_export_path CONSTANT VARCHAR := 'C:\...';`
-   Run this command: `psql -U postgres -d template1 -f ebird-create-db-and-load-ebd.sql`
-   If you have additional eBird data dumps to load, you can edit `ebird-load-ebd.sql` in the same way, then:
-   `psql -U postgres -d template1 -f ebird-load-ebd.sql`

## To run

`python manage.py year_end_report -r <region_code> -y <year>` where region_code is, eg, `US-DC-001` or `US-MD`.

## Existing outputs

Here is the [2020 District of Columbia eBird report](https://github.com/ses4j/ebird-statistical-report/raw/main/2020%20Annual%20eBird%20Statistical%20Report%20-%20US-DC-001%20-%20v1.1.pdf), generated with this tool.

Some other generated reports can be found here:

https://drive.google.com/drive/folders/1UomIEXxTC4d2qzjap1W2McZqNT4SbsH7?usp=sharing
