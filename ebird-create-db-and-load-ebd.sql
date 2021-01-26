/*
This script will create a database called ebirddb with a table called ebird.
It will import the standard eBird export format file, as specified by `ebird_export_path` below into 
the new table.  Then it will create some indexes and geo points.  Customize to suit.
*/

-- Set the desired tablespace, if you want, to put it on a drive with room.  Skip this if desired.
/*
CREATE TABLESPACE ebird_db_space LOCATION 'C:\ebird-tablespace';
*/

SET default_tablespace = ebird_db_space;

\set ON_ERROR_STOP on

-- Create a new database:

SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'ebird' AND pid <> pg_backend_pid();
ALTER DATABASE ebirddb RENAME TO ebirddb_old;
-- DROP DATABASE if exists "ebirddb";
CREATE DATABASE "ebirddb" WITH ENCODING 'UTF8' TEMPLATE=template0;

\c "ebirddb"

DO $$
DECLARE ebird_export_path CONSTANT VARCHAR := 'C:\ebird-tablespace\ebd_US-DC_prv_relDec-2020\ebd_US-DC_prv_relDec-2020.txt';
BEGIN

CREATE TABLE "ebird"
(
    GLOBAL_UNIQUE_IDENTIFIER     char(50),         -- always 45-47 characters needed (so far)
    CATEGORY                     varchar(20),      -- Probably 10 would be safe
    COMMON_NAME                  varchar(70),      -- Some hybrids have really long names
    SUBSPECIES_COMMON_NAME       varchar(70),      --  ''
    OBSERVATION_COUNT_STR            varchar(8),       -- Someone saw 1.3 million Auklets.
    BREEDING_BIRD_ATLAS_CODE     varchar(2),
    BREEDING_BIRD_ATLAS_CATEGORY varchar(2),
    COUNTRY                      varchar(50),      -- long enough for "Saint Helena, Ascension and Tristan da Cunha"
    COUNTRY_CODE                 char(2),          -- alpha-2 codes
    STATE                        varchar(50),      -- no idea if this is long enough? U.S. Virgin Islands may be almost 30
    STATE_CODE                   varchar(30),
    COUNTY                       varchar(50),      -- no idea if this is long enough? U.S. Virgin Islands may be almost 30
    COUNTY_CODE                  varchar(30),
    ATLAS_BLOCK                  varchar(20),      -- i think max 10
    LOCALITY                     text,             -- unstructured/potentially long
    LOCALITY_ID                  char(10),         -- maximum observed so far is 8
    LOCALITY_TYPE                char(2),          -- short codes
    LATITUDE                     double precision, -- Is this the appropriate level of precision?
    LONGITUDE                    double precision, --    ''
    OBSERVATION_DATE             date,             -- Do I need to specify YMD somehow?
    TIME_OBSERVATIONS_STARTED    time,             -- How do I make this a time?
    OBSERVER_ID                  char(12),         -- max of 9 in the data I've seen so far
    SAMPLING_EVENT_IDENTIFIER    char(12),         -- Probably want to index on this.
    PROTOCOL_CODE                varchar(5),
    PROJECT_CODE                 varchar(20),      -- Needs to be at least 10 for sure.
    DURATION_MINUTES             int,              -- bigint?
    EFFORT_DISTANCE_KM           real,             -- precision?
    EFFORT_AREA_HA               real,             -- precision?
    NUMBER_OBSERVERS             int,              -- just a small int
    ALL_SPECIES_REPORTED         int,              -- Seems to always be 1 or 0.  Maybe I could make this Boolean?
    GROUP_IDENTIFIER             varchar(10),      -- Appears to be max of 7 or 8
    HAS_MEDIA                    boolean,
    APPROVED                     boolean,          -- Can be Boolean?
    REVIEWED                     boolean,          -- Can be Boolean?
    REASON                       text,             -- May need to be longer if data set includes unvetted data
    TRIP_COMMENTS                text,             -- Comments are long, unstructured,
    SPECIES_COMMENTS             text
);

EXECUTE( 'COPY "ebird" ' ||
         '(GLOBAL_UNIQUE_IDENTIFIER, CATEGORY, COMMON_NAME, SUBSPECIES_COMMON_NAME, OBSERVATION_COUNT_STR, BREEDING_BIRD_ATLAS_CODE, BREEDING_BIRD_ATLAS_CATEGORY, COUNTRY, COUNTRY_CODE, STATE, STATE_CODE, COUNTY, COUNTY_CODE, ATLAS_BLOCK, LOCALITY, LOCALITY_ID, LOCALITY_TYPE, LATITUDE, LONGITUDE, OBSERVATION_DATE, TIME_OBSERVATIONS_STARTED, OBSERVER_ID, SAMPLING_EVENT_IDENTIFIER, PROTOCOL_CODE, PROJECT_CODE, DURATION_MINUTES, EFFORT_DISTANCE_KM, EFFORT_AREA_HA, NUMBER_OBSERVERS, ALL_SPECIES_REPORTED, GROUP_IDENTIFIER, HAS_MEDIA, APPROVED, REVIEWED, REASON, TRIP_COMMENTS, SPECIES_COMMENTS) ' ||
         'FROM PROGRAM ''cut -f 1,4,5,7,9,10,11,13,14,15,16,17,18,22,23,24,25,26,27,28,29,30,31,33,34,35,36,37,38,39,40,41,42,43,44,45,46 ' || ebird_export_path || '''' ||
         ' WITH (FORMAT CSV, HEADER, QUOTE E''\5'', ENCODING ''UTF8'', DELIMITER E''\t'')');

END $$;

CREATE EXTENSION if not exists postgis;

alter table "ebird" add column observation_count int;
UPDATE "ebird" SET observation_count = observation_count_str::int where observation_count_str != 'X' and observation_count_str is not null;

ALTER TABLE "ebird"
    ADD COLUMN IF NOT EXISTS geog geography;
UPDATE "ebird"
SET geog = st_SetSRID(ST_MakePoint(longitude, LATITUDE), 4326)::geography
where true;

CREATE INDEX ebird_geog_idx ON "ebird" USING GIST (geog);
VACUUM ANALYZE "ebird" (geog);

ALTER TABLE "ebird"
    ADD COLUMN IF NOT EXISTS observation_doy smallint;

UPDATE "ebird"
SET observation_doy = extract(doy from OBSERVATION_DATE)
where true;

create index ebird_state_date_idx ON "ebird" (state_code asc, observation_date asc);
create index ebird_county_date_idx ON "ebird" (county_code asc, observation_date asc);
create index ebird_locality_doy_idx ON "ebird" (locality_id asc, observation_doy asc);
create index ebird_common_name_idx ON "ebird" (common_name asc);
create index ebird_breeding_bird_atlas_category_idx ON "ebird" (breeding_bird_atlas_category) WHERE breeding_bird_atlas_category is not NULL;
CREATE INDEX ON "ebird" (sampling_event_identifier);

VACUUM ANALYZE "ebird";

CREATE or REPLACE FUNCTION get_observer_name(varchar) RETURNS varchar AS
$$
select case $1
    WHEN 'obsr676032' THEN 'Scott Stafford'
    else concat('unknown (', $1, ')')
    end
$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

