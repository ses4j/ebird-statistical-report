/*
This script will add new EBD data to an existing database, as specified by `ebird_export_path`.
*/

SET default_tablespace = ebird_db_space;

\set ON_ERROR_STOP on
\c "ebirddb"

DO $$
-- DECLARE ebird_export_path CONSTANT VARCHAR := 'C:\ebird-tablespace\ebd_US-DC_prv_relDec-2020\ebd_US-DC_prv_relDec-2020.txt';
-- DECLARE ebird_export_path CONSTANT VARCHAR := 'C:\ebird-tablespace\ebd_US-MD_prv_relDec-2020\ebd_US-MD_prv_relDec-2020.txt';
-- DECLARE ebird_export_path CONSTANT VARCHAR := '/mnt/import-data/ebd_US-FL-069_relDec-2020/ebd_US-FL-069_relDec-2020.txt';
DECLARE ebird_export_path CONSTANT VARCHAR := 'C:\ebird-tablespace\ebd_US-DC_prv_relDec-2021\ebd_US-DC_prv_relDec-2021.txt';
BEGIN
raise notice 'Importing EBD file: %', ebird_export_path;

EXECUTE( 'COPY "ebird" ' ||
         '(GLOBAL_UNIQUE_IDENTIFIER, LAST_EDITED_DATE, TAXONOMIC_ORDER, CATEGORY, COMMON_NAME, SCIENTIFIC_NAME, SUBSPECIES_COMMON_NAME, OBSERVATION_COUNT_STR, BREEDING_CODE, BREEDING_CATEGORY, BEHAVIOR_CODE, COUNTRY, COUNTRY_CODE, STATE, STATE_CODE, COUNTY, COUNTY_CODE, ATLAS_BLOCK, LOCALITY, LOCALITY_ID, LOCALITY_TYPE, LATITUDE, LONGITUDE, OBSERVATION_DATE, TIME_OBSERVATIONS_STARTED, OBSERVER_ID, SAMPLING_EVENT_IDENTIFIER, PROTOCOL_CODE, PROJECT_CODE, DURATION_MINUTES, EFFORT_DISTANCE_KM, EFFORT_AREA_HA, NUMBER_OBSERVERS, ALL_SPECIES_REPORTED, GROUP_IDENTIFIER, HAS_MEDIA, APPROVED, REVIEWED, REASON, TRIP_COMMENTS, SPECIES_COMMENTS) ' ||
         'FROM PROGRAM ''cut -f 1,2,3,4,5,6,7,9,10,11,12,14,15,16,17,18,19,23,24,25,26,27,28,29,30,31,32,34,35,36,37,38,39,40,41,42,43,44,45,46,47 ' || ebird_export_path || '''' ||
         ' WITH (FORMAT CSV, HEADER, QUOTE E''\5'', ENCODING ''UTF8'', DELIMITER E''\t'')');

raise notice 'Complete import of EBD file: %', ebird_export_path;
END $$;

UPDATE "ebird" SET observation_count = observation_count_str::int where observation_count_str != 'X' and observation_count_str is not null and observation_count is null;
UPDATE "ebird"
SET geog = st_SetSRID(ST_MakePoint(longitude, LATITUDE), 4326)::geography
where geog is null;
VACUUM ANALYZE "ebird" (geog);

UPDATE "ebird"
SET observation_doy = extract(doy from OBSERVATION_DATE)
where observation_doy = 0;
VACUUM ANALYZE "ebird";

