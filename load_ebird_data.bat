@echo off
@REM for initial db creation
set POSTGRES_HOST=localhost
set PGPASSWORD=postgres
@REM psql -h %POSTGRES_HOST% -U postgres -d template1 -f ebird-create-db-and-load-ebd.sql
@REM psql -h %POSTGRES_HOST% -U postgres -d ebirddb -f ebird-create-observer-name-func.sql
@REM for adding other datasets to existing db
psql -h %POSTGRES_HOST% -U postgres -d template1 -f ebird-load-ebd.sql

echo Completed load.  Opening psql for you.  You might want to do this:
echo SET CLIENT_ENCODING TO 'UTF-8';
echo psql -U postgres -d ebirddb
