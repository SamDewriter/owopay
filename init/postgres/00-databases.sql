-- Runs once, when the pg_data volume is first created.
-- owopay (the OLTP source) is created by the image from POSTGRES_DB.
create database dw;
create database airflow;
create database metabase;
