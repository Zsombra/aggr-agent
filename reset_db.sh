#!/bin/bash
export PAGER=cat
export PGHOST=127.0.0.1
export PGPORT=5433
export PGUSER=postgres

echo "Dropping/Creating user and database..."
sudo -u postgres psql -p 5433 -P pager=off -c "DROP DATABASE IF EXISTS aggr_data;"
sudo -u postgres psql -p 5433 -P pager=off -c "DROP USER IF EXISTS aggr_user;"
sudo -u postgres psql -p 5433 -P pager=off -c "CREATE USER aggr_user WITH PASSWORD 'AggrData2026!';"
sudo -u postgres psql -p 5433 -P pager=off -c "CREATE DATABASE aggr_data OWNER aggr_user;"
sudo -u postgres psql -p 5433 -P pager=off -d aggr_data -f init_db.sql
