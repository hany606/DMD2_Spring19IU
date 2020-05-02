sudo -u postgres psql
	DROP DATABASE dvdrental;
	CREATE DATABASE dvdrental;

sudo -u postgres psql -d dvdrental
	DROP SCHEMA public CASCADE;

sudo -u postgres psql -U postgres -d dvdrental -f "restore.sql" 


