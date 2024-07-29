DROP USER IF EXISTS serviceaccount;

CREATE USER serviceaccount WITH PASSWORD 'your_serviceaccount_password';

DROP DATABASE IF EXISTS sales;

CREATE DATABASE sales;

GRANT ALL PRIVILEGES ON DATABASE sales TO serviceaccount;

\c sales;

GRANT ALL PRIVILEGES ON SCHEMA public TO serviceaccount;