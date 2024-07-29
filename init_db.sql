DROP USER IF EXISTS <username>;

CREATE USER <username> WITH PASSWORD '<password>';

DROP DATABASE IF EXISTS <database>;

CREATE DATABASE <database>;

GRANT ALL PRIVILEGES ON DATABASE <database> TO <username>;

\c <database>;

GRANT ALL PRIVILEGES ON SCHEMA public TO <username>;