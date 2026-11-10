# Connect to PostgreSQL Database container

docker exec \
    -it cluster_util_db \
    psql -U postgres \
    -W
    
# Password: itversity

# Perform following activities on PostgreSQL Prompt

# List databases

# Inside your interactive psql session in the container, execute:

\l

# Create a database training and then a table test

# Inside your interactive psql session in the container, execute:

DROP DATABASE IF EXISTS training;
\l
CREATE DATABASE training;

# Change to database
\c training;

# Password = itversity

# List tables
\dt

DROP TABLE IF EXISTS test;

CREATE TABLE test (
    batchId INTEGER,
    name VARCHAR(255),
    "Date" VARCHAR(255),
    open VARCHAR(255)
);

# Describe newly created table
\d test;

# Quit psql prompt
\q
