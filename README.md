# Postgres Data Modeling Project

## Summary

This repository implements a relational data modeling process, proposed as the first project of Udacity's Data Engineering Nanodegree. Using PostgreSQL, the objective here is to ingest two datasets from a fictional music app called Sparkify, the first one containing songs and artists data and the second one with user logs from the application.

The data was modeled in a Star Schema, with four dimension tables and a fact table:

- `artists`: Dimension table with data from various artists, obtained through the songs dataset.
- `songs`: Dimension table with data from various songs, also obtained from the songs dataset.
- `users`: Dimension table with app user data, obtained from the logs dataset.
- `time`: Dimension table with timestamps references, also obtained from the logs dataset.
- `songplays`: Fact table, with references to all dimensions and additional data obtained from the logs dataset, such as the user session id, the location of this session and the application user agent.

## Project organization

This project is implemented in Python and has several modules, which are described ahead:

- `create_tables.py`: this is an executable module and is responsible from creation and dropping of all tables. It can be used to clear the database of all data when testing changes to the ETL process. This module was already implemented by Udacity and remains mostly unchanged.
- `etl.py`: also an executable module, is responsible for coordinating the processing and insertion of data, aka ETL. Mostly calls functions from other modules.
- `etl_utils`: implements a few functions that are used throughout the ETL to process data and interact with the database in ways that are common on other modules.
- `log_file_etl.py`: this module hosts functions that perform ETL on logs dataset.
- `song_file_etl.py`: this module implements the ETL process for the songs dataset.
- `sql_queries.py`: finally, this modules hosts all queries that are used to create, insert, select and drop from the database.
- `settings.py`: contains some parameters for database connection.

## Execution

This project is intended to run on Udacity's workspace, which already has a PostgreSQL database set up. By changing the variables on `settings.py` it is likely that this would work on a database hosted elsewhere, but I haven't tested this. You would also need the data to ingest, which is not included in this repository.

There are two executable scripts in this project and they're pretty straigthforward.

```bash
python create_tables.py
python etl.py
```

On a fresh database, the tables won't exist so be sure to run `create_tables.py` before `etl.py` or you will encounter errors. Also, `etl.py` has error treatment, so you may run it multiple times. In this case, be aware that duplicate records will be inserted in the facts table, since the songplay ID is autoincremental.

## Additional information

As suggested by the project rubric, I used bulk insertions using Postgres' `COPY` query. Each dataset is loaded to a staging table, which is basically a replica of the final table without constraints. When I want to load the data into the final tables, I do so by querying the staging table, using `DISTINCT ON (table_id)` to make sure there are no duplicate. Here's is an example query inserting on the `users` table.

```sql
INSERT INTO users
SELECT DISTINCT ON (user_id) *
FROM users_staging
ON CONFLICT DO NOTHING
```
