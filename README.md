# Data Engineering Nanodegree, Data Modeling with Postgres

This codebase creates a database containing 5 tables in star schema,
that organizes data related to a music library, and user listening data. This data
has been extracted from two type of `json` files: 

- One `json` type of file that contains organized music data, with columns for:
  - Artist ID
  - Artist Name
  - Song Title
  - Song Duration
  - Song year of release
  - etc.
- One `json` type of file that contains user listening data, with columns for: 
  - User's first and last names
  - User's general location
  - Song title
  - Artist name
  - Other metadata

These files are parsed and organized into this 5 different tables: 

- **Songplay Table**: this represents the only fact table in the star schema. It contains 
  data related to how users listen to music, including the time at which they listen to it, 
  their location, what song and artist (related with their IDs) the event relates to, and other
  pieces of information that can be used to analyze user listening activity. 

- **Users Table**: a dimension table, that holds user's data, including their first and last name,
  their gender and whether or not they're subscribed. 

- **Songs Table**: a dimension table, that holds songs details, including the title, it's 
  contributing artist, the duration, the year of its release, etc. 

- **Artist Table**: a dimension table, that holds aritst details, including their name and 
  their location. 

- **Time Table**: a dimension table, that holds many different ways of interpreting a timestamp, 
  like a weekday, hour, month, day of month, etc. 

## Brief explanation of each file in this repository

- `sql_queries.py`: contains the queries for `DROP`-ping, `CREATE`-ing, and `INSERT`-ing data
  into tables. 

- `create_tables.py`: connects safely to database and runs the table creation queries, one by one,
  dropping a table with the same name if it exists first. This way we make sure that we insert
  into a fresh new table. 

- `etl.py`: performs the bulk of the work, analyzing the files cited above, parsing the data, and 
  inserting it into their respective tables, while giving a progress in the console. 

## Running this project

1. Run the `create_table.py` first and foremost. This will delete existing tables with
   the names that we are using and create new, empty ones for us to insert the data parsed from
   the files. 
2. Run the `etl.py` and wait for the script to finish. 
3. Run your queries to get the data. 

Example: 
```
SELECT * FROM songplays LIMIT 10;
```