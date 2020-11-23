# Sparkify song play aggregation

Sparkify currently stores song play instance data in log form in a series of json files in S3.
This instrumentation loads the song play records as well as the song records themselves into a 
postgres star schema database

## Postgres

Both the default database name and postgres root user name are "postgres" instead of "student",
when attempting to compose testing utilities for the etl scripting this will need to be considered.

Connect locally thereto with ```psql postgresql://postgres:postgres@127.0.0.1/sparkifydb```

#### Schema

songplays: This table is the fact table that contains which users listened to what songs at what time
  - songplay_id: The serial ID of the song play instance
  - start_time: This is the milliseconds timestamp when the songplay instance occurred
  - user_id: ID of the user who listened to the song. This might be null.
  - level: Whether or not the user's subscription is "paid" or "free"
  - song_id: ID of the song that was listened to
  - artist_id: ID of the artist who composed the song that was listened to
  - session_id: Appears to be the web session id
  - location: Readable location, city, state
  - user_agent: Device by which the song was listened to, e.g. Firefox on macos

users: The set of known users
  - user_id: ID of the user
  - firstName: User first name
  - lastName: User last name
  - gender: User gender "M"\"F"
  - level: Whether or not the user's subscription is "paid" or "free"

songs: Set of known songs
  - song_id: ID of the song
  - title: Song title
  - artist_id: ID of the artist who composed the song
  - year: Year the song was composed
  - duration: Duration of the song in seconds

artists: Set of known song artists
  - artist_id: ID of the artist
  - name: First and last name of artist
  - location: Readable location, city, state of the artist
  - latitude: Artist's latitude
  - longitude: Artist's longitude

time: Breakdowns of song play instance timestamps
  - start_time: The raw milliseconds play start
  - hour: The hour of the songplay (0 - 23)
  - day: Day of the month of the songplay
  - week: Week of the year of the songplay
  - month: Month of the year of the songplay
  - year: Year of the songplay
  - weekday: Day of the week, e.g. "Monday"

## Creating the tables and inserting the data

Reset tables with ```python3 create_tables.py```
Insert data from S3 with ```python3 etl.py```
