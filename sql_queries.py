# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id int primary key, start_time timestamp, user_id int , level int, 
song_id int, artist_id int, session_id int, location varchar(20), user_agent varchar(10))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int primary key, first_name varchar(30), last_name varchar(30), 
gender varchar(10), level varchar(10))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar(40), title varchar(70), artist_id varchar, year REAL, duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar(120), location varchar(50), 
latitude numeric, longitude numeric)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, 
year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, 
song_id, artist_id, session_id, location, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level) VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO UPDATE set level = EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs (song_id, artist_id, title, year, duration) VALUES (%s,%s,%s,%s,%s)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s)
""")


time_table_insert = ("""INSERT INTO time (start_time,year,month,day,hour,week,weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)
""")

# FIND SONGS

song_select = ("""SELECT s.song_id, a.artist_id FROM songs s INNER JOIN artists a ON a.artist_id = s.artist_id
WHERE s.title = %(songname)s AND a.name = %(artistname)s 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]