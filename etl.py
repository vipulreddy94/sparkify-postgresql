import os
import glob
import psycopg2
import json
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Accepts the cursor object and each file from the song_file folder, processes it.
    - Inserts into song and artist table are executed from the processed data.
    - cursor is comitted after the function call.
    """
    # open song file
    df = pd.DataFrame(json.load(open(filepath)), index = [0])

    # insert song record
    song_data = df.loc[:,['song_id','artist_id','title','year','duration']]
    cur.execute(song_table_insert, song_data.values[0])
    
    # insert artist record
    artist_data = df.loc[:,['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    cur.execute(artist_table_insert, artist_data.values[0])


def process_log_file(cur, filepath):
    """
    - Accepts the cursor object and each file from the log_file folder, processes it.
    - Inserts into user, time and songplays table are executed from the processed data.
    - cursor is comitted after the function call.
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df['page']=='NextSong',:]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    
    day = list(df['ts'].dt.day)
    year = list(df['ts'].dt.year)
    month = list(df['ts'].dt.month)
    week = list(df['ts'].dt.week)
    weekday = list(df['ts'].dt.weekday)
    hour = list(df['ts'].dt.hour)
    ts = list(df['ts'])
    
    date_list = []

    for i in range(0,len(ts)):
        templist = [ts[i],year[i],month[i],day[i],hour[i],week[i],weekday[i]]
        date_list.append(templist)

    # insert time data records
    time_data = (date_list)
    column_labels = ('timestamp','year','month','day','hour','week','weekday')
    time_df = pd.DataFrame(data=(time_data),columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:,['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates()
   
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, {'songname':row.song, 'artistname':row.artist, 'duration':row.length})
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,row.userId,row.level,songid, artistid,row.sessionId,row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Accepts cursor and connection objects,file path and the function to be used to process the files in that path. 
    - all the files in the file path are iterated and stored in a list. 
    - each file is passed onto the func along with cursor obejct. 
    - Processing of data and inserts are made in the passed func, but commits are done here. 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()