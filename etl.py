import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ 
    - Opens a file <filepath>, reads data from it to pandas DataFrame.
    - Takes data from DataFrame with columns "song_id", "title", "artist_id", "year", "duration" and insert them 
    into "songs" table.
    - Takes data from DataFrame with columns "artist_id", "artist_name", "artist_location", "artist_latitude", 
    "artist_longitude" and insert them into "artists" table.
    """

    df=pd.read_json(filepath, lines=True, orient='records')
   
    for i, row in df[["song_id", "title", "artist_id", "year", "duration"]].iterrows():
        try:
          cur.execute(song_table_insert, list(row))
        except psycopg2.Error as e:
                print("Error: Inserting Rows songs")
                print(e)
                print(list(row))        
        
    for i, row in df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].iterrows():
        try:
          cur.execute(artist_table_insert, list(row))
        except psycopg2.Error as e:
                print("Error: Inserting Rows artists")
                print(e)
                print(list(row))  


def process_log_file(cur, filepath):
    
    """
    - Opens a file <filepath>, reads data from it to pandas Data Frame.
    - Filters records by page = "NextSong" action.
    
    - Extracts the timestamp, hour, day, week of year, month, year, and weekday from the "ts" column, and puts 
    the data to "time" table.
    - Takes data from Data Frame for columns "userId", "firstName", "lastName", "gender", "level", and insert 
    them into "users" table.
    
    - For each row in DataFrame for columns "song", "length", and "artist", finds correspondigs song_id and 
    artist_id in songs and artists tables. If no song_id and artist_id were found, uses None for them. 
    
    - Select the "timestamp", "user ID", "level", found songId or None, found artistid or None, "session ID", 
    "location", and "user agent" and inserts records into "songplays" Table
    """
    
    df=pd.read_json(filepath, lines=True, orient='records')

    df = df[df["page"]=="NextSong"]

    df["ts_date"] = pd.to_datetime(df["ts"], unit='ms')

    time_data = (df["ts"], df["ts_date"].dt.hour, df["ts_date"].dt.day, df["ts_date"].dt.weekofyear, df["ts_date"].dt.month, df["ts_date"].dt.year, df["ts_date"].dt.weekday )
    column_labels = ("timestamp", "hour", "day", "week_of_year", "month", "year", "weekday")
    
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        try:
          cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
                print("Error: Inserting Rows time")
                print(e)
                print(list(row))   

    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    
    # getting unique users from log data 
    user_df = user_df.drop_duplicates()
    user_df["userId"] = user_df["userId"].astype(int)

    for i, row in user_df.iterrows():
        try:
          cur.execute(user_table_insert, list(row))
        except psycopg2.Error as e:
                print("Error: Inserting Rows users")
                print(e)
                print(list(row))  

    for index, row in df.iterrows():

        cur.execute(song_select, (row.song, row.length, row.artist))

        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (row.ts, int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        try:
          cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
                print("Error: Inserting Rows songplays")
                print(e)
                print(songplay_data) 

        
def process_data(cur, conn, filepath, func):
    """
    - Calls function "func" that processes data from all json files in "filepath" folder.
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ 
    - Creates a connection to "sparkifydb" database.
    - Calls corresponding functions for processing data from "data/song_data" and "data/log_data" folders.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()