import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description:
        - Insert into our song and artist dimension tables
        - insert data from our directory to the song and artist tables
    Args:
        - cur: runs the sql queries
        - filepath: path to the directory containing our target data
    Returns:
        None
    """
    df =   df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description:
        - Insert into our time, user dimension tables and songplays fact table
        - to get all the data we filter the page column by "NextSong"
        - we convert our timestamp into a datetime datatype then insert in our time table
        - we insert into our user table
        - we set a condition to accept null("None") values in the songId and artistId columns of the songplays table
        - we insert into our songplays table
    Args:
        - cur: runs the sql queries
        - filepath: path to the directory containing our target data
    Returns:
        None
    """
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = df["ts"]
    
    # insert time data records
    time_data = ([(x, x.hour, x.day, x.week, x.month, x.year, x.dayofweek) for x in [pd.to_datetime(row, unit="ms") for row in t]])
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df =df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =  (row.ts, row.userId, row.level, songid, artistid, row.itemInSession, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description:
        - listing the files in a directory,
        - Then execute ingest process for each file in each sql fact and dimensions tables
        - Then saves it to our database

    Arguments:
        - cur: the cursor runs our sql queries.
        - conn: connects to our database and commits our cur queries
        - filepath: the path to the directory for our target data: log_data and/or song_data.
        - func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
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
    Description:
        - connects to our database
        - performs the ingestion queries from our song_data and log data file paths to sql tables
        - saves and closes the database connection
    Returns:
        - None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()