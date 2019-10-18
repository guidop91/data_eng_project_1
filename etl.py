import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Extract song data from file and add to song and artist tables.

    Parameters
    ----------
    cur : cursor connection to db\n
    filepath : path of current file to be analized
    Returns
    -------
    None
    """
    # open song file
    song_df = pd.read_json(filepath, lines=True).values[0]

    # insert song record
    song_data = list([
        song_df[7],  # song_id
        song_df[8],  # title
        song_df[0],  # artist_id
        song_df[9],  # year
        song_df[5]   # duration
    ])

    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Could not execute song insertion query.")
        print(e)

    # insert artist record
    artist_data = list([
        song_df[0],  # artist_id
        song_df[4],  # artist_name
        song_df[2],  # artist_location
        song_df[1],  # artist_latitude
        song_df[3]   # artist_longitude
    ])
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Could not execute artist insertion query.")
        print(e)


def process_log_file(cur, filepath):
    """Extract log data from file and add to time, user and songplay tables.

    Parameters
    ----------
    cur : cursor connection to db\n
    filepath : path of current file to be analized
    Returns
    -------
    None
    """
    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_df = log_df[log_df['page'] == 'NextSong']

    # convert timestamp column to datetime
    time_series = pd.to_datetime(log_df['ts'], unit='ms')

    # insert time data records
    time_dictionary = {
        'start_time': log_df['ts'],
        'hour': time_series.dt.hour,
        'day': time_series.dt.day,
        'week': time_series.dt.weekofyear,
        'month': time_series.dt.month,
        'year': time_series.dt.year,
        'weekday': time_series.dt.weekday_name,
    }
    time_df = pd.DataFrame(time_dictionary)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Could not execute time insertion query")
            print(e)

    # load user table
    user_dictionary = {
        'userId': log_df['userId'],
        'first_name': log_df['firstName'],
        'last_name': log_df['lastName'],
        'gender': log_df['gender'],
        'level': log_df['level']
    }
    user_df = pd.DataFrame(user_dictionary)

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Could not execute user insertion query")
            print(e)

    # insert songplay records
    for index, row in log_df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print("Could not execute song selection query")
            print(e)
        
        results = (None, None)
        try:
            results = cur.fetchone()
        except psycopg2.Error as e:
            print("Could not fetch from cursor")
            print(e)

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = list([
            row.ts,  # timestamp
            row.userId,
            row.level,
            songid,  # song-id from song table
            artistid,  # artist-id from song table
            row.sessionId,
            row.location,
            row.userAgent
        ])
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Could not execute songplay insertion query")
            print(e)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    try:
        conn = psycopg2.connect(
            "host=127.0.0.1 dbname=sparkifydb user=student password=student"
        )
    except psycopg2.Error as e:
        print("Error: Could not establish connection to Postgres DB")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: could not get cursor to the DB")
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
