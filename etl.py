import os
import glob
import psycopg2
import pandas as pd
import json
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    with open(filepath) as f:
        data = json.loads(f.read())

    df = pd.DataFrame([data])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    cur.execute(song_table_insert, song_data[0])
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    cur.execute(artist_table_insert, artist_data[0])


def process_log_file(cur, filepath):
    # open log file
    with open(filepath) as f:
        data = [json.loads(line) for line in f]
    
    df = pd.DataFrame(data)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    frame = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(frame)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level']]
    udf = user_df[user_df.userId != '']
    user_df = udf.drop_duplicates('userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        songid, artistid = None, None
        try:
            if results:
                songid, artistid = results
        except (ValueError):
            print(f'Corrupted soing_id or artist_id pair has ({len(results)}) length!')


        # insert songplay record
        ts = pd.to_datetime(row.ts, unit='ms')
        songplay_data = (ts, row.userId, row.level, str(songid), str(artistid), row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()