from pandas import DataFrame, read_json, to_datetime

from etl_utils import copy_df_to_table
from sql_queries import SONG_SELECT

USER_DATA_COLS = [
    'userId',
    'firstName',
    'lastName',
    'gender',
    'level',
]

SONGPLAY_COPY_COLS = [
    'start_time',
    'user_id',
    'song_id',
    'artist_id',
    'session_id',
    'location',
    'user_agent',
]


def process_log_file(db_cursor, filepath):
    """Process data from log datasets, inserting time, users and songplays data
       to a database
    """

    log_data_df = read_json(filepath, lines=True)

    # Filter log data by NextSong action
    log_data_df = log_data_df[log_data_df['page'] == 'NextSong']

    time_df = get_time_df(log_data_df)
    copy_df_to_table(time_df, db_cursor, 'time_staging')

    user_df = log_data_df[USER_DATA_COLS]
    copy_df_to_table(user_df, db_cursor, 'users_staging')

    songplays_df = get_songplays_df(log_data_df, db_cursor)
    copy_df_to_table(songplays_df,
                     db_cursor,
                     'songplays',
                     columns=SONGPLAY_COPY_COLS)


def get_time_df(log_data_df):
    """Process the `log_data_df` timestamp columns to obtain a valid entry for
       the time database table, converting this columns to a pandas Datetime
       type. Return a DataFrame with data from all timestamps in `log_data_df`.
    """

    # Convert timestamp column to datetime
    timestamp_df = to_datetime(log_data_df['ts'], unit='ms')

    # Get time data from datetime
    time_data = {
        'timestamp': log_data_df['ts'].tolist(),
        'hour': timestamp_df.dt.hour.tolist(),
        'day': timestamp_df.dt.day.tolist(),
        'week': timestamp_df.dt.week.tolist(),
        'month': timestamp_df.dt.month.tolist(),
        'year': timestamp_df.dt.year.tolist(),
        'weekday': timestamp_df.dt.weekday.tolist(),
    }

    return DataFrame(time_data)


def get_songplays_df(log_data_df, db_cursor):
    """Process `log_data_df` to obtain songplay data. Return a pandas DataFrame
       with all valid songplays found.
    """

    # Get data from each row using pandas apply method
    all_songplays_data = log_data_df.apply(
        lambda row: get_songplay_data(row, db_cursor),
        axis=1,
    )
    # Filter null data and convert from pd.Series to list
    all_songplays_data = all_songplays_data.dropna().tolist()

    return DataFrame(all_songplays_data, columns=SONGPLAY_COPY_COLS)


def get_songplay_data(row, db_cursor):
    """Get songplay data from a `row` of `log_data_df`, querying a database to
       find valid song and artist IDs using data in this row. Return a dict
       containing the songplay data if those IDs are found, else it will return
       None.
    """

    # Get song ID and artist ID from song and artist tables
    db_cursor.execute(SONG_SELECT, (row.song, row.artist, row.length))
    results = db_cursor.fetchone()

    # If song_id and artist_id are found, make the songplay data dict
    if results:
        song_id, artist_id = results

        songplay_data = {
            'start_time': row.ts,
            'user_id': row.userId,
            'song_id': song_id,
            'artist_id': artist_id,
            'session_id': row.sessionId,
            'location': row.location,
            'user_agent': row.userAgent,
        }

        return songplay_data
    return None
