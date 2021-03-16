from pandas import read_json

from etl_utils import copy_df_to_table

SONG_DATA_COLS = [
    'song_id',
    'title',
    'artist_id',
    'year',
    'duration',
]

ARTIST_DATA_COLS = [
    'artist_id',
    'artist_name',
    'artist_location',
    'artist_latitude',
    'artist_longitude',
]


def process_song_file(db_cursor, filepath):
    """Process data from song datasets, inserting song and artist data to a
       database.
    """

    song_data_df = read_json(filepath, lines=True)

    song_df = song_data_df[SONG_DATA_COLS]
    copy_df_to_table(song_df, db_cursor, 'songs_staging')

    artist_df = song_data_df[ARTIST_DATA_COLS]
    copy_df_to_table(artist_df, db_cursor, 'artists_staging')
