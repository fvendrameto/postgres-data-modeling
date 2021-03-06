import psycopg2

from etl_utils import execute_query_sequence, process_data
from log_file_etl import process_log_file
from settings import SPARKIFY_DB_CONNECT
from song_file_etl import process_song_file
from sql_queries import (TRUNCATE_STAGING_TABLES, INSERT_LOG_DATA_FROM_STAGING,
                         INSERT_SONG_DATA_FROM_STAGING)


def run_etl():
    """Perform ETL on datasets of songs and logs, inserting data on both fact
       and dimension tables.
    """

    db_connection = psycopg2.connect(SPARKIFY_DB_CONNECT)
    db_connection.set_session(autocommit=True)
    db_cursor = db_connection.cursor()

    process_data(db_cursor, filepath='data/song_data', func=process_song_file)
    execute_query_sequence(db_cursor, INSERT_SONG_DATA_FROM_STAGING)

    process_data(db_cursor, filepath='data/log_data', func=process_log_file)
    execute_query_sequence(db_cursor, INSERT_LOG_DATA_FROM_STAGING)

    execute_query_sequence(db_cursor, TRUNCATE_STAGING_TABLES)

    db_connection.close()


if __name__ == "__main__":
    run_etl()
