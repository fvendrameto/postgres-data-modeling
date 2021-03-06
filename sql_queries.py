# Songplays table queries
SONGPLAY_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id bigserial,
        start_time bigint NOT NULL,
        user_id varchar NOT NULL,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session_id bigint,
        location varchar,
        user_agent varchar,
        PRIMARY KEY (songplay_id)
    )
    """)

SONGPLAY_TABLE_DROP = "DROP TABLE IF EXISTS songplays"

# Users table queries
USER_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id varchar,
        first_name varchar,
        last_name varchar,
        gender char(1),
        level varchar,
        PRIMARY KEY (user_id)
    );
    """)

USER_TABLE_INSERT_FROM_STAGING = ("""
    INSERT INTO users
    SELECT DISTINCT ON (user_id) *
    FROM users_staging
    ON CONFLICT DO NOTHING
    """)

USER_TABLE_DROP = "DROP TABLE IF EXISTS users"

USER_STAGING_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS users_staging (
        user_id varchar,
        first_name varchar,
        last_name varchar,
        gender char(1),
        level varchar
    );
    """)

USER_STAGING_TABLE_TRUNCATE = "TRUNCATE TABLE users_staging"

USER_STAGING_TABLE_DROP = "DROP TABLE IF EXISTS users_staging"

# Songs table queries
SONG_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar,
        title varchar,
        artist_id varchar,
        year int,
        duration decimal,
        PRIMARY KEY (song_id)
    );
    """)

SONG_TABLE_INSERT_FROM_STAGING = ("""
    INSERT INTO songs
    SELECT DISTINCT ON (song_id) *
    FROM songs_staging
    ON CONFLICT DO NOTHING
    """)

SONG_TABLE_DROP = "DROP TABLE IF EXISTS songs"

SONG_STAGING_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS songs_staging (
        song_id varchar,
        title varchar,
        artist_id varchar,
        year int,
        duration decimal
    );
    """)

SONG_STAGING_TABLE_TRUNCATE = "TRUNCATE TABLE songs_staging"

SONG_STAGING_TABLE_DROP = "DROP TABLE IF EXISTS songs_staging"

# Artists table queries
ARTIST_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal,
        PRIMARY KEY (artist_id)
    );
    """)

ARTIST_TABLE_INSERT_FROM_STAGING = ("""
    INSERT INTO artists
    SELECT DISTINCT ON (artist_id) *
    FROM artists_staging
    ON CONFLICT DO NOTHING
    """)

ARTIST_TABLE_DROP = "DROP TABLE IF EXISTS artists"

ARTIST_STAGING_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS artists_staging (
        artist_id varchar,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal
    );
    """)

ARTIST_STAGING_TABLE_TRUNCATE = "TRUNCATE TABLE artists_staging"

ARTIST_STAGING_TABLE_DROP = "DROP TABLE IF EXISTS artists_staging"

# Time table queries
TIME_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar,
        PRIMARY KEY (start_time)
    );
    """)

TIME_TABLE_INSERT_FROM_STAGING = ("""
    INSERT INTO time
    SELECT DISTINCT ON (start_time) *
    FROM time_staging
    ON CONFLICT DO NOTHING
    """)

TIME_TABLE_DROP = "DROP TABLE IF EXISTS time"

TIME_STAGING_TABLE_CREATE = ("""
    CREATE TABLE IF NOT EXISTS time_staging (
        start_time bigint,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
    );
    """)

TIME_STAGING_TABLE_TRUNCATE = "TRUNCATE TABLE time_staging"

TIME_STAGING_TABLE_DROP = "DROP TABLE IF EXISTS time_staging"

# Find songs and artists query
SONG_SELECT = ("""
    SELECT S.song_id, A.artist_id
    FROM songs S JOIN artists A
    ON S.artist_id = A.artist_id
    WHERE S.title = %s AND A.name = %s AND S.duration = %s
    """)

# Query lists
CREATE_TABLE_QUERIES = [
    SONGPLAY_TABLE_CREATE,
    USER_TABLE_CREATE,
    SONG_TABLE_CREATE,
    ARTIST_TABLE_CREATE,
    TIME_TABLE_CREATE,
    USER_STAGING_TABLE_CREATE,
    SONG_STAGING_TABLE_CREATE,
    ARTIST_STAGING_TABLE_CREATE,
    TIME_STAGING_TABLE_CREATE,
]

INSERT_SONG_DATA_FROM_STAGING = [
    SONG_TABLE_INSERT_FROM_STAGING,
    ARTIST_TABLE_INSERT_FROM_STAGING,
]

INSERT_LOG_DATA_FROM_STAGING = [
    USER_TABLE_INSERT_FROM_STAGING,
    TIME_TABLE_INSERT_FROM_STAGING,
]

TRUNCATE_STAGING_TABLES = [
    SONG_STAGING_TABLE_TRUNCATE,
    USER_STAGING_TABLE_TRUNCATE,
    ARTIST_STAGING_TABLE_TRUNCATE,
    TIME_STAGING_TABLE_TRUNCATE,
]

DROP_TABLE_QUERIES = [
    SONGPLAY_TABLE_DROP,
    USER_TABLE_DROP,
    SONG_TABLE_DROP,
    ARTIST_TABLE_DROP,
    TIME_TABLE_DROP,
    USER_STAGING_TABLE_DROP,
    SONG_STAGING_TABLE_DROP,
    ARTIST_STAGING_TABLE_DROP,
    TIME_STAGING_TABLE_DROP,
]
