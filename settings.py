HOST = '127.0.0.1'
USER = 'student'
PASSWORD = 'student'

DEFAULT_DBNAME = 'studentdb'
SPARKIFY_DBNAME = 'sparkifydb'

DEFAULT_DB_CONNECT = (f'host={HOST} dbname={DEFAULT_DBNAME} user={USER} '
                      f'password={PASSWORD}')
SPARKIFY_DB_CONNECT = (f'host={HOST} dbname={SPARKIFY_DBNAME} user={USER} '
                       f'password={PASSWORD}')
