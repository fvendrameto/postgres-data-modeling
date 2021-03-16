import glob
import io
import os


def process_data(db_cursor, filepath, func):
    """Process all datasets located under `filepath` using `func`, reporting
       progress for each file.
    """

    # Get all files matching extension from directory
    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(db_cursor, datafile)
        print(f'{i}/{num_files} files processed.')


def copy_df_to_table(df, db_cursor, table_name, columns=None):
    """Copy a pandas DataFrame `df` a certain database table and its columns,
       specified by `table_name` and `columns` arguments.
    """

    # Save dataframe to an IO buffer in memory
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)

    db_cursor.copy_from(output, f'{table_name}', null="", columns=columns)


def execute_query_sequence(db_cursor, all_queries):
    """Executes a sequence of queries to a database."""

    for query in all_queries:
        db_cursor.execute(query)
