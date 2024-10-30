from typing import Optional

import pandas as pd
import sqlite3
import logging
import time

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

def insert_data_to_db(df, table_name, conn):
    start_time = time.time()
    try:
        logging.info(f"Inserting data into {table_name} with {len(df)} records.")
        df.to_sql(table_name, conn, if_exists='append', index=False)
        elapsed_time = time.time() - start_time
        logging.info(f"Inserted data into {table_name} successfully in {elapsed_time:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error inserting data into {table_name}: {e}", exc_info=True)

def process_genres(df, conn):
    logging.info("Processing genres.")
    df['genres'] = df['genres'].str.split(',')
    df_exploded = df.explode('genres')[['tconst', 'genres']]
    df_exploded = df_exploded.drop_duplicates().rename(columns={'genres': 'genre'})
    insert_data_to_db(df_exploded, 'genres', conn)

def process_primary_professions(df, conn):
    logging.info("Processing primary professions.")
    df['primaryProfession'] = df['primaryProfession'].str.split(',')
    df_exploded = df.explode('primaryProfession')[['nconst', 'primaryProfession']]
    df_exploded = df_exploded.drop_duplicates().rename(columns={'primaryProfession': 'profession'})
    insert_data_to_db(df_exploded, 'primaryProfession', conn)

def process_known_for_titles(df, conn):
    logging.info("Processing known for titles.")
    df['knownForTitles'] = df['knownForTitles'].str.split(',')
    df_exploded = df.explode('knownForTitles')[['nconst', 'knownForTitles']]
    df_exploded = df_exploded.drop_duplicates().rename(columns={'knownForTitles': 'tconst'})
    insert_data_to_db(df_exploded, 'knownForTitles', conn)

def main():
    conn = sqlite3.connect('imdb.db')

    tables = {
        'title_basics': 'title_basics.tsv',
        'name_basics': 'name_basics.tsv',
        'title_akas': 'title_akas.tsv',
        'title_episode': 'title_episode.tsv',
        'title_principals': 'title_principals.tsv',
        'title_ratings': 'title_ratings.tsv',
    }
    title_basics_df = pd.read_csv(tables['title_basics'], sep='\t', dtype=title_basics_dtype)
    conn.close()
    logging.info("Database connection closed.")

if __name__ == '__main__':
    main()
