import requests
import gzip
import sqlite3
import os
from io import TextIOWrapper
import csv
import pandas as pd

# IMDb dataset URLs
DATASETS = {
    'name_basics': 'https://datasets.imdbws.com/name.basics.tsv.gz',
    'title_akas': 'https://datasets.imdbws.com/title.akas.tsv.gz',
    'title_basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
    'title_crew': 'https://datasets.imdbws.com/title.crew.tsv.gz',
    'title_episode': 'https://datasets.imdbws.com/title.episode.tsv.gz',
    'title_principals': 'https://datasets.imdbws.com/title.principals.tsv.gz',
    'title_ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz'
}

DB_FILE = 'imdb.db'
SCHEMA_FILE = 'create_tables.sql'

def download_and_extract(url, filename):
    if os.path.exists(filename):
        return None
    response = requests.get(url, stream=True)
    with open(filename, 'wb') as f:
        f.write(response.content)
    with gzip.open(filename, 'rb') as gz:
        with open(filename[:-3], 'wb') as out:
            out.write(gz.read())
    os.remove(filename)  # Remove the .gz file after extraction
    return filename[:-3]

def create_tables_from_schema(conn, schema_file):
    with open(schema_file, 'r') as f:
        sql_script = f.read()
    conn.executescript(sql_script)
    conn.commit()

def main():
    conn = sqlite3.connect(DB_FILE)

    for table, url in DATASETS.items():
        print(f"Processing {table}...")
        tsv_path = download_and_extract(url, f'{table}.tsv.gz')
        if tsv_path is None:
            continue


    conn.close()
    print("Database setup complete.")

if __name__ == '__main__':
    main()
