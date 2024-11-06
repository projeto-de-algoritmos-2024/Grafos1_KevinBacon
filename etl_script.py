import locale
import logging
import os.path
import shutil
import sqlite3
import tempfile
import time
from dataclasses import dataclass
import gzip
from typing import Callable, Optional

import polars as pl
import requests

db_name = 'imdb.db'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])
locale.setlocale(locale.LC_ALL, '')
lines_per_chunk = 150_000
chunks_per_batch = 15

def split_attribute(df: pl.DataFrame, col_name: str, new_col_name: str) -> pl.DataFrame:
    return df.select(
        pl.col("tconst"),
        pl.col(col_name).str.split(',').alias(new_col_name)
    ).explode(new_col_name).drop_nulls()

def transform_title_basics(df: pl.DataFrame):
    df = df.with_columns(pl.when(pl.col("isAdult") == 1).then(True).otherwise(False).alias("isAdult"))
    genres_df = split_attribute(df, "genres", "genre")
    df = df.drop("genres")
    return {'title_basics': df, 'genres': genres_df}

def transform_title_akas(df: pl.DataFrame):
    types_df = split_attribute(df, "types", "type")
    attributes_df = split_attribute(df, "attributes", "attribute")
    df = df.drop("attributes").drop("types")
    return {'title_akas': df, 'types': types_df, 'attributes': attributes_df}

def transform_title_crew(df: pl.DataFrame):
    directors_df = split_attribute(df, "directors", "director")
    writers_df = split_attribute(df, "writers", "writer")
    return {'crew_directors': directors_df, 'crew_writers': writers_df}

def transform_name_basics(df: pl.DataFrame):
    professions = split_attribute(df, "primaryProfession", "profession")
    known_for_titles = split_attribute(df, "knownForTitles", "title")
    df = df.drop("primaryProfession").drop("knownForTitles")
    return {'name_basics': df, 'knownForTitles': known_for_titles, 'primaryProfession': professions}

@dataclass(frozen=True)
class Dataset:
    table_name: str
    url: str
    transform_fn: Optional[Callable] = None

datasets = [
    Dataset('name_basics', 'https://datasets.imdbws.com/name.basics.tsv.gz', transform_name_basics),
    Dataset('title_akas', 'https://datasets.imdbws.com/title.akas.tsv.gz', transform_title_akas),
    Dataset('title_basics', 'https://datasets.imdbws.com/title.basics.tsv.gz', transform_title_basics),
    Dataset('title_crew', 'https://datasets.imdbws.com/title.crew.tsv.gz', transform_title_crew),
    Dataset('title_episode', 'https://datasets.imdbws.com/title.episode.tsv.gz'),
    Dataset('title_principals', 'https://datasets.imdbws.com/title.principals.tsv.gz'),
    Dataset('title_ratings', 'https://datasets.imdbws.com/title.ratings.tsv.gz')
]

def insert_table(df: pl.DataFrame, table_name:str):
    df.write_database(table_name, f'sqlite:///{db_name}', if_table_exists='append')

def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

def process_and_insert(path, transform_fn=None):
    # Remove header line
    total_size = count_lines(path) - 1
    logging.info(f"Starting batch processing for file: {path}")

    reader = pl.read_csv_batched(path, separator='\t', null_values=['\\N'], quote_char=None, batch_size=lines_per_chunk)
    processed_lines = 0
    time_per_batch = []
    start_time = time.time()

    batch = reader.next_batches(chunks_per_batch)

    while batch:
        df_batch = pl.concat(batch)
        num_lines_in_batch = len(df_batch)
        processed_lines += num_lines_in_batch

        batch_start_time = time.time()

        logging.info(f"Processing batch for table: {path}")

        if transform_fn is None:
            table_name = os.path.basename(path).split('.')[0]
            insert_table(df_batch, table_name)
        else:
            transformed_data = transform_fn(df_batch)
            for sub_table_name, df in transformed_data.items():
                insert_table(df, sub_table_name)

        batch_time = time.time() - batch_start_time
        time_per_batch.append(batch_time)

        avg_time_per_line = sum(time_per_batch) / processed_lines if processed_lines > 0 else 0
        remaining_lines = total_size - processed_lines
        estimated_time_remaining = remaining_lines * avg_time_per_line
        eta_formatted = time.strftime('%H:%M:%S', time.gmtime(estimated_time_remaining))

        logging.info(f"Processed {processed_lines:n}/{total_size:n} lines\tprogress: {(processed_lines / total_size) * 100:.2f}%\tETA: {eta_formatted}")

        batch = reader.next_batches(chunks_per_batch)

    logging.info(f"Completed processing for file: {path} in {time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time))}")


def download_and_decompress(dataset: Dataset, dir: str):
    logging.info(f"Downloading {dataset.url} to {dir}")
    filename = dataset.url.split('/')[-1]
    archive_path = os.path.join(dir, filename)
    with requests.get(dataset.url, stream=True) as r:
        with open(archive_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    logging.info(f"Decompressing {archive_path}")
    file_path = os.path.join(dir, dataset.table_name+'.tsv')
    with gzip.open(archive_path, 'rb') as f_in:
        with open(file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logging.info(f"Decompressed {archive_path}")
    return file_path


def create_schema(conn: sqlite3.Connection):
    logging.info(f"Creating schema {db_name}")
    with open('create_schema.sql', 'r') as schema:
        conn.executescript(schema.read())
    logging.info(f"Created schema {db_name}")

with tempfile.TemporaryDirectory(dir='./') as tmpdirname:
    for count, dataset in enumerate(datasets):
        logging.info(f'Starting processing for dataset {count}/{len(datasets)}')
        path = download_and_decompress(dataset, tmpdirname)
        process_and_insert(path, dataset.transform_fn)
        logging.info(f'Completed processing for dataset {count}/{len(datasets)}')