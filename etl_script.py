import logging
import os.path
import sqlite3
import subprocess
import time
from functools import wraps
from typing import Callable

import polars as pl

db_name = 'imdb.db'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

batch_size = 150_000
max_batches = 15

def count_lines(file_path):
    result = subprocess.run(['wc', '-l', file_path], stdout=subprocess.PIPE, text=True)
    line_count = int(result.stdout.split()[0])
    return line_count

def insert_table(df: pl.DataFrame, table_name:str):
    df.write_database(table_name, f'sqlite:///{db_name}', if_table_exists='append')

def batch_insert(path: str):
    def decorator(transform_fn: Callable):
        @wraps(transform_fn)
        def wrapper(*args, **kwargs):
            #remove header line
            total_size = count_lines(path) - 1
            logging.info(f"Starting batch processing for file: {path}")

            reader = pl.read_csv_batched(path, separator='\t' ,null_values=['\\N'], quote_char=None, batch_size=batch_size)
            lines_read = 0
            batch_count = 1
            batch = reader.next_batches(max_batches)

            while batch:
                df_batch = pl.concat(batch)
                lines_read += len(df_batch)
                progress = (lines_read / total_size) * 100
                logging.info(f"Processing batch no. {batch_count} for table: {path}")

                transformed_data = transform_fn(df_batch, *args, **kwargs)

                if progress != 100:
                    logging.info(f"Transformed batch no. {batch_count} for table: {path}, lines_processed: {progress:.2f}%, now inserting {len(transformed_data)} table(s)")
                else:
                    logging.info(f"Transformed final batch for table {path}, all lines processed, now inserting {len(transformed_data)} table(s)")

                if transformed_data is None:
                    table_name = os.path.basename(path).split('.')[0]
                    insert_table(df_batch, table_name)
                    logging.info(f"Inserted batch no. {batch_count} for table: {table_name} in database")
                    return

                for sub_table_name, df in transformed_data.items():
                    insert_table(df, sub_table_name)
                    logging.info(f"Inserted batch no. {batch_count} for table: {sub_table_name}")

                batch_count += 1
                batch = reader.next_batches(batch_count)

            logging.info(f"Completed processing for file: {path}")
        return wrapper
    return decorator

@batch_insert(path='title_ratings.tsv')
def insert_title_ratings():
    return None

@batch_insert(path='title_episode.tsv')
def insert_title_ratings():
    return None

@batch_insert(path='title_principals.tsv')
def insert_title_principals():
    return None

@batch_insert(path='title_akas.tsv')
def transform_title_basics(df: pl.DataFrame):
    df = df.with_columns(
        pl.when(pl.col("isAdult") == 1).then(True)
        .otherwise(False)
        .alias("isAdult")
    )

    genres_df = df.select(
        pl.col("tconst"),
        pl.col("genres").str.split(',').alias("genre")
    ).explode("genre").drop_nulls()

    df = df.drop("genres")
    return {'title_basics': df, 'genres': genres_df}

@batch_insert(path='title_akas.tsv')
def transform_title_akas(df: pl.DataFrame):
    types_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("types").str.split(',').alias("type")
    ).explode("type").drop_nulls()

    attributes_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("attributes").str.split(',').alias("attribute")
    ).explode("attribute").drop_nulls()

    df = df.drop("attributes").drop("types")

    return {
        'title_akas': df,
        'types': types_df,
        'attributes': attributes_df
    }

@batch_insert(path='title_crew.tsv')
def transform_title_crew(df: pl.DataFrame):
    directors_df = df.select(
        pl.col("tconst"),
        pl.col("directors").str.split(',').alias("director")
    ).explode("director").drop_nulls()

    writers_df = df.select(
        pl.col("tconst"),
        pl.col("writers").str.split(',').alias("writer")
    ).explode("writer").drop_nulls()

    return {'crew_directors': directors_df, 'crew_writers': writers_df}

@batch_insert(path='name_basics.tsv')
def transform_name_basics(df: pl.DataFrame):
    professions = df.select(
        pl.col("nconst"),
        pl.col("primaryProfession").str.split(',').alias("profession")
    ).explode("profession").drop_nulls()

    knownForTitles = df.select(
        pl.col("nconst"),
        pl.col("knownForTitles").str.split(',').alias("title")
    ).explode("title").drop_nulls()

    df = df.drop("primaryProfession").drop("knownForTitles")
    return {
        'name_basics': df,
        'knownForTitles': knownForTitles,
        'primaryProfession': professions
    }


def create_schema(conn: sqlite3.Connection,overwrite: bool = True) -> bool:
    logging.info(f"Creating schema {db_name}")
    with open('create_schema.sql', 'r') as schema:
        script = schema.read()
        conn.executescript(script)
        logging.info(f"Created schema {db_name}")

with sqlite3.connect(db_name) as conn:
    create_schema(conn)

transform_title_akas()
transform_title_crew()
transform_title_basics()
insert_title_principals()
insert_title_ratings()