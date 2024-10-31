import logging
import os.path
import sqlite3
from dataclasses import dataclass
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

def insert_table(df: pl.DataFrame, table_name:str):
    # TODO: write stuff in a single transaction
    df.write_database(table_name, f'sqlite:///{db_name}', if_table_exists='append')

def batch_transform(path: str):
    def decorator(transform_fn: Callable):
        @wraps(transform_fn)
        def wrapper(*args, **kwargs):
            logging.info(f"Starting batch processing for file: {path}")

            reader = pl.read_csv_batched(path, separator='\t' ,null_values=['\\N'], quote_char=None, batch_size=batch_size)
            batch_count = 1
            batch = reader.next_batches(max_batches)

            while batch:
                logging.info(f"Processing batch no. {batch_count} for table: {path}")

                transformed_data = transform_fn(pl.concat(batch), *args, **kwargs)

                logging.info(f"Transformed batch no. {batch_count} for table: {path}, now inserting {len(transformed_data)} table(s)")
                for sub_table_name, df in transformed_data.items():
                    insert_table(df, sub_table_name)
                    logging.info(f"Inserted batch no. {batch_count} for table: {sub_table_name}")

                batch_count += 1
                batch = reader.next_batches(batch_count)


            logging.info(f"Completed processing for file: {path}")
        return wrapper
    return decorator


def insert_no_transforms():
    no_transforms = ['title_ratings', 'title_episode', 'title_principals']
    for table in no_transforms:
        reader = pl.read_csv_batched(f'{table}.tsv', separator='\t' ,null_values=['\\N'], quote_char=None, batch_size=batch_size)
        batch_count = 1
        batch = reader.next_batches(max_batches)
        while batch:
            logging.info(f"Inserting batch no. {batch_count} for table: {table}")
            insert_table(pl.concat(batch), table)
            logging.info(f"Finished inserting batch no. {batch_count} table: {table}")
            batch_count += 1
            batch = reader.next_batches(batch_count)

def transform_title_basics():
    logging.info(f"Transforming basic titles")
    df = pl.read_csv('title_basics.tsv', separator='\t', null_values=['\\N'], quote_char=None)
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

    logging.info(f"Transformed basic titles")
    return {'title_basics': df, 'genres': genres_df}

@batch_transform(path='title_akas.tsv')
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

@batch_transform(path='title_crew.tsv')
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

@batch_transform(path='name_basics.tsv')
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
    if os.path.exists(db_name):
        if overwrite:
            os.remove(db_name)
        else:
            return False
    with open('create_schema.sql', 'r') as schema:
        script = schema.read()
        conn.executescript(script)
        logging.info(f"Created schema {db_name}")

# TODO: add df.to_sql for each table, append to existing schema
insert_no_transforms()