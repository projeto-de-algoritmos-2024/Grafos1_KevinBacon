import logging
import os.path
import sqlite3
from dataclasses import dataclass

import polars as pl

db_name = 'imdb.db'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

def no_transform(path: str):
    logging.info(f"No-transforming {path}")
    return pl.read_csv(path, separator='\t', null_values=['\\N'], quote_char=None)

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
    ).explode("genre")

    df = df.drop("genres")

    logging.info(f"Transformed basic titles")
    return {'title_basics': df, 'genres': genres_df}

def transform_title_akas():
    logging.info(f"Transforming aka titles")
    df = pl.read_csv('title_akas.tsv', separator='\t' ,null_values=['\\N'], quote_char=None)

    types_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("types").str.split(',').alias("type")
    ).explode("type")

    attributes_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("attributes").str.split(',').alias("attribute")
    ).explode("attribute")

    df = df.drop("attributes").drop("types")

    logging.info(f"Transformed aka titles")
    return {'title_akas': df, 'types': types_df, 'attributes': attributes_df}

def transform_title_crew():
    logging.info(f"Transforming crew titles")
    df = pl.read_csv('title_crew.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    directors_df = df.select(
        pl.col("tconst"),
        pl.col("directors").str.split(',').alias("director")
    ).explode("director")

    writers_df = df.select(
        pl.col("tconst"),
        pl.col("writers").str.split(',').alias("writer")
    ).explode("writer")

    logging.info(f"Transformed crew titles")
    return {'crew_directors': directors_df, 'crew_writers': writers_df}

def transform_name_basics():
    logging.info(f"Transforming basic names")
    df = pl.read_csv('name_basics.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    professions = df.select(
        pl.col("nconst"),
        pl.col("primaryProfession").str.split(',').alias("profession")
    ).explode("profession")

    knownForTitles = df.select(
        pl.col("nconst"),
        pl.col("knownForTitles").str.split(',').alias("title")
    ).explode("title")

    logging.info(f"Transformed basic names")
    return {'name_basics': df, 'professions': professions, 'knownForTitles': knownForTitles}

def transform_title_principals():
    logging.info(f"Transforming principal titles")
    df = pl.read_csv('title_principals.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    characters = df.select(
        pl.col("tconst"),
        pl.col("ordering"),
        pl.col("characters").str.strip_chars("[]").str.split(',').alias("character")
    ).explode("character")

    df = df.drop("character")

    logging.info(f"Transformed principal titles")
    return {'title_principals': df, 'characters': characters}

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
with sqlite3.connect(db_name) as conn:
    create_schema(conn)