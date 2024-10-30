import logging
from dataclasses import dataclass

import polars as pl

db_name = 'imdb.db'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

@dataclass(frozen=True)

def no_transform(path: str):
    return pl.read_csv(path, separator='\t', null_values=['\\N'], quote_char=None)

def transform_title_basics():
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

    return {'title_basics': df, 'genres': genres_df}

def transform_title_akas():
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

    return {'title_akas': df, 'types': types_df, 'attributes': attributes_df}

def transform_title_crew():
    df = pl.read_csv('title_crew.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    directors_df = df.select(
        pl.col("tconst"),
        pl.col("directors").str.split(',').alias("directors")
    ).explode("directors")

    writers_df = df.select(
        pl.col("tconst"),
        pl.col("writers").str.split(',').alias("writers")
    ).explode("writers")

    return {'directors_crew': directors_df, 'writers_crew': writers_df}

def transform_name_basics():
    df = pl.read_csv('name_basics.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    professions = df.select(
        pl.col("nconst"),
        pl.col("primaryProfession").str.split(',').alias("profession")
    ).explode("profession")

    knownForTitles = df.select(
        pl.col("nconst"),
        pl.col("knownForTitles").str.split(',').alias("title")
    ).explode("title")

    return {'name_basics': df, 'professions': professions, 'knownForTitles': knownForTitles}

def transform_title_principals():
    df = pl.read_csv('title_principals.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    characters = df.select(
        pl.col("tconst"),
        pl.col("ordering"),
        pl.col("characters").str.strip_chars("[]").str.split(',').alias("character")
    ).explode("character")

    df = df.drop("character")

    return {'title_principals': df, 'characters': characters}

print(transform_title_principals())
