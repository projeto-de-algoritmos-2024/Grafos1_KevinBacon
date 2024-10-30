import logging
import polars as pl

db_name = 'imdb.db'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("app.log"),
                        logging.StreamHandler()
                    ])

# TODO: add constraints

def transform_title_basics():
    df = pl.read_csv('title_basics.tsv', separator='\t', null_values=['\\N'], quote_char=None)
    df = df.with_columns(
        pl.when(pl.col("isAdult") == 1).then(True)
        .otherwise(False)
        .alias("isAdult")
    )

    genres_df = df.select(
        pl.col("tconst"),
        pl.col("genres").str.split(',').alias("genres")
    ).explode("genres")

    df = df.drop("genres")

    return [df, genres_df]

def transform_title_akas():
    df = pl.read_csv('title_akas.tsv', separator='\t' ,null_values=['\\N'], quote_char=None)

    types_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("types").str.split(',').alias("types")
    ).explode("types")

    attributes_df = df.select(
        pl.col("titleId"),
        pl.col("ordering"),
        pl.col("attributes").str.split(',').alias("attributes")
    ).explode("attributes")

    df = df.drop("attributes").drop("types")

    return [df, types_df, attributes_df]

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

    return [directors_df, writers_df]

def transform_name_basics():
    df = pl.read_csv('name_basics.tsv', null_values=['\\N'], quote_char=None, separator='\t')

    professions = df.select(
        pl.col("nconst"),
        pl.col("primaryProfession").str.split(',').alias("primaryProfession")
    ).explode("primaryProfession")

    knownForTitles = df.select(
        pl.col("nconst"),
        pl.col("knownForTitles").str.split(',').alias("knownForTitles")
    ).explode("knownForTitles")

    return [df, professions, knownForTitles]
