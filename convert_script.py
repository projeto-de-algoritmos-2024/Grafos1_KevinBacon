import os.path
import sqlite3
import sys
import pandas as pd
import logging

# Set up logging configuration
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

db_name = 'actorfilms.db'

# Check if the database file exists
if os.path.isfile(db_name):
    logging.info("Removing existing database file.")
    os.remove(db_name)

# Connect to the database
con = sqlite3.connect(db_name)
con.autocommit = False
con.execute("PRAGMA foreign_keys = 1")
cur = con.cursor()

# Create tables
logging.info("Creating tables in the database.")
cur.execute("""
CREATE TABLE actor (
    actorId TEXT NOT NULL,
    name TEXT NOT NULL,
    CONSTRAINT actor_PK PRIMARY KEY(actorId)
);
""")

cur.execute("""
CREATE TABLE film (
    filmId TEXT NOT NULL,
    name TEXT NOT NULL,
    year INT NOT NULL,
    votes INT NOT NULL,
    rating REAL NOT NULL,
    CONSTRAINT film_PK PRIMARY KEY(filmId)
); 
""")

cur.execute("""
CREATE TABLE filmCast (
    actorId TEXT NOT NULL,
    filmId TEXT NOT NULL,
    CONSTRAINT movieCast_UK UNIQUE(actorId, filmId),
    CONSTRAINT movieCast_actor_FK FOREIGN KEY (actorId)
        REFERENCES actor(actorId)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    CONSTRAINT movieCast_film_FK FOREIGN KEY (filmId)
        REFERENCES film(filmId)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);
""")

cur.execute("""
CREATE VIEW actors_in_film AS 
SELECT actor.name, film.name, film.year, film.votes, film.rating 
FROM filmCast
JOIN actor ON actor.actorId = filmCast.actorId
JOIN film ON film.filmId = filmCast.filmId
""")

# Load data from CSV
logging.info("Loading data from 'actorfilms.csv'.")
data = pd.read_csv('actorfilms.csv')

# Prepare to insert data into tables
actor_inserted = 0
film_inserted = 0
film_cast_inserted = 0

# Insert data into tables
for index, row in data.iterrows():
    actor_exists = cur.execute("SELECT * FROM actor WHERE actorId = ?", (row['ActorID'],))
    if actor_exists.fetchone() is None:
        cur.execute("INSERT INTO actor (actorId, name) VALUES (?, ?)", (row['ActorID'], row['Actor']))
        actor_inserted += 1

    film_exists = cur.execute("SELECT * FROM film WHERE filmId = ?", (row['FilmID'],))
    if film_exists.fetchone() is None:
        cur.execute("INSERT INTO film (filmId, name, year, votes, rating) VALUES (?, ?, ?, ?, ?)", (row['FilmID'], row['Film'], row['Year'], row['Votes'], row['Rating']))
        film_inserted += 1

    film_cast_inserted += 1
    cur.execute("INSERT INTO filmCast (actorId, filmId) VALUES (?, ?)", (row['ActorID'], row['FilmID']))

# Commit changes and close connection
con.commit()
logging.info(f"Inserted {actor_inserted} actors, {film_inserted} films, and {film_cast_inserted} film casts into the database.")
con.close()
logging.info("Data committed to the database, deleting .csv file")
os.remove("actorfilms.csv")
