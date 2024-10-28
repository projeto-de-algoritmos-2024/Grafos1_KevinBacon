import os.path
import sqlite3
import sys
import pandas as pd

db_name = 'actorfilms.db'

if os.path.isfile(db_name):
    response = input('File exists, overwrite?[Y/N]: ')
    if response.upper() == 'N':
        sys.exit(1)
    else:
        os.remove(db_name)

con = sqlite3.connect(db_name)
con.autocommit = False
con.execute("PRAGMA foreign_keys = 1")
cur = con.cursor()

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

data = pd.read_csv('actorfilms.csv')

for index, row in data.iterrows():
    actor_exists = cur.execute("SELECT * FROM actor WHERE actorId = ?", (row['ActorID'],))
    if actor_exists.fetchone() is None:
        cur.execute("INSERT INTO actor (actorId, name) VALUES (?, ?)", (row['ActorID'], row['Actor']))

    film_exists = cur.execute("SELECT * FROM film WHERE filmId = ?", (row['FilmID'],))
    if film_exists.fetchone() is None:
        cur.execute("INSERT INTO film (filmId, name, year, votes, rating) VALUES (?, ?, ?, ?, ?)", (row['FilmID'], row['Film'], row['Year'], row['Votes'], row['Rating']))

    cur.execute("INSERT INTO filmCast (actorId, filmId) VALUES (?, ?)", (row['ActorID'], row['FilmID']))


con.commit()
con.close()