import sqlite3

db_name = 'actorfilms.db'

def get_movies_from_actor(actor_name: str):
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT film.name
        FROM filmCast
        JOIN film ON film.filmId = filmCast.filmId
        JOIN actor ON actor.actorId = filmCast.actorId
        WHERE actor.name = ?
        """
        movies = con.execute(query, (actor_name,)).fetchall()
        movie_names = [film[0] for film in movies]
    if not movie_names:
        raise ValueError(f'Actor {actor_name} has no films or could not be found')

    return movie_names

def get_actors_from_movie(movie_name: str):
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT actor.name
        FROM filmCast
        JOIN film ON filmCast.filmId = filmCast.filmId
        JOIN actor ON actor.actorId = filmCast.actorId
        WHERE actor.name = ?
        """

        actors = con.execute(query, (movie_name,)).fetchall()
        actor_names = [actor[0] for actor in actors]

    if not actor_names:
        raise ValueError(f'Movie {movie_name} has no actors or could not be found')

    return actor_names
