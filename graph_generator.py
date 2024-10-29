import logging
import sqlite3
import sys
from queue import Queue
from typing import Optional, Tuple, Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

db_name = 'actorfilms.db'

def build_graph() -> Dict[str, List[str]]:
    logging.info("Building the graph with actor-film relationships")
    G = {}

    with sqlite3.connect(db_name) as con:
        query = """
        SELECT actor.name, film.filmId
        FROM filmCast
        JOIN film ON film.filmId = filmCast.filmId
        JOIN actor ON actor.actorId = filmCast.actorId
        """
        results = con.execute(query).fetchall()

        for actor, film in results:
            G.setdefault(actor, []).append(film)
            G.setdefault(film, []).append(actor)

        logging.info("Graph building complete")
    return G

def get_distance(G: Dict[str, List[str]], actorA: str, actorB: str) -> Optional[Tuple[int, Dict[str, str]]]:
    if actorA not in G or actorB not in G:
        logging.error(f"One or both actors '{actorA}' and '{actorB}' not found in the graph.")
        return None

    parent = {}
    distances = {actorA: 0}
    q = Queue()
    q.put(actorA)

    while not q.empty():
        actor = q.get()
        if actor == actorB:
            return distances[actor], parent
        for neighbour in G[actor]:
            if neighbour not in distances:
                parent[neighbour] = actor
                distances[neighbour] = distances[actor] + 1
                q.put(neighbour)
    return None

def get_kevin_bacon_number(g: Dict[str, List[str]], actor_name: str) -> Optional[Tuple[int, Dict[str, str]]]:
    return get_distance(g, actor_name, "Kevin Bacon")

def convert_movie_id_name(connection, movie_id):
    query = connection.execute("""
    SELECT name
    FROM film
    WHERE filmId = ?
    """, (movie_id,))

    name = query.fetchone()
    if name is None:
        return None

    return name[0]

def show_kevin_bacon_number(G, actor_name:str):
    with sqlite3.connect(db_name) as con:
        result = get_kevin_bacon_number(G, actor_name)
        if result is None:
            print(f"No connection found for actor {actor_name}.", file=sys.stderr)
        else:
            distance, parent = result
            print(f"Kevin Bacon number for {actor_name}: {distance}")

            # Trace the path
            actor = "Kevin Bacon"
            path = []
            while actor in parent:
                movie_name = convert_movie_id_name(con, actor)
                path.append(movie_name if movie_name is not None else actor)
                actor = parent[actor]
            path.append(actor_name)

            print(" -> ".join(reversed(path)))


if __name__ == "__main__":
    G = build_graph()
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT name
        FROM actor
        ORDER BY RANDOM()
        """
        actors = con.execute(query).fetchall()

    for row in actors:
        show_kevin_bacon_number(G, row[0])
        print("")

