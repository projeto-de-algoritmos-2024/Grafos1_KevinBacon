import pickle
import sqlite3
import sys
from collections import deque
from typing import Optional, Tuple, Dict, List

from PIL.ImageFile import ERRORS

adj_list = Dict[int, List[int]]
db_name = 'imdb.db'
graph_file = 'actor_film_graph.pkl'

def build_graph() -> adj_list:
    print("Building graph with actor-film relationships...")
    G = {}

    with sqlite3.connect(db_name) as con:
        query = """
        SELECT DISTINCT nconst, tconst
        FROM title_principals
        WHERE title_principals.category = 'actor'
        """
        results = con.execute(query).fetchall()

        for actor, film in results:
            G.setdefault(actor, []).append(film*-1)
            G.setdefault(film*-1, []).append(actor)

    print("Graph building complete.")
    return G

def save_graph(g: adj_list, filename: str):
    with open(filename, 'wb') as f:
        pickle.dump(g, f)
    print(f"Graph saved to {filename}.")

def load_graph(filename: str) -> adj_list:
    with open(filename, 'rb') as f:
        g = pickle.load(f)
    print(f"Graph loaded from {filename}.")
    return g

def get_actor_id(actor_name: str):
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT nconst
        FROM name_basics
        WHERE primaryName = ?
        COLLATE NOCASE
        LIMIT 1;
        """
        results = con.execute(query, (actor_name,)).fetchone()
        if results:
            return results[0]
        return None

def get_movie_name(movie_id: int):
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT primaryTitle
        FROM title_basics
        WHERE tconst = ?
        COLLATE NOCASE
        LIMIT 1;
        """
        results = con.execute(query, (movie_id,)).fetchone()
        if results:
            return results[0]
        raise ValueError(f"Movie {movie_id} not found.")

def get_actor_name(actor_id: int):
    with sqlite3.connect(db_name) as con:
        query = """
        SELECT primaryName
        FROM name_basics
        WHERE nconst = ?
        COLLATE NOCASE
        LIMIT 1;
        """
        results = con.execute(query, (actor_id,)).fetchone()
        if results:
            return results[0]
        raise ValueError(f"Actor {actor_id} not found.")


def bfs_distance(g: adj_list, start: int, end: int) -> Optional[Tuple[int, Dict[int, int]]]:
    if start not in g or end not in g:
        return None

    parent = {}
    distances = {start: 0}
    queue = deque([start])

    while queue:
        current = queue.popleft()
        if current == end:
            return distances[current], parent
        for neighbor in g[current]:
            if neighbor not in distances:
                parent[neighbor] = current
                distances[neighbor] = distances[current] + 1
                queue.append(neighbor)
    return None

def display_bfs(g, start, end):
    print(f"\nSearching path from '{start}' to '{end}'...")

    result = bfs_distance(g, start, end)
    if result is None:
        print(f"No connection found between {start} and {end}.")
    else:
        distance, parent = result
        print(f"Distance between {start} and {end}: {distance}")
        path = []
        current = end
        while current in parent:
            path.append(current)
            current = parent[current]
        path.append(start)

        print("Path:")
        for step in reversed(path):
            if step < 0:
                print(get_movie_name(step*-1))
            else:
                print(get_actor_name(step))

if __name__ == "__main__":
    try:
        G = load_graph(graph_file)
        print("Found cached graph, loading.")
    except FileNotFoundError:
        G = build_graph()
        save_graph(G, graph_file)
    start_actor_name = input("Enter the name of the starting actor: ")
    end_actor_name = "Kevin Bacon"

    start_id = get_actor_id(start_actor_name)
    end_id = get_actor_id(end_actor_name)

    if start_id is None:
        print(f'Error while searching for actor {start_actor_name}.', file=sys.stderr)
    if end_id is None:
        print(f'Error while searching for actor {end_actor_name}.', file=sys.stderr)

    display_bfs(G, start_id, end_id)
