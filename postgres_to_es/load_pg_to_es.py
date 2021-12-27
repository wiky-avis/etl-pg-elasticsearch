import logging
import json
import sqlite3
from collections import namedtuple
from typing import List, Generator, Any

import backoff
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from const import BODY_SETTINGS, DSL, INDEX_NAME, URL, BLOCK_SIZE
from models import FilmWork

SQL = """
    WITH x as (
    SELECT fw.id, array_agg(pfw.id) as actors_ids, array_agg(p.full_name) as actors_names FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id and pfw.role = 'actor'
    LEFT JOIN content.person p on pfw.person_id = p.id
    where p.full_name is not null
    GROUP BY fw.id
    ), y as(
    SELECT fw.id, array_agg(pfw.id) as directors_ids, array_agg(p.full_name) as directors_names FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id and pfw.role = 'director'
    LEFT JOIN content.person p on pfw.person_id = p.id
    where p.full_name is not null
    GROUP BY fw.id
    ), z as(
    SELECT fw.id, array_agg(pfw.id) as writers_ids, array_agg(p.full_name) as writers_names
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw on fw.id = pfw.film_work_id
    LEFT JOIN content.person p on pfw.person_id = p.id and pfw.role = 'writer'
    where p.full_name is not null
    GROUP BY fw.id
    ), g as(
    SELECT fw.id, array_agg(gfw.id) as genres_ids, array_agg(g.name) as genres_names FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw on fw.id = gfw.film_work_id
    LEFT JOIN content.genre g on gfw.genre_id = g.id
    GROUP BY fw.id
    )
    SELECT fw.id, title, description, rating, x.actors_ids, x.actors_names, y.directors_ids, y.directors_names, z.writers_ids, z.writers_names, g.genres_ids, g.genres_names
    FROM content.film_work fw 
    LEFT JOIN x ON fw.id = x.id
    LEFT JOIN y ON fw.id = y.id
    LEFT JOIN z ON fw.id = z.id
    LEFT JOIN g ON fw.id = g.id
"""

pg_conn = psycopg2.connect(**DSL, cursor_factory=DictCursor)
cursor = pg_conn.cursor()


def create_index(client):
    client.indices.create(
        index=INDEX_NAME,
        body=BODY_SETTINGS,
        ignore=400,
    )


def load_from_pgdb() -> Generator[FilmWork, Any, None]:
    cursor.execute(SQL)

    while True:
        page = cursor.fetchmany(BLOCK_SIZE)
        if not page:
            break
        for data in page:
            yield FilmWork(**data)


def generate_actions() -> dict:
    list_data = load_from_pgdb()
    for row in list_data:
        json_schema = {
              "id": row.id,
              "imdb_rating": row.rating,
              "genre": row.genres_names,
              "title": row.title,
              "description": row.description,
              "director": row.directors_names,
              "actors_names": row.actors_names,
              "writers_names": row.writers_names,
              "actors": [
                {"id": _id, "name": name} for _id, name in zip(row.actors_ids.replace('{', '').replace('}', '').split(","), row.actors_names)
              ] if row.actors_ids else None,
              "writers": [
                  {"id": _id, "name": name} for _id, name in zip(row.writers_ids.replace('{', '').replace('}', '').split(","), row.writers_names) if _id
              ] if row.writers_ids else None
        }
        json_data = json.dumps(json_schema)

        yield json_data


def load_to_es():
    es_client = Elasticsearch(URL)
    create_index(es_client)

    successes = 0
    failed = 0
    for ok, item in streaming_bulk(
            client=es_client, index="movies", actions=generate_actions(), chunk_size=BLOCK_SIZE
    ):
        if not ok:
            failed += 1
        else:
            successes += 1

    print("Indexed %d/%d documents" % (successes, failed))


if __name__ == "__main__":
    load_to_es()

    pg_conn.close()
