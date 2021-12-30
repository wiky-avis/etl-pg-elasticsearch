import json

from base.decorators import coroutine

from models.movies import FilmWork
from datetime import datetime
from typing import Coroutine

from base.state_manager import BaseStorage, State

from settings import BLOCK_SIZE
from sql.get_movies import SQL
from elasticsearch.helpers import streaming_bulk


def producer_generator(target: Coroutine, *, cursor, storage: BaseStorage):
    state_manager = State(storage=storage)
    default_date = str(datetime(year=2021, month=1, day=1))
    current_state = state_manager.state.get('film_work', default_date)

    cursor.execute(SQL, (current_state,))

    while True:
        page = cursor.fetchmany(BLOCK_SIZE)
        if not page:
            break
        target.send(page)

        state_manager.set_state(
            key='film_work', value=str(page[-1]['updated_at']))


@coroutine
def transform_coroutine(target: Coroutine):
    while page := (yield):
        movies = []
        for data in page:
            movies.append(FilmWork(**data))
        target.send(movies)


def generate_data_coroutine(movies):
    for data in movies:
        json_schema = {
            "id": data.id,
            "imdb_rating": data.rating,
            "genre": data.genres_names,
            "title": data.title,
            "description": data.description,
            "director": data.directors_names,
            "actors_names": data.actors_names,
            "writers_names": data.writers_names,
            "actors": [
                {"id": _id, "name": name}
                for _id, name in zip(
                    data.actors_ids.replace("{", "")
                        .replace("}", "")
                        .split(","),
                    data.actors_names,
                )
            ]
            if data.actors_ids
            else None,
            "writers": [
                {"id": _id, "name": name}
                for _id, name in zip(
                    data.writers_ids.replace("{", "")
                        .replace("}", "")
                        .split(","),
                    data.writers_names,
                )
                if _id
            ]
            if data.writers_ids
            else None,
        }
        json_data = json.dumps(json_schema)

        yield json_data


@coroutine
def coro_loader(es_client):
    while movies := (yield):
        successes = 0
        failed = 0
        for ok, item in streaming_bulk(
            client=es_client,
            index="movies",
            actions=generate_data_coroutine(movies),
            chunk_size=BLOCK_SIZE,
        ):
            if not ok:
                failed += 1
            else:
                successes += 1

        print("Indexed %d/%d documents" % (successes, failed))
