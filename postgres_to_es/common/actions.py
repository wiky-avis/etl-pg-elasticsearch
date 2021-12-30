import json
from datetime import datetime
from typing import Coroutine

from base.decorators import coroutine
from base.state_manager import BaseStorage, State
from common.resources import ResourcesMixin
from elasticsearch.helpers import streaming_bulk
from models.movies import FilmWork
from settings import BLOCK_SIZE, BODY_SETTINGS, INDEX_NAME
from sql.get_movies import SQL


def producer_generator(target: Coroutine, *, cursor, storage: BaseStorage):
    state_manager = State(storage=storage)
    default_date = str(datetime(year=2021, month=1, day=1))
    current_state = state_manager.state.get("film_work", default_date)

    cursor.execute(SQL, (current_state,))

    while True:
        page = cursor.fetchmany(BLOCK_SIZE)
        if not page:
            break
        target.send(page)

        state_manager.set_state(
            key="film_work", value=str(page[-1]["updated_at"])
        )


@coroutine
def transform_coroutine(target: Coroutine):
    while page := (yield):
        movies = []
        for data in page:
            movies.append(FilmWork(**data))
        target.send(movies)


class GenerateData:
    def __call__(self, list_data) -> dict:
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
                    {"id": _id, "name": name}
                    for _id, name in zip(
                        row.actors_ids.replace("{", "")
                        .replace("}", "")
                        .split(","),
                        row.actors_names,
                    )
                ]
                if row.actors_ids
                else None,
                "writers": [
                    {"id": _id, "name": name}
                    for _id, name in zip(
                        row.writers_ids.replace("{", "")
                        .replace("}", "")
                        .split(","),
                        row.writers_names,
                    )
                    if _id
                ]
                if row.writers_ids
                else None,
            }
            json_data = json.dumps(json_schema)

            yield json_data


@coroutine
def coro_loader(es_client):
    while movies := (yield):
        generator_data = GenerateData()
        successes = 0
        failed = 0
        for ok, item in streaming_bulk(
            client=es_client,
            index="movies",
            actions=generator_data(movies),
            chunk_size=BLOCK_SIZE,
        ):
            if not ok:
                failed += 1
            else:
                successes += 1

        print("Indexed %d/%d documents" % (successes, failed))


class ElasticIndexCreator(ResourcesMixin):
    def __call__(self):
        self.get_es_client().indices.create(
            index=INDEX_NAME,
            body=BODY_SETTINGS,
            ignore=400,
        )


class ETLManager(ResourcesMixin):
    def __call__(self):
        cursor = self.get_pg_conn().cursor()
        client = self.get_es_client()

        loader = coro_loader(client)
        merger = transform_coroutine(loader)
        producer_generator(
            merger, cursor=cursor, storage=self.resources.storage
        )

        self.get_pg_conn().close()
