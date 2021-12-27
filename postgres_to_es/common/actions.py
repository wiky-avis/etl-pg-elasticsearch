import json
from typing import Tuple

from common.resources import ResourcesMixin
from elasticsearch.helpers import streaming_bulk
from models.movies import FilmWork
from settings import BLOCK_SIZE, BODY_SETTINGS, INDEX_NAME

from postgres_to_es.sql.get_movies import SQL


class PGSQLLoader(ResourcesMixin):
    def __call__(self) -> Tuple[FilmWork]:
        cursor = self.resources.pg_conn.cursor()
        cursor.execute(SQL)

        while True:
            page = cursor.fetchmany(BLOCK_SIZE)
            if not page:
                break
            for data in page:
                yield FilmWork(**data)

        self.resources.pg_conn.close()


class GenerateData:
    def __call__(self) -> dict:
        list_data = PGSQLLoader()

        for row in list_data():
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


class ElasticIndexCreator(ResourcesMixin):
    def __call__(self):
        self.resources.es_client.indices.create(
            index=INDEX_NAME,
            body=BODY_SETTINGS,
            ignore=400,
        )


class ElasticSaver(ResourcesMixin):
    def __call__(self):
        generate_actions = GenerateData()
        successes = 0
        failed = 0
        for ok, item in streaming_bulk(
            client=self.resources.es_client,
            index="movies",
            actions=generate_actions(),
            chunk_size=BLOCK_SIZE,
        ):
            if not ok:
                failed += 1
            else:
                successes += 1

        print("Indexed %d/%d documents" % (successes, failed))
