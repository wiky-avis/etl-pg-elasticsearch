from common.resources import ResourcesMixin
from elasticsearch.helpers import streaming_bulk
from settings import BLOCK_SIZE, BODY_SETTINGS, INDEX_NAME

from postgres_to_es.common.coro import generate_data_coroutine, transform_coroutine, producer_generator, \
    coro_loader


# class PGSQLLoader(ResourcesMixin):
#     def __call__(self) -> Tuple[FilmWork]:
#         cursor = self.get_pg_conn().cursor()
#         cursor.execute(SQL)
#
#         while True:
#             page = cursor.fetchmany(BLOCK_SIZE)
#             if not page:
#                 break
#             for data in page:
#                 yield FilmWork(**data)
#
#         self.get_pg_conn().close()


# class GenerateData:
#     def __call__(self) -> dict:
#         list_data = PGSQLLoader()
#
#         for row in list_data():
#             json_schema = {
#                 "id": row.id,
#                 "imdb_rating": row.rating,
#                 "genre": row.genres_names,
#                 "title": row.title,
#                 "description": row.description,
#                 "director": row.directors_names,
#                 "actors_names": row.actors_names,
#                 "writers_names": row.writers_names,
#                 "actors": [
#                     {"id": _id, "name": name}
#                     for _id, name in zip(
#                         row.actors_ids.replace("{", "")
#                         .replace("}", "")
#                         .split(","),
#                         row.actors_names,
#                     )
#                 ]
#                 if row.actors_ids
#                 else None,
#                 "writers": [
#                     {"id": _id, "name": name}
#                     for _id, name in zip(
#                         row.writers_ids.replace("{", "")
#                         .replace("}", "")
#                         .split(","),
#                         row.writers_names,
#                     )
#                     if _id
#                 ]
#                 if row.writers_ids
#                 else None,
#             }
#             json_data = json.dumps(json_schema)
#
#             yield json_data


class ElasticIndexCreator(ResourcesMixin):
    def __call__(self):
        self.get_es_client().indices.create(
            index=INDEX_NAME,
            body=BODY_SETTINGS,
            ignore=400,
        )


class ElasticSaver(ResourcesMixin):
    def __call__(self):
        while json_data := (yield):
            successes = 0
            failed = 0
            for ok, item in streaming_bulk(
                client=self.get_es_client(),
                index="movies",
                actions=json_data,
                chunk_size=BLOCK_SIZE,
            ):
                if not ok:
                    failed += 1
                else:
                    successes += 1

            print("Indexed %d/%d documents" % (successes, failed))


class ETLManager(ResourcesMixin):

    def __call__(self):
        cursor = self.get_pg_conn().cursor()
        client = self.get_es_client()

        loader = coro_loader(client)
        transform = generate_data_coroutine(loader)
        merger = transform_coroutine(transform)
        producer_generator(merger, cursor=cursor, storage=self.resources.storage)

        self.get_pg_conn().close()
