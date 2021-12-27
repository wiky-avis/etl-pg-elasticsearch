import backoff
from common.actions import ElasticSaver
from elasticsearch import Elasticsearch
from settings import BODY_SETTINGS, INDEX_NAME, URL


def create_index():
    es_client = Elasticsearch(URL)
    es_client.indices.create(
        index=INDEX_NAME,
        body=BODY_SETTINGS,
        ignore=400,
    )


if __name__ == "__main__":
    create_index()

    pg_loader = ElasticSaver()
    pg_loader()
