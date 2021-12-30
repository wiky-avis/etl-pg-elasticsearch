import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg2
from base.decorators import backoff
from base.state_manager import BaseStorage, JsonFileStorage
from elasticsearch import Elasticsearch
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from settings import DSL, URL


BASEDIR = Path(__file__).resolve(strict=True).parent.parent


@dataclass
class Resources:
    es_client: Optional[Elasticsearch] = None
    pg_conn: _connection = None
    storage: Optional[BaseStorage] = None


class ResourcesMixin:
    @property
    def resources(self) -> Resources:
        return Resources(
            es_client=Elasticsearch(URL),
            pg_conn=psycopg2.connect(**DSL, cursor_factory=DictCursor),
            storage=JsonFileStorage(
                os.path.join(BASEDIR, "film_work_state.json")
            ),
        )

    @backoff()
    def get_es_client(self):
        return self.resources.es_client

    @backoff()
    def get_pg_conn(self) -> _connection:
        return self.resources.pg_conn
