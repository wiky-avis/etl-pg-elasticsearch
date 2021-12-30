from common.actions import ElasticIndexCreator, ETLManager
from pathlib import Path

BASEDIR = Path(__file__).resolve(strict=True).parent.parent

if __name__ == "__main__":
    create_index = ElasticIndexCreator()
    create_index()

    # pg_loader = ElasticSaver()
    # pg_loader()

    # pg = PGSQLLoader()
    # pg()
    etl = ETLManager()
    etl()
