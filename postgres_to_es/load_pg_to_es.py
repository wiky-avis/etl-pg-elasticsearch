from pathlib import Path

from common.actions import ElasticIndexCreator, ETLManager


BASEDIR = Path(__file__).resolve(strict=True).parent.parent

if __name__ == "__main__":
    create_index = ElasticIndexCreator()
    create_index()

    etl = ETLManager()
    etl()
