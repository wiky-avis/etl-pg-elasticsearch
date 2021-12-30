from common.actions import ElasticIndexCreator, ETLManager


if __name__ == "__main__":
    create_index = ElasticIndexCreator()
    create_index()

    etl = ETLManager()
    etl()
