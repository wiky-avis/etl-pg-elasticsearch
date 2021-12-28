from common.actions import ElasticIndexCreator, PGSQLLoader, ElasticSaver

if __name__ == "__main__":
    create_index = ElasticIndexCreator()
    create_index()

    pg_loader = ElasticSaver()
    pg_loader()
    #
    # pg = PGSQLLoader()
    # pg()
