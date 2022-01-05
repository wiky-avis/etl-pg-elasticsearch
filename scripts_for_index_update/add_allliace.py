import requests


ES_URL = "http://0.0.0.0:9200"


message = {
    "actions": [
        {"add": {"index": "movie_movie_v1", "alias": "movies_all"}},
        {
            "add": {
                "index": "movie_movie_v1",
                "alias": "movie_type_movie_v1",
                "filter": {"term": {"movie_type": "movie"}},
            }
        },
        {
            "add": {
                "index": "movie_movie_v1",
                "alias": "movie_type_tv_show_v1",
                "filter": {"term": {"movie_type": "tv_show"}},
            }
        },
        {"add": {"index": "movie_movie_v1", "alias": "movie_type"}},
    ]
}


if __name__ == "__main__":

    resp = requests.post(f"{ES_URL}/_aliases", json=message)
    print(resp, resp.text)
