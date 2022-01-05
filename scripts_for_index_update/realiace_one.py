import requests


index_new = "movie_movie_v1"

message = {
    "actions": [
        {"add": {"index": index_new, "alias": "movies_all"}},
        {"add": {"index": index_new, "alias": "movie_type_tv_show_v1"}},
        {"add": {"index": index_new, "alias": "movie_type_movie_v1"}},
    ]
}
ES_URL = "http://0.0.0.0:9200"

resp = requests.post(f"{ES_URL}/_aliases", json=message)
print(resp.status_code, resp.text)
