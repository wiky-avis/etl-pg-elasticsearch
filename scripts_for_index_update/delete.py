import json
from typing import NoReturn

import requests

from scripts_for_index_update.config import DELETE_FILE, ES_URL


with open(DELETE_FILE) as f:
    index_names = json.load(f)["index_names"]


def delete_index() -> NoReturn:
    """
    Удаляет индексы из ./update_data/delete.json
    """

    for index_name in index_names:
        _url = f"{ES_URL}/{index_name}"
        resp = requests.delete(_url)
        print(_url, resp.status_code, resp.text)


def delete_data_from_index():
    for index_name in index_names:
        _url = f"{ES_URL}/{index_name}/_delete_by_query"
        resp = requests.post(_url, json={"query": {"match_all": {}}})
        print(_url, resp.status_code, resp.text)


if __name__ == "__main__":
    delete_index()
