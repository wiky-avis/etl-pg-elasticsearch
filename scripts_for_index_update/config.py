import os
from distutils.util import strtobool


ES_URL = "http://0.0.0.0:9200"

# посмотреть что получается
REALLY = strtobool(os.environ.get("REALLY", "True"))

JSON_DUMP_PARAMS = {"sort_keys": True, "indent": 2, "separators": (",", ": ")}
BASE_DIR = "./update_data/"

# если True - обновляет маппинг перед PUT'ом
RECOURSIVE_MAPPING_UPDATE = True

REALIACE_FILE = BASE_DIR + "realiace.json"
DIR_NAME_FILE = BASE_DIR + "dir_name.json"
DELETE_FILE = BASE_DIR + "delete.json"

# Взаимоисключающие переменные. Если задан список INCLUDE_INDICES, то обновляются только указанные в нем индексы.
# Иначе обновляются все индексы, кроме указанных в EXCLUDE_INDICES.
INCLUDE_INDICES = [
    "movies"
]
EXCLUDE_INDICES = [
    ".geoip_databases",
]
