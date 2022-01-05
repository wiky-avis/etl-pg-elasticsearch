"""
Скрипты помогают забрать данные из еластика;
данные для создания индексов с маппингами
и названия индексов
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from typing import List, NoReturn, Optional

import requests


from scripts_for_index_update.config import (
    BASE_DIR,
    DELETE_FILE,
    DIR_NAME_FILE,
    ES_URL,
    EXCLUDE_INDICES,
    INCLUDE_INDICES,
    JSON_DUMP_PARAMS,
    REALIACE_FILE,
    REALLY,
    RECOURSIVE_MAPPING_UPDATE,
)
from utils.utils import get_in

dir_name = BASE_DIR + datetime.now().isoformat()

print(f"[INFO] create dir {dir_name} for dumping index settings")

if not os.path.exists(BASE_DIR):
    os.mkdir(BASE_DIR)

try:
    os.mkdir(dir_name)
except OSError:
    print("[ERR] creation of the directory %s failed" % dir_name)
else:
    print("[INFO] successfully created the directory %s " % dir_name)

print("[INFO] create settings directory")
dir_name_settings = f"{dir_name}/settings/"
os.mkdir(dir_name_settings)

print("[INFO] create aliases directory")
dir_name_aliases = f"{dir_name}/aliases/"
os.mkdir(dir_name_aliases)

print("[INFO] create mappings directory")
dir_name_mappings = f"{dir_name}/mappings/"
os.mkdir(dir_name_mappings)

print("[INFO] create new_mappings directory")
dir_name_new_mappings = f"{dir_name}/new_mappings/"
os.mkdir(dir_name_new_mappings)

print("[INFO] create new_settings directory")
dir_name_new_settings = f"{dir_name}/new_settings/"
os.mkdir(dir_name_new_settings)


def download_indexes_names() -> List[str]:
    """
    Забираем все имена индексов
    """

    url_indexies = f"{ES_URL}/_cat/indices?format=json"
    resp = requests.get(url_indexies)
    body = resp.json()
    index_names = list(set(index["index"] for index in body))
    index_names = list(filter(get_index_names_filter(), index_names))
    index_names = sorted(index_names)

    return index_names


def get_index_names_filter():
    return is_index_included if INCLUDE_INDICES else is_index_not_excluded


def is_index_included(index_name):
    return index_name in INCLUDE_INDICES


def is_index_not_excluded(index_name):
    return index_name not in EXCLUDE_INDICES


def check_fields(dct, field, entity):
    for k in dct.keys():
        if k != field:
            print(f"[ERROR] Incorrect {entity} format")
            sys.exit(0)


def get_new_settings() -> Optional[dict]:
    if os.path.isfile("./new_settings.json"):
        with open("./new_settings.json") as f:
            settings = json.load(f)
            check_fields(settings, "settings", "new_settings")
            return settings
    return None


def save_indexes_meta(index_names: list):
    """
    Сохраняет мета-данные индексов в
    ./update_data/{datetime}/[settings,aliases,mappings]/{index_name}.json
    Алгоритм:
    ->  В цикле для каждого имени индекса:
        ->  Грузит мета-данные по имени индекса из эластика по {./config.ES_URL}
        ->  Удаляет ненужные данные из settings
        ->  Если есть ./new_settings.json - обновляет settings
        ->  Сохраняет полученные мета-данные индекса в
        соответствующией директории/файлы
    :param index_names: list: массив загруженных мета-данных индексов
    из эластика
    :return: None
    """

    for index_name in index_names:
        print(f"[INFO] saving index {index_name} meta")
        url_index = f"{ES_URL}/{index_name}"
        resp = requests.get(url_index)
        body = resp.json()
        name = list(body.keys())[0]
        body = body[name]
        body["settings"]["index"].pop("uuid")
        body["settings"]["index"].pop("version")
        body["settings"]["index"].pop("provided_name")
        body["settings"]["index"].pop("creation_date")

        with open(f"{dir_name}/settings/{index_name}.json", "w") as f:
            json.dump({"settings": body["settings"]}, f, **JSON_DUMP_PARAMS)

        with open(f"{dir_name}/aliases/{index_name}.json", "w") as f:
            json.dump({"aliases": body["aliases"]}, f, **JSON_DUMP_PARAMS)

        with open(f"{dir_name}/mappings/{index_name}.json", "w") as f:
            json.dump({"mappings": body["mappings"]}, f, **JSON_DUMP_PARAMS)


def up_index_version(index_name) -> str:
    """
    Обновляет версию индекса в названии *_v{version += 1}
    :param index_name: str: название предыдущей версии индекса
    """

    res = re.search(r"_v(\d+)", index_name)
    if not res:
        return index_name + "_v1"
    version = int(res.group(1))
    return index_name.replace(f"_v{version}", f"_v{version + 1}")


def create_indexes(index_names: list) -> list:
    """
    Создает новые индексы
    :param index_names: list: массив старые названий индексов
    :return: new_index_names: list: массив новых названий индексов
    """

    new_index_names = []
    for index_name in index_names:
        new_index_names.append(create_new_index(index_name))
    return new_index_names


def merge_settings(old, new, path=None):
    if path is None:
        path = []
    for key in new:
        if key in old:
            if isinstance(old[key], dict) and isinstance(new[key], dict):
                merge_settings(old[key], new[key], path + [str(key)])
            elif old[key] == new[key]:
                pass
            else:
                old[key] = new[key]
        else:
            old[key] = new[key]
    return old


def create_new_index(index_name: str):
    """
    Алгоритм внутри по шагам
    :param index_name: str: название текущей версии индекса
    :return: new_index_name: str: название обновленного индекса
    """

    print("*" * 10)
    # получаем новое имя индекса
    new_index_name = up_index_version(index_name)
    print(index_name, "->", new_index_name)

    # создаем новый индекс с настройками старого
    new_index_url = f"{ES_URL}/{new_index_name}"

    # читаем settings существующего индекса
    with open(f"{dir_name_settings}/{index_name}.json", "r+") as f:
        settings = json.load(f)
        # проверяем, есть ли изменения settings
        new_settings = get_new_settings()
        settings["settings"]["index"].pop("sort", None)

        if new_settings:
            print(f"[INFO] settings update: {new_settings}")
            merge_settings(settings, new_settings)
            print("[INFO] settings updated")

        # сохраняем новые settings для индекса
        with open(f"{dir_name}/new_settings/{new_index_name}.json", "w") as f:
            json.dump(settings, f, **JSON_DUMP_PARAMS)

    # читаем mapping существующего индекса
    with open(f"{dir_name_mappings}/{index_name}.json", "r+") as f:
        mappings = json.load(f)
        # проверяем, есть ли обновления mapping ДО put'a
        if RECOURSIVE_MAPPING_UPDATE:
            check_mappings_for_updates(mappings, index_name)

    # сохраняем новый mapping для индекса
    with open(f"{dir_name_new_mappings}/{new_index_name}.json", "w") as fw:
        json.dump(mappings, fw, **JSON_DUMP_PARAMS)

    # put'аем новые settings и mapping
    if REALLY:
        print(f"request PUT {new_index_url} settings")
        resp = requests.put(new_index_url, json={**settings, **mappings})
        if resp.status_code == 200:
            print(f"[WARN] {resp.text}")
        else:
            print(f"[WARN] {resp.text}")

    print("*" * 10)
    return new_index_name


def check_mappings_for_updates(mappings: dict, index_name: str) -> NoReturn:
    """
    Проверяет файл ./new_mapping.json, если там валидный маппинг для
    обновления - вызывает функцию рекурсивного обновления маппинга
    :param mappings: dict: загруженный маппинг индекса;
    :param index_name: str: название индекса
    """

    print(f"[INFO] check {index_name} mappings for update")
    if os.path.isfile("./new_mapping.json"):
        with open("./new_mapping.json") as f:
            mapping = json.load(f)
            check_fields(mapping, "properties", "new_mapping")
            fields_to_update = mapping.get("properties", dict())
            if not fields_to_update:
                print("[WARN] no mapping to update")
                return
            update_mapping(mappings, fields_to_update)


def update_mapping(mappings: dict, fields_to_update: dict) -> NoReturn:
    """
    Проверяет корректность загруженного и соххраненного маппинга индекса,
    если маппинг есть - вызывает функцию рекурсивного апдейта маппинга
    :param mappings: dict: загруженный маппинг индекса;
    :param fields_to_update: поля для обновления маппинга
    """

    top_level_properties = get_in(mappings, "mappings", "properties")
    if not top_level_properties:
        print("[WARN] existing mappings is empty")
        return
    recoursive_mappings_update(top_level_properties, fields_to_update)


def recoursive_mappings_update(dct: dict, dct_merge: dict) -> NoReturn:
    """
    Функция рекурсивного обновления маппинга
    :param dct: dict: маппинг для обновления;
    :param dct_merge: dict: новые поля в маппинг
    """

    p = "properties"
    for k in dct.keys():
        if k in dct_merge:
            if p in dct[k] and p in dct_merge[k]:
                recoursive_mappings_update(dct[k][p], dct_merge[k][p])
            elif p not in dct[k] and p not in dct_merge[k]:
                dct[k].update(dct_merge[k])
            else:
                raise Exception("mappings nested sync error")

    for k in dct_merge.keys():
        if k not in dct:
            dct[k] = dct_merge[k]


def get_body(index_name):
    with open("./new_mapping.json") as f:
        new_mapping = json.load(f)
    return new_mapping


def update_indexes_by_new_mappings(new_indexies):
    for index_name in new_indexies:
        print(f"[INFO] update {index_name}")
        new_mapping = get_body(index_name)
        print(f"[INFO] body {new_mapping}")
        new_index_url = f"{ES_URL}/{index_name}/_mapping/_doc"
        print(f"[INFO] PUT {new_index_url} {new_mapping}")
        if REALLY:
            resp = requests.put(new_index_url, json=new_mapping)
            if not resp.status_code == 200:
                raise Exception(resp.text)


def wait():
    a = input("Continue(yes/no):")
    a = True if a == "yes" else False

    while not a:
        time.sleep(10)
        a = input("Continue(yes/no):")
        if a == "yes":
            return


def reindex(old_index, new_index):
    print("[INFO] reindex", old_index, new_index)
    if REALLY:
        try:
            resp = requests.post(
                f"{ES_URL}/_reindex",
                json={"source": {"index": old_index}, "dest": {"index": new_index}, "conflicts": "proceed"},
                timeout=60,
            )
        except Exception as e:
            print(e)
            wait()
        else:
            if resp.status_code != 200:
                print(resp.text)
                wait()


def update():
    # забираем текущие индексы в эластике
    print("[INFO] download EXISTING indexes")
    index_names = download_indexes_names()
    print("[INFO] downloaded indexes:")
    print(*enumerate(index_names), sep="\n")
    save_indexes_meta(index_names)
    print("[INFO] existing indices meta saved")
    new_index_names = create_indexes(index_names)

    print("[INFO] new indexies", new_index_names)
    if not REALLY:
        return new_index_names
    _update(index_names, new_index_names)


def _update(index_names, new_index_names):
    print("[INFO] update NEW indexes")
    if not RECOURSIVE_MAPPING_UPDATE:
        update_indexes_by_new_mappings(new_index_names)

    for old_index, new_index in zip(index_names, new_index_names):
        reindex(old_index, new_index)

    print("[INFO] COPY THIS!")
    print(dir_name)

    with open(DIR_NAME_FILE, "w") as f:
        json.dump({"dir_name": dir_name}, f)

    print("[INFO] then step 1")
    realiace = list(zip(index_names, new_index_names))

    with open(REALIACE_FILE, "w") as f:
        json.dump({"realiace": realiace}, f)

    print(" \u001b[31m realiace inde§xes: \u001b[0m", realiace)
    print("[INFO] then step 2")

    with open(DELETE_FILE, "w") as f:
        json.dump({"index_names": index_names}, f)

    print("\u001b[31m delete indexes: \u001b[0m", index_names)


if __name__ == "__main__":
    update()
