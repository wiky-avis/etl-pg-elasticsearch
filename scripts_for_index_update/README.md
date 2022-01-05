1) Активируем виртуальное окружение.
2) Заполняем изменения, которые хотим внести в индекс (файлы new_mapping.json, new_settings.json должны находятся в scripts/scripts_for_index_update). Там есть примеры (_example) содержимого этих файлов.
3) Заполняем `config.py`
    ```
    ES_URL = "http://0.0.0.0:9200"  # адрес эластика индексы которого будем менять
    REALLY = False # применятся ли изменения, если False, то изменения не будут применены, 
                   #  но будут сформированы данные, которые будут использованы для изменения 
                   #  индекса, файлики будут лежать в директории update_data
   
   # Если INCLUDE_INDICES, то используется она.
   # Иначе используется EXCLUDE_INDICES.
   INCLUDE_INDICES = []  # список индексов, которые будут изменяться. 
   EXCLUDE_INDICES = [
        ".elastichq",
        ...
    ]  # список индексов которые не будут изменяться
    ```
4) Если собираемся запускать команды через терминал, а не через pycharm выполняем команду
    ```
    export PYTHONPATH="${PYTHONPATH}:$(pwd)"
    ```
    и переходим в нужный каталог
    ```
    cd scripts/scripts_for_index_update
    ```
5) Запускаем `update.py` (ВАЖНО! Сначала лучше запустить с `REALLY = False` и проверить что все ок)
    ```
    python -m update
    ```
6) Проверяем в директории `./update_data/<timestamp>` что в `new_mappings` есть новые корректные mapping'и для всех нужных индексов 
7) Меняем значение `REALLY` на `True` и запускаем `update.py`
    ```
    python -m update
    ```
    Выполнение команды может отвалиться по таймауту, но это не страшно нужно следить за ходом выполнения и вводить `yes`, когда будет предложено продолжить. К следующему шагу можно преступать когда созданы все новые индексы и все документы из старого индекса переиндексированы, это можно проверить так:
    ```
    GET /_cat/indices?v
    Host: localhost:9200
    ```
    Вернет:
    ```
    health status index                     uuid                   pri rep docs.count docs.deleted store.size pri.store.size
    yellow open   movies_v16    2G0FtoprTmag7fYVESkraw   5   1     302656         9407      161mb          161mb
    yellow open   movies_v17    6JH50HskS7C3FcJuDR9LoQ   5   1     302656            0    157.1mb        157.1mb
    ```
8) Проверяем схему и настройки новых индексов
    ```
    GET /movies_v17
    Host 0.0.0.0:9200
    ```
9) Запускаем `realiace.py`, который переназначает алиасы (`./update_data/<timestamp>/realiace.json`) на новые только что созданные индексы.
    ```
    python -m realiace
    ```
10) Проверяем что все работает
11) Включаем запись в эластик
12) Через како-то время можно удалить старые индексы (`./update_data/<timestamp>/delete.json`), выполнив скрипт `delete.py`
    ```
    python -m delete
    ```
