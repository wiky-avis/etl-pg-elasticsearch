# etl-pg-elasticsearch

ETL-процесс для перекачки данных из Poestgresql в Elasticsearch

## Установка

Склонируйте репозиторий на локальную машину:

    `git clone https://github.com/wiky-avis/etl-pg-elasticsearch.git`

Создайте виртуальное окружение:

    `python -m venv venv`

и активируйте его (команда зависит от ОС):

    `source venv/bin/activate`

Ус тановите необходимые зависимости:

    `pip install -r requirements.txt`

Запустите docker-compose:

    `make up_local_compose`

## Запуск скрипта

    `make run_script`

После запуска скрипт автоматически создаст индекс movies в Elasticsearch и загрузит в него данные.

