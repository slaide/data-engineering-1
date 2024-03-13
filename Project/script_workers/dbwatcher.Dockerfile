FROM python:3.12-alpine

RUN apk --no-cache add mariadb-connector-c-dev gcc musl-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install mariadb

COPY dbwatcher_tasks.py dbwatcher_tasks.py
COPY cpreducer_tasks.py cpreducer_tasks.py
COPY cpworker_tasks.py cpworker_tasks.py
RUN python3 dbwatcher_tasks.py
