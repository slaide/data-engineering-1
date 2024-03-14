FROM python:3.12-bookworm

RUN apt update
RUN apt upgrade -fy
RUN apt install -fy bash gcc g++ make cmake
RUN apt install -fy libopenblas-dev libmariadb-dev
RUN apt install -fy python3 python3-pip virtualenv

RUN virtualenv venv
ENV py3=venv/bin/python3

RUN $py3 -m pip install --upgrade pip setuptools
RUN $py3 -m pip install celery tqdm
RUN $py3 -m pip install mariadb

COPY dbwatcher_tasks.py dbwatcher_tasks.py
COPY cpreducer_tasks.py cpreducer_tasks.py
COPY cpworker_tasks.py cpworker_tasks.py

CMD $py3 dbwatcher_tasks.py
