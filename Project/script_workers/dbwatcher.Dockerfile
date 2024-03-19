FROM python:3.12-bookworm

RUN apt update
RUN apt install -fy bash gcc g++ gfortran make cmake wget
RUN apt install -fy libmariadb-dev
# used by some numerical libraries
RUN apt install -fy libopenblas-dev 
RUN apt install -fy python3 virtualenv python3-pip python3-dev

RUN virtualenv venv
ENV py3=venv/bin/python3

RUN $py3 -m pip install --upgrade pip
RUN $py3 -m pip install celery tqdm mariadb

COPY tasks.py tasks.py
COPY dbwatcher.py dbwatcher.py

ENV PYTHONUNBUFFERED=1

# switch to non-root user
RUN useradd -m dbwatcher
USER dbwatcher

CMD $py3 dbwatcher.py
