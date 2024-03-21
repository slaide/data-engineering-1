FROM python:3.12-bookworm

RUN apt update
RUN apt install -fy bash gcc g++ gfortran make cmake wget
RUN apt install -fy libmariadb-dev
RUN apt install -fy python3 virtualenv python3-pip python3-dev
# used by some numerical libraries
RUN apt install -fy libopenblas-dev

WORKDIR /app

RUN virtualenv venv
ENV py3=venv/bin/python3

RUN $py3 -m pip install --upgrade pip
RUN $py3 -m pip install celery tqdm pandas numpy flask==3.0.2 werkzeug \
                        mariadb SQLAlchemy==2.0.22 mysql-connector-python

COPY . /app/

ENV PYTHONUNBUFFERED=1

# switch to non-root user
RUN useradd -m webfrontend
#USER webfrontend

CMD $py3 main.py
