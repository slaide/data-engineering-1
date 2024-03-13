FROM python:3.12-alpine

RUN apk --no-cache add rust cargo
RUN apk --no-cache add gcc musl-dev g++ gfortran
RUN apk --no-cache add openblas-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install matplotlib polars scipy pandas numpy

COPY cpreducer_tasks.py cpreducer_tasks.py
RUN python3 -m celery -A cpreducer_tasks worker --loglevel=INFO
