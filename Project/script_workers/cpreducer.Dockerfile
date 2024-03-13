FROM python:3.12-alpine

RUN apk --no-cache add bash gcc musl-dev g++ gfortran linux-headers make
RUN apk --no-cache add rust cargo
RUN apk --no-cache add openblas-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install matplotlib
RUN python3 -m pip install numpy
RUN python3 -m pip install "pandas>=1.5.3,<1.6"
RUN python3 -m pip install "polars>=0.19.9,<0.20.0"
RUN python3 -m pip install scipy

COPY cpreducer_tasks.py cpreducer_tasks.py
RUN python3 -m celery -A cpreducer_tasks worker --loglevel=INFO
