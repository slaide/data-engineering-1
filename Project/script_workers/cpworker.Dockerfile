FROM python:3.11-alpine

RUN apk --no-cache add bash gcc musl-dev g++ gfortran linux-headers make
RUN apk --no-cache add openblas-dev
RUN apk --no-cache add mariadb-connector-c-dev mariadb-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install matplotlib
RUN python3 -m pip install numpy==1.24

RUN apk --no-cache add openjdk21-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk

RUN python3 -m pip install cellprofiler

COPY cpworker_tasks.py cpworker_tasks.py
RUN python3 -m celery -A cpworker_tasks worker --loglevel=INFO
