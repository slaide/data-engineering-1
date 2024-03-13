FROM python:3.11-alpine3.18

RUN apk --no-cache add bash gcc g++ musl-dev linux-headers make
RUN apk --no-cache add openblas-dev
RUN apk --no-cache add mariadb-connector-c-dev mariadb-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install matplotlib
# required by python-javabridge (which is required by cellprofiler)
RUN python3 -m pip install numpy==1.24

# required by cellprofiler
RUN apk --no-cache add openjdk17-jdk
# needs to be set manually for cellprofiler to find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN python3 -m pip install cython==0.29.36
RUN python3 -m pip install centrosome==1.1.7
RUN python3 -m pip install cellprofiler==4.2.6

COPY cpworker_tasks.py cpworker_tasks.py
RUN python3 -m celery -A cpworker_tasks worker --loglevel=INFO
