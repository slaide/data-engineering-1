FROM python:3.9-bookworm

RUN apt update
RUN apt upgrade -fy
RUN apt install -fy bash gcc g++ make cmake
RUN apt install -fy libopenblas-dev libmariadb-dev
RUN apt install -fy python3 python3-pip virtualenv python3-dev

RUN virtualenv venv
ENV py3=venv/bin/python3

# required by cellprofiler
RUN apt install -fy openjdk-17-jdk

RUN $py3 -m pip install --upgrade pip setuptools
RUN $py3 -m pip install celery matplotlib

# numpy is required by python-javabridge (which is required by cellprofiler)
RUN $py3 -m pip install numpy
# mariadb version specified by dependency on mariadb-c-connector which is a system package
RUN $py3 -m pip install mariadb

# needs to be set manually for cellprofiler to find it (use the wildcard pattern to match whatever architecture, allows deploying the container e.g. on arm64 and amd64 without changes)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
RUN TOOLCHAIN_DIR=$(find /usr/lib/jvm -mindepth 1 -maxdepth 1 -name 'java-17-openjdk-*' -type d) && \
    ln -s $TOOLCHAIN_DIR $JAVA_HOME

RUN $py3 -m pip install "Cython<3"
RUN $py3 -m pip install cellprofiler==4.2.6

COPY cpworker_tasks.py cpworker_tasks.py

CMD $py3 -m celery -A cpworker_tasks worker --loglevel=INFO
