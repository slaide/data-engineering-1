FROM python:3.9-bullseye

RUN apt update
RUN apt upgrade -fy
RUN apt install -fy bash gcc g++ gfortran make cmake wget
RUN apt install -fy libmariadb-dev
# used by some numerical libraries
RUN apt install -fy libopenblas-dev 
RUN apt install -fy python3 virtualenv python3-pip python3-dev
# required for wxpython
RUN apt install -fy libgtk-3-dev libgtk-3-0

RUN virtualenv venv
ENV py3=venv/bin/python3

# required by cellprofiler
RUN apt install -fy openjdk-17-jdk-headless

# needs to be set manually for cellprofiler to find it (use the wildcard pattern to match whatever architecture, allows deploying the container e.g. on arm64 and amd64 without changes)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
RUN TOOLCHAIN_DIR=$(find /usr/lib/jvm -mindepth 1 -maxdepth 1 -name 'java-17-openjdk-*' -type d) && \
    ln -s $TOOLCHAIN_DIR $JAVA_HOME

RUN $py3 -m pip install celery matplotlib numpy
RUN $py3 -m pip install mariadb "Cython<3.0" cellprofiler==4.2.6

COPY tasks.py tasks.py

CMD $py3 -m celery -A tasks worker --queues=map_queue
# --loglevel=INFO
