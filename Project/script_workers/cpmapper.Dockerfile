FROM python:3.9-bullseye

RUN apt update
RUN apt install -fy bash gcc g++ gfortran make cmake wget micro tree
RUN apt install -fy libmariadb-dev
# used by some numerical libraries
RUN apt install -fy libopenblas-dev 
RUN apt install -fy python3 virtualenv python3-pip python3-dev
# required for wxpython
RUN apt install -fy libgtk-3-dev libgtk-3-0

# required by cellprofiler
RUN apt install -fy openjdk-17-jdk-headless
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# needs to be set manually for cellprofiler to find it (use the wildcard pattern to match whatever architecture, allows deploying the container e.g. on arm64 and amd64 without changes)
RUN TOOLCHAIN_DIR=$(find /usr/lib/jvm -mindepth 1 -maxdepth 1 -name 'java-17-openjdk-*' -type d) && \
    ln -s $TOOLCHAIN_DIR $JAVA_HOME

RUN useradd -m pharmbio

WORKDIR /home/pharmbio
RUN virtualenv venv
ENV py3=venv/bin/python3

RUN $py3 -m pip install celery matplotlib numpy
# numpy needs to already be installed before cellprofiler gets installed
RUN $py3 -m pip install mariadb SQLAlchemy==2.0.22 "Cython<3.0" cellprofiler==4.2.6
RUN $py3 -m pip install pandas pyarrow tqdm mariadb SQLAlchemy==2.0.22 mysql-connector-python boto3

COPY tasks.py tasks.py

COPY cellprofilerinput /home/pharmbio/cellprofilerinput
RUN mkdir -p /home/pharmbio/cellprofileroutput

#RUN mkdir -p /home/pharmbio/Downloads/test/inputfiles
#RUN mkdir -p /home/pharmbio/Downloads/test/outputfiles

# switch to non-root user
# USER pharmbio

CMD $py3 -m celery -A tasks worker --queues=map_queue --concurrency=1
# --loglevel=INFO
