FROM python:3.9-bullseye

ENV WORKDIR=/home/pharmbio
WORKDIR /home/pharmbio

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV py3=venv/bin/python3

RUN <<EOF
useradd -m pharmbio

mkdir -p /home/pharmbio/cellprofileroutput

apt update

# common dev dependencies
apt install -fy bash gcc g++ gfortran make cmake wget micro tree

# python dependencies
apt install -fy python3 virtualenv python3-pip python3-dev
apt install -fy libmariadb-dev
apt install -fy libopenblas-dev
apt install -fy libgtk-3-dev libgtk-3-0

# required by cellprofiler
apt install -fy openjdk-17-jdk-headless

# needs to be set manually for cellprofiler to find it (use the wildcard pattern to match whatever architecture, allows deploying the container e.g. on arm64 and amd64 without changes)
TOOLCHAIN_DIR=$(find /usr/lib/jvm -mindepth 1 -maxdepth 1 -name 'java-17-openjdk-*' -type d)
ln -s $TOOLCHAIN_DIR $JAVA_HOME

virtualenv venv

$py3 -m pip install celery matplotlib numpy==1.24 pandas pyarrow tqdm mariadb "SQLAlchemy<2.0" mysql-connector-python boto3
# numpy needs to already be installed before cellprofiler gets installed, it is mentioned a second time to fix the version
$py3 -m pip install numpy==1.24 mariadb "SQLAlchemy<2.0" "Cython<3.0" cellprofiler==4.2.6

EOF

COPY dbi_and_cell-profile/ .

RUN $py3 -m pip install $WORKDIR/dbi $WORKDIR/cell-profile

COPY --link cellprofilerinput /home/pharmbio/cellprofilerinput
COPY --link tasks.py tasks.py

# switch to non-root user
# USER pharmbio

CMD $py3 -m celery -A tasks worker --queues=map_queue --concurrency 1 --loglevel=WARNING
