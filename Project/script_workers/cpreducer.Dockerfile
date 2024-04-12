FROM python:3.12-bookworm

ENV WORKDIR=/home/pharmbio
WORKDIR /home/pharmbio

ENV py3=venv/bin/python3
ENV RUSTUP_HOME=/home/rust
ENV PATH=${PATH}:${RUSTUP_HOME}/bin

RUN <<EOF
useradd -m celeryworker

apt update

# common dev dependencies
apt install -fy bash gcc g++ gfortran make cmake wget micro tree

# python dependencies
apt install -fy python3 virtualenv python3-pip python3-dev
apt install -fy libmariadb-dev
apt install -fy libopenblas-dev

# install the rust nightly toolchain, which is required by some polars dependency
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain nightly -y
TOOLCHAIN_DIR=$(find $RUSTUP_HOME/toolchains -mindepth 1 -maxdepth 1 -type d)
mkdir -p $RUSTUP_HOME/bin 
ln -s $TOOLCHAIN_DIR/bin/* $RUSTUP_HOME/bin/

virtualenv venv

$py3 -m pip install --upgrade pip
$py3 -m pip install polars pandas pyarrow celery matplotlib numpy "pandas>=1.5.3,<1.6" scipy tqdm mariadb SQLAlchemy==2.0.22 mysql-connector-python boto3

EOF

COPY dbi_and_cell-profile/ .
RUN $py3 -m pip install ./dbi ./cell-profile

COPY --link tasks.py tasks.py

# switch to non-root user
# USER celeryworker

CMD $py3 -m celery -A tasks worker --queues reduce_queue --concurrency 1 --loglevel=WARNING
