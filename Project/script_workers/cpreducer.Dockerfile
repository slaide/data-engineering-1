FROM python:3.12-bookworm

RUN apt update
RUN apt upgrade -fy
RUN apt install -fy bash gcc g++ gfortran make cmake wget
RUN apt install -fy libmariadb-dev
# used by some numerical libraries
RUN apt install -fy libopenblas-dev 
RUN apt install -fy python3 virtualenv python3-pip python3-dev

# install the rust nightly toolchain, which is required by some polars dependency
ENV RUSTUP_HOME=/home/rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain nightly -y
RUN TOOLCHAIN_DIR=$(find $RUSTUP_HOME/toolchains -mindepth 1 -maxdepth 1 -type d) && \
    mkdir -p $RUSTUP_HOME/bin && \
    ln -s $TOOLCHAIN_DIR/bin/* $RUSTUP_HOME/bin/
ENV PATH=${PATH}:${RUSTUP_HOME}/bin
# ensure cargo is found and working
RUN cargo --help

RUN virtualenv venv
ENV py3=venv/bin/python3

RUN $py3 -m pip install --upgrade pip
RUN $py3 -m pip install polars celery matplotlib numpy "pandas>=1.5.3,<1.6" scipy

COPY tasks.py tasks.py

# switch to non-root user
RUN useradd -m celeryworker
USER celeryworker

CMD $py3 -m celery -A tasks worker --queues=reduce_queue
# --loglevel=INFO
