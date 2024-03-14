FROM python:3.12-bookworm

RUN apt update
RUN apt upgrade -fy
RUN apt install -fy bash gcc g++ make cmake gfortran curl
RUN apt install -fy libopenblas-dev libmariadb-dev
RUN apt install -fy python3 python3-pip virtualenv

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

RUN $py3 -m pip install --upgrade pip setuptools
RUN $py3 -m pip install polars
RUN $py3 -m pip install celery
RUN $py3 -m pip install matplotlib
RUN $py3 -m pip install numpy
RUN $py3 -m pip install "pandas>=1.5.3,<1.6"
RUN $py3 -m pip install scipy

COPY cpreducer_tasks.py cpreducer_tasks.py

CMD $py3 -m celery -A cpreducer_tasks worker --loglevel=INFO
