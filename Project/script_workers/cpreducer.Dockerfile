FROM python:3.12-alpine

RUN apk --no-cache add bash gcc musl-dev g++ gfortran linux-headers make curl cmake

# install the rust nightly toolchain, which is required by some polars dependency
ENV RUSTUP_HOME=/home/rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain nightly -y
RUN TOOLCHAIN_DIR=$(find $RUSTUP_HOME/toolchains -mindepth 1 -maxdepth 1 -type d) && \
    mkdir -p $RUSTUP_HOME/bin && \
    ln -s $TOOLCHAIN_DIR/bin/* $RUSTUP_HOME/bin/
ENV PATH=${PATH}:${RUSTUP_HOME}/bin
# ensure cargo is found and working
RUN cargo --help

RUN apk --no-cache add openblas-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install polars
RUN python3 -m pip install celery
RUN python3 -m pip install matplotlib
RUN python3 -m pip install numpy
RUN python3 -m pip install "pandas>=1.5.3,<1.6"
RUN python3 -m pip install scipy

COPY cpreducer_tasks.py cpreducer_tasks.py

CMD python3 -m celery -A cpreducer_tasks worker --loglevel=INFO
