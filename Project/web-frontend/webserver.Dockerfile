FROM python:3.12-bookworm

ENV WORKDIR=/app
WORKDIR /app

ENV py3=venv/bin/python3
# without this, the web server will buffer the output and not show it in real-time
ENV PYTHONUNBUFFERED=1

RUN <<EOF
useradd -m webfrontend

apt update

# common dev dependencies
apt install -fy bash gcc g++ gfortran make cmake wget micro tree

# python dependencies
apt install -fy python3 virtualenv python3-pip python3-dev
apt install -fy libmariadb-dev
apt install -fy libopenblas-dev

# setup python virtualenv in WORKDIR
virtualenv venv

# install python packages
$py3 -m pip install --upgrade pip
$py3 -m pip install celery tqdm pandas pyarrow numpy flask==3.0.2 werkzeug mariadb SQLAlchemy==2.0.22 mysql-connector-python boto3==1.34.72 boto3-stubs==1.34.72
EOF

COPY dbi_and_cell-profile/ ./

RUN $py3 -m pip install ./dbi ./cell-profile

COPY --link main.py main.py
COPY --link static static

# switch to non-root user
USER webfrontend

CMD $py3 main.py
