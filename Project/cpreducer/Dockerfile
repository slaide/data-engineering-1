FROM python:3.12-alpine
RUN <<EOF
#!/usr/bin/env bash
python3 -m pip install --upgrade pip setuptools
python3 -m pip install celery

python3 -m pip install polars scipy pandas numpy
EOF
RUN celery -A tasks worker --loglevel=INFO
