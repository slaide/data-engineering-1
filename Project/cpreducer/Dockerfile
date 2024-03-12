FROM python:3.12-alpine
RUN <<EOF
#!/usr/bin/env sh
python3 -m pip install --upgrade pip setuptools
python3 -m pip install celery

apk --no-cache add rust cargo musl-dev

python3 -m pip install polars scipy pandas numpy
EOF
RUN python3 -m celery -A tasks worker --loglevel=INFO
