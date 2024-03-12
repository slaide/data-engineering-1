FROM python:3.12-alpine
RUN <<EOF
#!/usr/bin/env sh
pkg 
python3 -m pip install --upgrade pip setuptools
python3 -m pip install celery

apk --no-cache add mariadb-connector-c-dev gcc musl-dev

python3 -m pip install mariadb
EOF
RUN python3 -m celery -A tasks worker --loglevel=INFO
