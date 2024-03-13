FROM python:3.11-alpine

RUN apk --no-cache add curl gcc musl-dev g++ gfortran
RUN apk --no-cache add openblas-dev

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery
RUN python3 -m pip install numpy==1.24
RUN <<EOF
# download prebuilt wxpython wheel to avoid local compilation which takes 30 minutes
curl -o wxPython-4.2.1-cp311-cp311-linux_x86_64.whl https://extras.wxpython.org/wxPython4/extras/linux/gtk3/rocky-9/wxPython-4.2.1-cp311-cp311-linux_x86_64.whl
python3 -m pip install wxPython-4.2.1-cp311-cp311-linux_x86_64.whl
EOF
RUN python3 -m pip install cellprofiler

COPY cpworker_tasks.py cpworker_tasks.py
RUN python3 -m celery -A cpworker_tasks worker --loglevel=INFO
