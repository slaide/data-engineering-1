FROM python:3.11-alpine
RUN <<EOF
#!/usr/bin/env sh
python3 -m pip install --upgrade pip setuptools
python3 -m pip install celery

apk --no-cache add curl

pip3 install numpy==1.24
# download prebuilt wxpython wheel to avoid local compilation which takes 30 minutes
curl -o wxpython.whl https://extras.wxpython.org/wxPython4/extras/linux/gtk3/rocky-9/wxPython-4.2.1-cp311-cp311-linux_x86_64.whl
pip3 install wxpython.whl
pip3 install cellprofiler==4.2.6
EOF
RUN python3 -m celery -A tasks worker --loglevel=INFO
