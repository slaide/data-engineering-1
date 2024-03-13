FROM python:3.9-alpine

RUN apk --no-cache add bash gcc g++ musl-dev linux-headers make
RUN apk --no-cache add openblas-dev
RUN apk --no-cache add mariadb-connector-c-dev mariadb-dev

# required by cellprofiler
RUN apk --no-cache add openjdk17-jdk

RUN python3 -m pip install --upgrade pip setuptools
RUN python3 -m pip install celery matplotlib

# numpy is required by python-javabridge (which is required by cellprofiler)
RUN python3 -m pip install numpy
# mariadb version specified by dependency on mariadb-c-connector which is a system package
RUN python3 -m pip install mariadb

# needs to be set manually for cellprofiler to find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
#ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}:/home/ubuntu/.local/bin

RUN python3 -m pip install python-javabridge

RUN python3 -m pip install cellprofiler==4.2.6

COPY cpworker_tasks.py cpworker_tasks.py

CMD python3 -m celery -A cpworker_tasks worker --loglevel=INFO
