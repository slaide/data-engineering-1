# project outline

this is my submission for the final project of this course.

The topic will be an alternative take on the infrastructure we are using for the research in our group. images will be stored in an object storage solution, references to which will be stored in an sql database. a script will query the database frequently to look for new submissions, create tasks based on those new entries and submit those tasks to a work queue.
a worker will fetch images from the object storage based on the task description and then run the cellprofiler command line program. the result files from cellprofiler are stored in the object storage, and references to the file are submitted to the database.
the database query script will also look for cellprofiler result files, and when all images from an experiment are done processing by cellprofiler, a new task will be created (again consumed by a worker) that reduces the tabular data from all relevant result files into something usable by an end-user, like a plot.

# selected software solutions

below are the software solutions I have chosen for this project.

## object storage:

the object storage solution chosen for this project is _localstack_, which offers local object storage. localstack offers local versions of all aws components, and official docker [images](https://hub.docker.com/r/localstack/localstack) are available as well. there are also images containing only specific aws components, like s3, which is what we are using here (hence we are using the s3-localstack docker image). the documentation for localstack s3 is [here](https://docs.localstack.cloud/user-guide/aws/s3/).

## sql database:

the sql database used here is _mariadb_. there is an official docker image which I will be using here.

the documentation for mariadb+docker is [here](https://hub.docker.com/_/mariadb).

## task queue

the task queue is managed by _rabbitmq_. it offers an api for many implementation languages, python being one, which I will be using. there is also an official docker image [here](https://hub.docker.com/_/rabbitmq).

## db query script

I will be using _python_ as a scripting language. There are also official docker [images](https://hub.docker.com/_/python) available to run python inside a well-defined environment, for which i will chose alpine linux and python 3.12.

## workers

the _cellprofiler_ workers will be running via python as well. _celery_ is a python framework built on top of rabbitmq, so I will that on top of python. The celery docs are [here](https://docs.celeryq.dev/en/stable/).

## reducers

the service to _reduce_ the cellprofiler outputs into plots (or whatever) will also be written in python. this service will consume tasks from the rabbitmq task queue, so it will also use celery.
