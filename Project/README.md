# project outline

this is my submission for the final project of this course.

The topic will be an alternative take on the infrastructure we are using for the research in our group. images will be stored in an object storage solution, references to which will be stored in an sql database. a script will query the database frequently to look for new submissions, create tasks based on those new entries and submit those tasks to a work queue.
a worker will fetch images from the object storage based on the task description and then run the cellprofiler command line program. the result files from cellprofiler are stored in the object storage, and references to the file are submitted to the database.
the database query script will also look for cellprofiler result files, and when all images from an experiment are done processing by cellprofiler, a new task will be created (again consumed by a worker) that reduces the tabular data from all relevant result files into something usable by an end-user, like a plot.

# selected software solutions

below are the software solutions I have chosen for this project.

## container and orchestration

I am using docker for containerization, and docker swarm for orchestration. dockerfile docs are [here](https://docs.docker.com/reference/dockerfile/). docker-compose documentation is [here](https://docs.docker.com/compose/compose-file/). docker swarm docs are [here](https://docs.docker.com/engine/swarm/).

## object storage:

the object storage solution chosen for this project is _localstack_, which offers local object storage. localstack offers local versions of all aws components, and official docker [images](https://hub.docker.com/r/localstack/localstack) are available as well. there are also images containing only specific aws components, like s3, which is what we are using here (hence we are using the s3-localstack docker image). the documentation for localstack s3 is [here](https://docs.localstack.cloud/user-guide/aws/s3/).

## sql database:

the sql database used here is _mariadb_. there is an official docker image which I will be using here.

the documentation for mariadb+docker is [here](https://hub.docker.com/_/mariadb).

## task queue

the task queue is managed by _rabbitmq_. it offers an api for many implementation languages, python being one, which I will be using. there is also an official docker image [here](https://hub.docker.com/_/rabbitmq).

## scripts

For any custom scripting, I will be using _python_. _celery_ is a python framework that allows RPC (Remote Procedure Calling) via rabbitmq. I will combine this with pure python and pythons ability to launch shell commands to run cellprofiler and the reducer task through an opaque scalable RPC service. There is an official docker [image](https://hub.docker.com/_/python) available to run python inside a well-defined environment, for which i will chose alpine linux and python 3.12. celery will be installed as a pypi package inside this container.

### database watcher

The database will be queried for new images stored in the object storage, that have not been processed yet (i.e. do not have an annotation in the database regarding this). When a full set of images (all relevant imaging channels) of an imaging site have been submitted, a celery task will run a cellprofiler pipeline on these images. the results will then be written back to the object storage and database. after this task is done, the watcher will check if all image sets of the same plate have been processed, and if so, will (also through celery) reduce the concatenated tabular results from across the plate into a single plot (also stored in object storage+database), which can be queried by an end-user at any point. docs for python+mariadb are [here](https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/).

### workers

the _cellprofiler_ workers will expose a celery task that takes a number of inputs:
1. a set of images, one per imaging channel (of the same imaging site on the same plate)
1. a cellprofiler pipeline to apply to these images
and one output:
1. the references to the result files

cellprofiler batch mode (i.e. 'run pre-configured pipeline headless') docs are [here](https://cellprofiler-manual.s3.amazonaws.com/CellProfiler-4.0.5/help/other_batch.html).

### reducers

the service to _reduce_ the cellprofiler outputs into plots (or whatever) will also be written in python and exposed as a celery task. the input is the identifier of the plate, which is then used to query all result files from the object storage. these files will be concatenated and processed into a result plot (using python libraries). a reference to this plot is then returned through celery as result of this task.
