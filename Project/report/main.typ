= project outline

== general idea

in our group, we have a large number of images of cells, treated with different compounds. the compounds effect the cell morphology (e.g. shape and size) in different ways. we want to know how different compounds affect the cells, to discover drugs that treat any number of diseases, e.g. cancer or MS. if we can find a drug that makes a diseased cell look like a healthy cell, it can be considered cured (or rather, a candidate for a drug to treat a human with).

== problem

we have a LOT of images. we have terabytes of images. the computational pipeline to retrieve the morphology data from these images is resource intensive. the software we use there is called cellprofiler, and it takes a lot of time to run, and crashes regularly.

== pitch

we can distribute the workload.

== solution

user #link("https://docs.docker.com/engine/swarm/")[docker swarm] to orchestrate and scale the following architecture:

1. store images in dedicated storage device (e.g. object storage, or some other central storage server). #link("https://docs.localstack.cloud/user-guide/aws/s3/")[localstack] seems like a good solution.
2. images are recorded in a database (some sql database, e.g. #link("https://mariadb.org/")[mariadb]), with submission data and other metadata (e.g. to be able to tell which images belong together, like same site in same well on same plate)
3. a worker queries the database regularly, checks for new (complete!) submission of images, and creates a new workload to be processed by a cellprofiler pipeline for each set of images. this can be just a python script.
4. a message queue (like #link("https://rabbitmq-website.pages.dev/")[RabbitMQ]) accepts workload messages and distributes them to workers
5. any number of worker nodes (docker containers) accept messages and run #link("https://cellprofiler.org/")[cellprofiler] in the workload. they fetch images from the database, calculate the data, write the result back to the storage server and submit an entry to the database.

salman mentioned #link("https://docs.celeryq.dev/en/stable/")[celery] to scale the worker nodes. looks like a rabbitmq python wrapper (to make the existing rabbitmq python api nicer to use).

== mini presentation

attended the session 14.15-15.00
