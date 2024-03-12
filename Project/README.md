this is my submission for the final project of this course

the project is done by myself, on a topic I have chosen myself.

The topic will be an alternative take on the infrastructure we are using for the research in our group. images will be stored in an object storage solution, references to which will be stored in an sql database. a script will query the database frequently to look for new submissions, create tasks based on those new entries and submit those tasks to a work queue.
a worker will fetch images from the object storage based on the task description and then run the cellprofiler command line program. the result files from cellprofiler are stored in the object storage, and references to the file are submitted to the database.
the database query script will also look for cellprofiler result files, and when all images from an experiment are done processing by cellprofiler, a new task will be created (again consumed by a worker) that reduces the tabular data from all relevant result files into something usable by an end-user, like a plot.
