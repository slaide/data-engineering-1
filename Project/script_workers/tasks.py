from celery import Celery
import os

app = Celery('tasks', backend="rpc://", broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

@app.task(queue="reduce_queue")
def cp_reduce(x, y):
    return x + y

@app.task(queue="map_queue")
def cp_map(x, y):
    return x + y
