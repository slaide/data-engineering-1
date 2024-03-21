from celery import Celery
import os

app = Celery('tasks', backend="rpc://", broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

@app.task(queue="map_queue")
def cp_map(file_list:[str])->[str]:
    return "_".join(file_list)+".csv"

@app.task(queue="reduce_queue")
def cp_reduce(image_list:[str])->[str]:
    return [f"{i}.res" for i in image_list]

