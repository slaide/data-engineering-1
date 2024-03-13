from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@localhost//')

@app.task
def runcp(x, y):
    return x + y
