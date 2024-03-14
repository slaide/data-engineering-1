import sys, time
from celery import Celery
import os

app = Celery('dbwatcher', backend='rpc://', broker=os.getenv('APP_BROKER_URI'), broker_pool_limit = 0)

from tasks import cp_reduce,cp_map

def checkTasksRegistered(tasks:[str]=None):
    registered_tasks=None
    try:
        inspector=app.control.inspect()
        registered_tasks=inspector.registered()
    except:
        pass
    finally:
        if registered_tasks is None:
            return False

    if tasks is None:
        return True

    task_names=set(tasks)

    registered_task_names=[]
    for tl in registered_tasks.values():
        registered_task_names.extend(tl)
    registered_task_names=set(registered_task_names)
    
    return registered_task_names.issuperset(task_names)

def main():
    while not checkTasksRegistered([cp_map.name,cp_reduce.name]):
        print("not ready. sleeping...")
        time.sleep(5)

    a=cp_map(2,3)
    b=cp_reduce(3,4)

    print(f"{a = }")
    print(f"{b = }")

    a_result=cp_map.delay(2,3)
    b_result=cp_reduce.delay(3,4)
    print(f"{a_result.get() = }")
    print(f"{b_result.get() = }")

if __name__=="__main__":
    main()
