import sys
from cpworker_tasks import runcp
from cpreducer_tasks import cpred

def main():
    runcp(2,3)
    cpred(3,4)

if __name__=="__main__":
    main()

