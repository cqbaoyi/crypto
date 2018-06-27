#######################################
##
## Use exchange websockets
## Multithreading
## Indexation every 1s
##
## Yi Bao    06/26/2018
##
#######################################
import time
import datetime
from threading import Thread
from ws_gdax_ob import *

def _assign_workers(exchange_names):
    for exchange in exchange_names:
        thread = Thread(target = ws_worker)


def main():
    exchange_names = ["gdax"]

if __name__ == "__main__":
    main()
