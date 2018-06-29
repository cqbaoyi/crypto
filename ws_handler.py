##########################################################################
##
## This script aims to provide a general interface of websockets' workers.
## Each worker caches a local copy of level2 ob.
## Exchanges: gdax, bitfinex
##
## Yi Bao    06/26/2018
##
##########################################################################
import websockets, asyncio
import json
import pprint
from threading import Thread

from gdax import *
from bitfinex import *

''' order book class '''
class ob(object):
    def __init__(self):
        self.bids = dict()
        self.asks = dict()

''' exchange class '''
class exchange(object):
    def __init__(self, name, url, ws_workers):
        self.name = name
        self.url  = url
        self.ob   = ob()
        self.ws_worker = ws_workers[self.name]

    def start(self):
        self.thread = Thread(target = start_event_loop, args = [self.ws_worker, self.ob, self.url])
        self.thread.start()

def initialize_ws_workers():
    ws_workers = dict()
    ws_workers["gdax"] = ws_worker_gdax
    ws_workers["bitfinex"] = ws_worker_bitfinex
    return ws_workers

''' generic start_event_loop '''
''' Each exchanges has specific requirements of websockets. Initialize each based on the name. '''
def start_event_loop(ws_worker, ob, url):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(ws_worker(ob, url))


def main():
    ## Initialize ws_workers ##
    ws_workers = initialize_ws_workers()
    ## Initialize exchanges ##
    gdax = exchange("gdax", "wss://ws-feed.gdax.com", ws_workers)
    bitfinex = exchange("bitfinex", "wss://api.bitfinex.com/ws/2", ws_workers)
    ## Start threads ##
    gdax.start()
    bitfinex.start()


if __name__ == "__main__":
    main()
