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
import threading
import time, datetime
import copy

from gdax import *
from bitfinex import *

import mylock
import lib_index

''' order book class '''
class ob(object):
    def __init__(self):
        self.bids = dict()
        self.asks = dict()
        self.bid = [None, None]
        self.ask = [None, None]
        self.mid = None
        self.crt = None

''' exchange class '''
class exchange(object):
    def __init__(self, name, url, ws_workers):
        self.name = name
        self.url  = url
        self.ob   = ob()
        self.ws_worker = ws_workers[self.name]

    def start(self):
        self.thread = threading.Thread(target = start_event_loop, args = [self.ws_worker, self.ob, self.url])
        self.thread.start()

def init_ws_workers():
    ws_workers = dict()
    ws_workers["gdax"] = ws_worker_gdax
    ws_workers["bitfinex"] = ws_worker_bitfinex
    return ws_workers

''' generic start_event_loop '''
''' Each exchanges has specific requirements of websockets. Initialize each based on the name. '''
def start_event_loop(ws_worker, ob, url):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(ws_worker(ob, url))


''' Initialize a master thread '''
def init_master(exchanges, depth):
    thread = threading.Thread(target = master, args = [exchanges, depth])
    thread.start()

def master(exchanges, depth):
    print("Master is here.")
    con_ob = ob()
    elapsed = 0
    while elapsed < 1000:
        ## Consolidated order book
        mids = []
        crts = []
        for ex in exchanges:
            with mylock.lock:
                print(ex.name, len(ex.ob.bids), len(ex.ob.asks))
                for key in ex.ob.bids:
                    if key not in con_ob.bids:
                        con_ob.bids[key] = ex.ob.bids[key]
                    else:
                        con_ob.bids[key] += ex.ob.bids[key]
                        if con_ob.bids[key] == 0.0:
                            del con_ob.bids[key]
                for key in ex.ob.asks:
                    if key not in con_ob.asks:
                        con_ob.asks[key] = ex.ob.asks[key]
                    else:
                        con_ob.asks[key] += ex.ob.asks[key]
                        if con_ob.asks[key] == 0.0:
                            del con_ob.asks[key]
                mids.append(ex.ob.mid)
                crts.append(ex.ob.crt)

        ## Indexation
        con_bid = [(p, q) for p, q in con_ob.bids.items()]
        con_ask = [(p, q) for p, q in con_ob.asks.items()]
        con_bid.sort(key = lambda x: x[0], reverse = True)
        con_ask.sort(key = lambda x: x[0])
        try:
            index = lib_index.cryptoindex(con_ask, con_bid, depth)
            print(datetime.datetime.now(), index, mids, crts)
        except Exception as e:
            print(e)

        elapsed += 1
        time.sleep(1)

def main():
    ## Initialize ws_workers ##
    ws_workers = init_ws_workers()
    ## Initialize exchanges ##
    gdax = exchange("gdax", "wss://ws-feed.gdax.com", ws_workers)
    bitfinex = exchange("bitfinex", "wss://api.bitfinex.com/ws/2", ws_workers)
    exchanges = [gdax, bitfinex]
    #exchanges = [gdax]

    ## Start workers ##
    gdax.start()
    bitfinex.start()
    ## Start the master thread ##
    depth = 100
    init_master(exchanges, depth)


if __name__ == "__main__":
    main()
