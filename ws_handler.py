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
import sys
import json
import pprint
from threading import Thread

''' order book class '''
class ob(object):
    def __init__(self):
        ob.bids = dict()
        ob.asks = dict()

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


''' bitfinex '''
def bitfinex_build_request():
   return '{ \
            "event": "subscribe", \
            "channel": "book", \
            "symbol": "tBTCUSD", \
            "prec": "P0" \
           }'

def bitfinex_start_event_loop(ob, url):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(ws_worker_bitfinex(ob, url))

async def ws_worker_bitfinex(ob, url):
   async with websockets.connect(url) as websocket:
       await websocket.send(bitfinex_build_request())
       async for m in websocket:
           j = json.loads(m)
           print("bitfinex", len(ob.bids))
           # skip the first, second messages and heartbeats
           if "event" in j or "chanId" in j or "hb" in j:
               continue
           # the third message is supposed to be the snapshot
           elif len(j[1]) > 3:
               for order in j[1]:
               # each order is [PRICE, COUNT, AMOUNT]
                   price = order[0]
                   count = order[1]
                   amount = order[2]
                   if amount > 0:
                       ob.bids[price] = amount
                   else:
                       ob.asks[price] = abs(amount)
           # otherwise it is update
           else:
               # each update is [CHANNEL_ID, [PRICE, COUNT, AMOUNT]]
               price = j[1][0]
               count = j[1][1]
               amount = j[1][2]
               if count > 0:
                   if amount > 0:
                       ob.bids[price] = amount
                   else:
                       ob.asks[price] = abs(amount)
               elif count == 0:
                   if amount == 1:
                       try:
                           del ob.bids[price]
                       except:
                           print("Bitfinex exception:", price, "not in bids")
                   elif amount == -1:
                       try:
                           del ob.asks[price]
                       except:
                           print("Bitfinex exception:", price, "not in asks")
           #print("Bitfinex", len(ob.bids), len(ob.asks))

''' gdax '''
def gdax_build_request():
    return '{ \
            "type": "subscribe", \
            "product_ids": ["BTC-USD"], \
            "channels": [ \
                          "level2" \
                        ] \
           }'

def gdax_start_event_loop(ob, url):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(ws_worker_gdax(ob, url))


async def ws_worker_gdax(ob, url):
    async with websockets.connect(url) as websocket:
        await websocket.send(gdax_build_request())
        async for m in websocket:
            j = json.loads(m)
            print("gdax", len(ob.bids))
            # the first message is the snapshot of asks and bids
            if "asks" in j:
                for order in j["bids"]:
                    ob.bids[float(order[0])] = float(order[1])
                for order in j["asks"]:
                    ob.asks[float(order[0])] = float(order[1])
            # the second message is channels information
            elif "channels" in j:
                continue
            # all the rest messages are l2update
            else:
                j["side"]  = j["changes"][0][0]
                j["price"] = float(j["changes"][0][1])
                j["size"]  = float(j["changes"][0][2])
                # update ob
                if j["side"] == "buy":
                    if j["size"] == 0:
                        del ob.bids[j["price"]]
                    else:
                        ob.bids[j["price"]] = j["size"]
                else:
                    if j["size"] == 0:
                        del ob.asks[j["price"]]
                    else:
                        ob.asks[j["price"]] = j["size"]
            #print("Gdax", len(ob.bids), len(ob.asks))


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
