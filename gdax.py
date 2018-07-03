##################################
##
## Gdax worker
##
## Yi Bao    06/29/2018
##
##################################
import websockets, asyncio
import json
import pprint
import threading

import mylock
import lib_index

def gdax_build_request():
    return '{ \
            "type": "subscribe", \
            "product_ids": ["BTC-USD"], \
            "channels": [ \
                          "level2" \
                        ] \
           }'

async def ws_worker_gdax(ob, url):
    async with websockets.connect(url) as websocket:
        await websocket.send(gdax_build_request())
        async for m in websocket:
            j = json.loads(m)
            #print("gdax", len(ob.bids))
            with mylock.lock:
                # the first message is the snapshot of asks and bids
                if "asks" in j:
                    for order in j["bids"]:
                        order = list(map(float, order))
                        ob.bids[order[0]] = order[1]
                    for order in j["asks"]:
                        order = list(map(float, order))
                        ob.asks[order[0]] = order[1]
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
            if ob.bids and ob.asks:
                bids = [(p, q) for p, q in ob.bids.items()]
                asks = [(p, q) for p, q in ob.asks.items()]
                ob.crt = lib_index.crt(asks, bids)                
                
                ob.bid = max(ob.bids)
                ob.ask = min(ob.asks)
                ob.mid = (ob.bid + ob.ask) / 2.0
