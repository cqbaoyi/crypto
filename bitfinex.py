###################################
##
## Bitfinex worker
##
## Yi Bao    06/29/2018
##
###################################
import websockets, asyncio
import json
import pprint
import threading
import pandas as pd

import mylock
import lib_index

def bitfinex_build_request():
   return '{ \
            "event": "subscribe", \
            "channel": "book", \
            "symbol": "tBTCUSD", \
            "prec": "P0" \
           }'

async def ws_worker_bitfinex(ob, url, span):
    async with websockets.connect(url) as websocket:
        await websocket.send(bitfinex_build_request())
        async for m in websocket:
            j = json.loads(m)
            with mylock.lock:
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
            if ob.bids and ob.asks:
                bids = [(p, q) for p, q in ob.bids.items()]
                asks = [(p, q) for p, q in ob.asks.items()]
                ob.crt.append(lib_index.crt(asks, bids))
                if len(ob.crt) > span:
                    ob.crt.pop(0)
                try:
                    ob.expcrt = pd.Series(ob.crt).ewm(span = span).mean().iloc[-1] 
                except:
                    print("bitfinex expcrt error")

                ob.bid = max(ob.bids)
                ob.ask = min(ob.asks)
                ob.mid = (ob.bid + ob.ask) / 2.0
