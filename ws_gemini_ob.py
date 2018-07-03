############################################
##
## Estalishes a websocket of gemini
## Subscribe level2 ob
## Cache a real-time local ob
##
## Yi Bao    07/02/2018
##
############################################
import websockets, asyncio
import json
import pprint

''' orderbook class'''
class ob(object):
    def __init__(self):
        self.bids = dict()
        self.asks = dict()


async def start_gdax_websocket(ob, url):
    async with websockets.connect(url) as websocket:
        await websocket.send(build_request())
        async for m in websocket:
            j = json.loads(m)
            pprint.pprint(j)


def build_request():
    return '{}'

def main():
    gemini_ob = ob()
    product = "btcusd"
    url = "wss://api.gemini.com/v1/marketdata/" + product + "?heartbeat=false&&top_of_book=false&&bids=true&&offers=true&&trades=true&&auctions=true"
    asyncio.get_event_loop().run_until_complete(start_gdax_websocket(gemini_ob, url))
   
if __name__=="__main__":
    main()
