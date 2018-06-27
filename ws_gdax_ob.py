############################################
##
## Estalishes a websocket of gdax
## Subscribe level2 ob
## Cache a real-time local ob
## Write data into a csv
##
## Yi Bao    06/25/2018
##
############################################
import websockets, asyncio
import json
import csv
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
            pprint.pprint(j)

#            entry = {k: j[k] for k in csv_columns}
#            writer.writerow(entry)
#            f.flush()


def build_request():
    return '{ \
            "type": "subscribe", \
            "product_ids": ["BTC-USD"], \
            "channels": [ \
                          "level2" \
                        ] \
           }'


def main():
    gdax_ob = ob()
    url = "wss://ws-feed.gdax.com"
    #f = open("ob.csv", "w")
    ''' "changes" is a list of ["side", "price", "size"] '''
    csv_columns = ["time", "product_id", "side", "price", "size", "type"]
    #writer = csv.DictWriter(f, fieldnames = csv_columns)
    #writer.writeheader()
    asyncio.get_event_loop().run_until_complete(start_gdax_websocket(gdax_ob, url))
   
if __name__=="__main__":
    main()
