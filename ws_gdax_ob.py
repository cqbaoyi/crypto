############################################
##
## Estalishes a websocket of gdax
## Subscribe level2 ob
## Write data into a csv
##
## Yi Bao    06/14/2018
##
############################################
import websockets, asyncio
import json
import csv
import pprint

url = "wss://ws-feed.gdax.com"
f = open("level2.csv", "w")

''' "changes" is a list of ["side", "price", "size"] '''
csv_columns = ["time", "product_id", "side", "price", "size", "type"]
writer = csv.DictWriter(f, fieldnames = csv_columns)
writer.writeheader()
async def start_gdax_websocket():
   async with websockets.connect(url) as websocket:
       await websocket.send(build_request())
       async for m in websocket:
           #print(m)
           j = json.loads(m)
           ''' the first message is asks, bids and channels '''
           if "channels" in j or "asks" in j:
               continue
           pprint.pprint(j)
           
           j["side"] = j["changes"][0][0]
           j["price"] = j["changes"][0][1]
           j["size"] = j["changes"][0][2]

           entry = {k: j[k] for k in csv_columns}
           writer.writerow(entry)
           f.flush()


def build_request():
   return '{ \
            "type": "subscribe", \
            "product_ids": ["BTC-USD"], \
            "channels": [ \
                          "level2" \
                        ] \
           }'


def main():
   asyncio.get_event_loop().run_until_complete(start_gdax_websocket())
   
if __name__=="__main__":
   main()
