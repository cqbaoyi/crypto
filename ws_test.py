############################################
## Estalishes a websocket of gdax.
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
f = open("received.csv", "w")

'''
{'client_oid': 'd92cf876-d1ee-472d-89c1-33ec9999f0e1',
 'order_id': 'eecfc13a-5a8f-4060-b396-920d1b8da1f1',
 'order_type': 'limit',
 'price': '7601.34000000',
 'product_id': 'BTC-USD',
 'sequence': 6010057942,
 'side': 'buy',
 'size': '0.90000000',
 'time': '2018-06-05T17:56:00.060000Z',
 'type': 'received'}
'''

''' condier "received" type '''
csv_columns = ["type", "order_id", "order_type", "funds", "size", "price", "side", "client_oid", "product_id", "sequence", "time"]
writer = csv.DictWriter(f, fieldnames = csv_columns)
writer.writeheader()
async def start_gdax_websocket():
   async with websockets.connect(url) as websocket:
       await websocket.send(build_request())
       async for m in websocket:
           print(m)
           j = json.loads(m)
           #pprint.pprint(j)
           if j['type'] == "received":
               writer.writerow(j)
               f.flush()


def build_request():
   #return "{ \"type\": \"subscribe\",\"channels\": [{ \"name\": \"ticker\", \"product_ids\": [\"ETH-USD\"] }]}"
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
