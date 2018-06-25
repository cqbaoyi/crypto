############################################
##
## Estalishes a websocket of bitfinex.
## Write data into a csv
##
## Yi Bao    06/25/2018
##
############################################
import websockets, asyncio
import json
import csv
import pprint

url = "wss://api.bitfinex.com/ws/2"
f = open("ticker_bitfinex.csv", "w")

''' ticker channel '''
csv_columns = ["CHANNEL_ID", "BID", "BID_SIZE", "ASK", "ASK_SIZE", "DAILY_CHANGE", \
               "DAILY_CHANGE_PERC", "LAST_PRICE", "VOLUME", "HIGH", "LOW"]
writer = csv.DictWriter(f, fieldnames = csv_columns)
writer.writeheader()
async def start_bitfinex_websocket():
   async with websockets.connect(url) as websocket:
       await websocket.send(build_request())
       async for m in websocket:
           j = json.loads(m)
           if "event" in j or "hb" in j:
               continue
           pprint.pprint(j)

           d = dict()
           d[csv_columns[0]] = j[0]
           for i, column in enumerate(csv_columns[1:]):
               d[column] = j[1][i]
           writer.writerow(d)
           f.flush()

'''
There are 5 channels: ticker, trades, books, raw books, candles
"ticker" response:
[
  CHANNEL_ID,
  [
    BID,
    BID_SIZE,
    ASK,
    ASK_SIZE,
    DAILY_CHANGE,
    DAILY_CHANGE_PERC,
    LAST_PRICE,
    VOLUME,
    HIGH,
    LOW
  ]
]
'''
def build_request():
   return '{ \
            "event": "subscribe", \
            "channel": "ticker", \
            "symbol": "tBTCUSD" \
           }'


def main():
   asyncio.get_event_loop().run_until_complete(start_bitfinex_websocket())

if __name__=="__main__":
   main()
