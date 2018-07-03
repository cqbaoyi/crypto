# crypto
1) Assign one worker thread to establish a websocket connection with each exchange. Each worker maintains a local copy of level2 order book and calculates CRT.
2) Run a master thread to build a consolidated order book.
3) The master thread calculates index every one second.

To do:
1) More exchange-specific websocket scripts.
2) Exception handling.
