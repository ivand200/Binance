import asyncio
import json
from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager, AsyncClient, BinanceSocketManager
from api import API_KEY, SECRET_KEY
# from database import connection


async def kline_listener(client):
    bm = BinanceSocketManager(client)
    ms = bm.multiplex_socket(['btcusdt@bookTicker'])
    async with ms as stream:
        while True:
            res = await stream.recv()
            print(json.dumps(res, indent=2))
            cursor = connection.cursor()
            query = """INSERT INTO test (ticker, price) VALUES (%s,%s)"""
            to_insert = (res["s"], res["p"])
            cursor.execute(query, to_insert)
            connection.commit()

            print(res)


async def main():

    client = await AsyncClient.create(API_KEY, SECRET_KEY)
    await kline_listener(client)

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
