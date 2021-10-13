import asyncio
from binance import AsyncClient, BinanceSocketManager
from api import API_KEY, SECRET_KEY
import json
from database import connection
from datetime import datetime, timezone
import time

async def main():
    client = await AsyncClient.create(API_KEY, SECRET_KEY)
    bm = BinanceSocketManager(client)
    ms = bm.multiplex_socket(['btcusdt@bookTicker'])  # 'btcusdt@depth5'
    async with ms as tscm:
        while True:
            time.sleep(1)
            res = await tscm.recv()
            cursor = connection.cursor()
            dt = datetime.now(timezone.utc)
            query = """INSERT INTO btc (ts, bid, ask) VALUES (%s,%s,%s)"""
            to_insert = (dt, res["data"]["b"], res["data"]["a"])
            cursor.execute(query, to_insert)
            connection.commit()
            print(dt, res['data']['b'], res['data']['a'])

    await client.close_connection()
    cursor.close()
    connection.close()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
