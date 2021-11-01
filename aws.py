import boto3
from boto3.dynamodb.conditions import Key

from api import API_KEY, SECRET_KEY
from db import AWS_TABLE
import asyncio

from datetime import datetime, timezone
import time

from binance import AsyncClient, BinanceSocketManager
import json


async def main():
    client = await AsyncClient.create(API_KEY,SECRET_KEY)
    bm = BinanceSocketManager(client)
    ms = bm.multiplex_socket(['btcusdt@bookTicker'])
    async with ms  as tscm:
        while True:
            time.sleep(1)
            res = await tscm.recv()
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(AWS_TABLE)
            dt = datetime.now(timezone.utc).timestamp()
            table.put_item(
               Item = {
               'id': 'btcusdt',
               'created_at_epoch': f'{dt}',
               'bid': res['data']['b'],
               'ask': res['data']['a']
               }
            )
            print(dt, res['data']['b'], res['data']['a'])

        await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
