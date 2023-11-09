from confluent_kafka import Consumer, KafkaError
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
from datetime import datetime
import requests, socket
import time
import asyncio
import httpx

HOSTNAME: str = socket.gethostname()

data_dict = {}
# Define the Kafka consumer configuration
conf = {
    'bootstrap.servers': '172.16.250.13:9092,172.16.250.14:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'jss',       # Consumer group ID
    'auto.offset.reset': 'earliest'       # Start from the beginning of the topic
}

async def get_season(date):
    seasons = {
        'spring': (datetime(date.year, 3, 20), datetime(date.year, 6, 20)),
        'summer': (datetime(date.year, 6, 21), datetime(date.year, 9, 22)),
        'autumn': (datetime(date.year, 9, 23), datetime(date.year, 12, 20)),
        'winter': (datetime(date.year, 12, 21), datetime(date.year, 12, 31))
    }
    if date >= datetime(date.year, 1, 1) and date <= datetime(date.year, 3, 19):
        return 'winter'
    for season, (start, end) in seasons.items():
        if start <= date <= end:
            return season

async def consume_messages(topic):
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    batch_size = 50  # Number of messages to batch before sending
    iteration = 0

    try:
        batch = []
        while True:
            iteration = iteration + 1
            msg = consumer.poll(0.2)
            if msg is None:
                print('Polling returned None')
                # await asyncio.sleep(0.05)
                continue

            if msg.error():
                print(msg.error().code())
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                value = json.loads(msg.value().decode('utf-8'))
                timestamp = value.get('timestamp')
                house_id = value.get('house_id')
                kwh = value.get('kwh')
                date = datetime.fromtimestamp(timestamp)

                # if int(house_id) in range(50):
                #     continue

                season = await get_season(date)

                data_string = f"jss{{house_id=\"{house_id}\", season=\"{season}\"}} {kwh} {timestamp}"
                batch.append(data_string)
            
                if len(batch) >= batch_size:
                    print('Sendig batch')
                    await process_batch(batch)
                    batch = []  # Reset the batch
            print(iteration)

    finally:
        consumer.close()

async def process_batch(batch):
    # Process the batch of messages and send them in a single request
    url = f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance={HOSTNAME}"
    data = '\n'.join(batch)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data)
        print(f'Prometheus response code: {response.status_code}')
    except Exception as e:
        print(e)

async def main():
    consumer_name = input(f'Input topic: ')
    await consume_messages(consumer_name)

if __name__ == "__main__":
    asyncio.run(main())