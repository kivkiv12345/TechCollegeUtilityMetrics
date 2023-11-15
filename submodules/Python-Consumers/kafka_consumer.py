from confluent_kafka import Consumer, KafkaError
import json
from datetime import datetime
import socket
import asyncio
import httpx

HOSTNAME: str = socket.gethostname()

data_dict = {}
# Define the Kafka consumer configuration
conf = {
    'bootstrap.servers': '172.16.250.13:9092,172.16.250.14:9092',  # Replace with your Kafka broker(s) address
    'group.id': 'seasonsPower18',       # Consumer group ID
    'auto.offset.reset': 'earliest'       # Start from the beginning of the topic
}

async def get_season(date):
    seasons = {
        'spring': (datetime(date.year, 3, 1), datetime(date.year, 6, 1)),
        'summer': (datetime(date.year, 6, 1), datetime(date.year, 9, 1)),
        'autumn': (datetime(date.year, 9, 1), datetime(date.year, 12, 1)),
        'winter': (datetime(date.year, 12, 1), datetime(date.year, 3, 1))
    }

    if date >= datetime(date.year, 1, 1) and date < datetime(date.year, 3, 1):
        return 'winter'
    for season, (start, end) in seasons.items():
        if start <= date < end:
            return season

async def consume_messages(topic):
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    batch_size = 1500  # Number of messages to batch before sending
    iteration = 0

    try:
        batch = []
        while True:
            iteration = iteration + 1
            msg = consumer.poll(0.1)
            if msg is None:
                print('Polling returned None')
                continue
            if msg.error():
                print(msg.error().code())
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('Reached end of partition')
                else:
                    print('Error while consuming message: {}'.format(msg.error()))
            else:
                value = json.loads(msg.value().decode('utf-8'))
                timestamp_ms = value.get('timestamp')
                house_id = value.get('house_id')
                kwh = value.get('kwh')
                timestamp_s = timestamp_ms / 1000
                date = datetime.utcfromtimestamp(float(timestamp_s))
                month = date.strftime("%B")
                season = await get_season(date)
                
                data_string = f"seasonsPower18{{house_id=\"{house_id}\", season=\"{season}\", month=\"{month}\"}} {kwh} {timestamp_s}"
                batch.append(data_string)
            
                if len(batch) >= batch_size: # Send after reaching batch size or after 3 seconds
                    # print('Sendig batch')
                    await process_batch(batch)
                    batch = []  # Reset the batch
                    # start_time = time.time() # Reset timer

            # if len(batch) > 0 and time.time() - start_time < 3:
            #     await process_batch(batch)
            print(iteration)

    finally:
        if len(batch) > 0:
            await process_batch(batch)
        consumer.close()

async def process_batch(batch):
    # Process the batch of messages and send them in a single request
    url = f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance={HOSTNAME}"
    data = '\n'.join(batch)
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, data=data)
        # print(f'Prometheus response code: {response.status_code}')
    except Exception as e:
        print(e)

async def main():
    consumer_name = input(f'Input topic: ')
    await consume_messages(consumer_name)

if __name__ == "__main__":
    asyncio.run(main())



# HOSTNAME: str = socket.gethostname()

# data_dict = {}
# # Define the Kafka consumer configuration
# conf = {
#     'bootstrap.servers': '172.16.250.13:9092,172.16.250.14:9092',  # Replace with your Kafka broker(s) address
#     'group.id': 'jappapower',       # Consumer group ID
#     'auto.offset.reset': 'earliest'       # Start from the beginning of the topic
# }

# async def get_season(date):
#     seasons = {
#         'spring': (datetime(date.year, 3, 1), datetime(date.year, 6, 1)),
#         'summer': (datetime(date.year, 6, 1), datetime(date.year, 9, 1)),
#         'autumn': (datetime(date.year, 9, 1), datetime(date.year, 12, 1)),
#         'winter': (datetime(date.year, 12, 1), datetime(date.year, 3, 1))
#     }
#     # date1 = date.replace(hour=0, minute=0, second=0, microsecond=0)

#     if date >= datetime(date.year, 1, 1) and date < datetime(date.year, 3, 1):
#         return 'winter'
#     for season, (start, end) in seasons.items():
#         if start <= date < end:
#             return season

# async def consume_messages(topic):
#     consumer = Consumer(conf)
#     consumer.subscribe([topic])
#     batch_size = 10  # Number of messages to batch before sending
#     iteration = 0

#     try:
#         batch = []
#         while True:
#             iteration = iteration + 1
#             msg = consumer.poll(0.2)
#             if msg is None:
#                 print('Polling returned None')
#                 continue

#             if msg.error():
#                 print(msg.error().code())
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     print('Reached end of partition')
#                 else:
#                     print('Error while consuming message: {}'.format(msg.error()))
#             else:
#                 # value = json.loads(msg.value().decode('utf-8'))

#                 # Convert bytes to string and split values by '|'
#                 values = msg.value().decode('utf-8').split('|')

#                 # Extracting individual values
#                 house_id = values[0].strip()
#                 kwh = values[1].strip()
#                 date = datetime.strptime(values[2].strip(), "%d-%m-%Y %H:%M:%S")
#                 print(date)
                
#                 # timestamp = value.get('timestamp')
#                 # house_id = value.get('house_id')
#                 # kwh = value.get('kwh')
#                 # date = datetime.utcfromtimestamp(timestamp)
#                 # # date = raw_date - timedelta(hours=6)
                
#                 month = date.strftime("%B")

#                 season = await get_season(date)

#                 if date.month in range(3,5) and season != 'spring' or date.month in range(6,8) and season != 'summer' or date.month in range(9,11) and season != 'autumn' or date.month in range(12,2) and season != 'spring' :
#                     print("OMEGAFAIL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
#                     print(season)

#                 data_string = f"seasonPower{{house_id=\"{house_id}\", season=\"{season}\", month=\"{month}\"}} {kwh} {datetime.timestamp(date)}"
#                 print(data_string)
#                 batch.append(data_string)
            
#                 if len(batch) >= batch_size:
#                     print('Sendig batch')
#                     await process_batch(batch)
#                     batch = []  # Reset the batch
#             print(iteration)

#     finally:
#         consumer.close()

# async def process_batch(batch):
#     # Process the batch of messages and send them in a single request
#     url = f"http://172.16.250.15:8428/api/v1/import/prometheus?extra_label=instance={HOSTNAME}"
#     data = '\n'.join(batch)
#     try:
#         async with httpx.AsyncClient() as client:
#             response = await client.post(url, data=data)
#         print(f'Prometheus response code: {response.status_code}')
#     except Exception as e:
#         print(e)

# async def main():
#     consumer_name = input(f'Input topic: ')
#     await consume_messages(consumer_name)

# if __name__ == "__main__":
#     asyncio.run(main())