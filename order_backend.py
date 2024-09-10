import json
import time

from kafka import KafkaProducer, producer

ORDER_KAFKA_TOPIC = 'order_details'
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers='localhost:9092')

print('Going to br generating order after 10 seconds')
print('Will generate 1 unique order every 10 seconds')

for i in range(1, ORDER_LIMIT):
    data = {
        'order_id': i,
        'user_id': f'tom_{i}',
        'total_cost': i*3,
        'items': 'burger',
        'quantity': i*2
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode('utf-8')
    )
    print('Done sending ....')
    time.sleep(10)