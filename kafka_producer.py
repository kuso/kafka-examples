# -*- coding: utf-8 -*-
import time
from kafka import KafkaProducer

topic = 'test'
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])


def send(msg):
    print('sending %s' % msg)
    producer.send(topic, msg).get(timeout=20)


if __name__ == '__main__':
    count = 1
    while True:
        time.sleep(1)
        send('test msg = %s' % count)
        count += 1
