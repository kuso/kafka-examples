# -*- coding: utf-8 -*-
import time
from kafka import KafkaConsumer

topic = 'test'

consumer = KafkaConsumer(topic,
                         group_id='group_1',
                         bootstrap_servers=['127.0.0.1:9092'],
                         auto_offset_reset='latest')

def get():
    raw_messages = consumer.poll(timeout_ms=5000, max_records=10)
    for topic_partition, msgs in raw_messages.items():
        for msg in msgs:
            print(msg.value.decode())



if __name__ == '__main__':

    while True:
        time.sleep(1)
        get()
