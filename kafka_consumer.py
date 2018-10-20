# -*- coding: utf-8 -*-
from kafka import KafkaConsumer, TopicPartition


def get_last_offset(topic):
    consumer = KafkaConsumer(group_id='group_1')
    partition = consumer.partitions_for_topic(topic).pop()
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])
    #committed = consumer.committed(tp)
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    consumer.close(autocommit=False)
    return last_offset


def main():
    topic = 'test'

    last_offset = get_last_offset(topic)
    print(last_offset)

    consumer = KafkaConsumer(topic,
                             group_id='group_1',
                             bootstrap_servers=['127.0.0.1:9092'],
                             auto_offset_reset='earliest')


    partition = consumer.partitions_for_topic(topic).pop()
    tp = TopicPartition(topic, partition)
    raw_messages = consumer.poll(timeout_ms=5000, max_records=5000)
    consumer.seek_to_end(tp)
    last_offset = consumer.position(tp)
    consumer.seek_to_beginning(tp)

    count = 0
    while True:
        raw_messages = consumer.poll(timeout_ms=5000, max_records=5000)
        for topic_partition, msgs in raw_messages.items():
            for msg in msgs:
                decoded = msg.value.decode()
                count += 1
                print('received text=%s' % decoded)
        print('%s msgs processed' % count)
        if count == last_offset:
            break

if __name__ == '__main__':
    main()
