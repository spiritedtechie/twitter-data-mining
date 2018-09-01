import twitter
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093','localhost:9094'], 
    retries=5, 
    acks=1,
    batch_size=0,
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)

def on_send_success(record_metadata):
    print("Message sent")
    print(" Topic: {}".format(record_metadata.topic))
    print(" Partition: {}".format(record_metadata.partition))
    print(" Offset: {}".format(record_metadata.offset))

def on_send_error(excp):
    logging.error('Error sending message', exc_info=excp)

with open('twitter-credentials.json', 'r') as config_file:
    config_data = json.load(config_file)

api = twitter.Api(consumer_key=config_data['consumer_key'],
                  consumer_secret=config_data['consumer_secret'],
                  access_token_key=config_data['access_token_key'],
                  access_token_secret=config_data['access_token_secret'])

# Get some data
key = b'1'
value = {'id': 1, 'name': 'John Smith', 'tweet': 'Kafka is cool'}

# Send it to kafka topic
producer.send(topic='twitter-data', key=key, value=value)\
    .add_callback(on_send_success)\
    .add_errback(on_send_error)

producer.flush()