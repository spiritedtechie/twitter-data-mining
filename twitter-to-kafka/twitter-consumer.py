import twitter
import json
import logging
from kafka import KafkaProducer
from progress.bar import Bar

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093', 'localhost:9094'],
    retries=5,
    acks=1,
    batch_size=0,
    value_serializer=lambda m: json.dumps(m).encode('ascii')
)


def on_send_success(record_metadata):
    return


def on_send_error(excp):
    logging.error('Error sending message', exc_info=excp)


with open('twitter-credentials.json', 'r') as config_file:
    config_data = json.load(config_file)

api = twitter.Api(consumer_key=config_data['consumer_key'],
                  consumer_secret=config_data['consumer_secret'],
                  access_token_key=config_data['access_token_key'],
                  access_token_secret=config_data['access_token_secret'])

friendIDs = api.GetFriendIDs()
bar = Bar('Processing', max=len(friendIDs))
for id in friendIDs:
    timeline = api.GetUserTimeline(user_id=id, count=200)

    for entry in timeline:
        entry_as_json = entry._json

        # Send it to kafka topic
        producer.send(topic='twitter-data', key=str(id).encode(), value=entry_as_json) \
            .add_callback(on_send_success) \
            .add_errback(on_send_error)

    producer.flush()

    bar.next()

bar.finish()
