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


def send_to_kafka_topic(key, value, topic='twitter-data'):
    producer.send(topic=topic, key=str(key).encode(), value=value) \
        .add_callback(on_send_success) \
        .add_errback(on_send_error)
    producer.flush()


def process_tweets_from_user_timeline(userId, timeline, actionFunc):
    tweetsProgressBar = Bar('Processing timeline tweets for friend with ID: %s' % userId, max=len(timeline))
    for entry in timeline:
        entry_as_json = entry._json
        actionFunc(userId, entry_as_json)
        tweetsProgressBar.next()

    tweetsProgressBar.finish()


def process_timelines(friendIDs):
    friendProgressBar = Bar('Processing friends'' timelines', max=len(friendIDs))
    for userId in friendIDs:
        timeline = api.GetUserTimeline(user_id=userId, count=200)
        process_tweets_from_user_timeline(userId, timeline,
                                          lambda userId, tweetAsJson : send_to_kafka_topic(userId, tweetAsJson))
        friendProgressBar.next()
    friendProgressBar.finish()


# Main script execution below
with open('twitter-credentials.json', 'r') as config_file:
    config_data = json.load(config_file)

api = twitter.Api(consumer_key=config_data['consumer_key'],
                  consumer_secret=config_data['consumer_secret'],
                  access_token_key=config_data['access_token_key'],
                  access_token_secret=config_data['access_token_secret'])

friendIDs = api.GetFriendIDs()

process_timelines(friendIDs)
