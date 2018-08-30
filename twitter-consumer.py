import twitter
import json

with open('twitter-credentials.json', 'r') as config_file:
    config_data = json.load(config_file)

api = twitter.Api(consumer_key=config_data['consumer_key'],
                  consumer_secret=config_data['consumer_secret'],
                  access_token_key=config_data['access_token_key'],
                  access_token_secret=config_data['access_token_secret'])
