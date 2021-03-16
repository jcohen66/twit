import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch

# import twitter keys and tokens
from config import *

# create instance of elasticsearch
from config.config import consumer_key, access_token, access_token_secret
from config.config import consumer_secret

import hashlib

es = Elasticsearch()

class TweetStreamListener(StreamListener):

    def __init__(self, message_dict ={}, message_count = 0):
        self.message_dict = message_dict
        self.message_count = message_count


    # on success
    def on_data(self, data):
        try:
            # decode json
            dict_data = json.loads(data)

            # pass tweet into TextBlob
            tweet = TextBlob(dict_data["text"])

            # output sentiment polarity
            print(tweet.sentiment.polarity)

            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

            # output sentiment
            print(sentiment)

            message_as_bytes = str.encode(dict_data["text"])
            hash_object = hashlib.md5(message_as_bytes)
            print(hash_object.hexdigest())
            if hash_object.hexdigest() in self.message_dict:
                self.message_dict[hash_object.hexdigest()] += 1
            else:
                self.message_dict[hash_object.hexdigest()] = 1

            if self.message_count % 50 == 0:
                for count in self.message_dict.values():
                    if count > 1:
                        print(count, end=" ")

            self.message_count += 1
            # add text and sentiment info to elasticsearch
            es.index(index="sentiment",
                     doc_type="test-type",
                     body={"author": dict_data["user"]["screen_name"],
                           "hash": hash_object.hexdigest(),
                           "date": dict_data["created_at"],
                           "message": dict_data["text"],
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "sentiment": sentiment})
        except:
            pass

        return True

    # on failure
    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "congress" keyword
    stream.filter(track=['trump'])
