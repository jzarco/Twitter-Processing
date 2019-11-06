
import tweepy
import time
from tweepy import OAuthHandler, Stream, API, Cursor
from tweepy.streaming import StreamListener
import socket
import json
import pickle
import datetime
import os

consumer_key = 'mspsDGQfT4NOYW5ZicnWQ9BPP'
consumer_secret = 'NRxCkfLpnmOHT7Wyt7rGKOPyaMMoluqHcINOF6Avw54efRxtac'
access_token = '1185999760832061442-XdUvPnd1nBCnR0utKFEoWWv9dz1YiT'
access_secret = 'nzxAClrVyKhVfL2IVI3IFDXcf3qSeUodNaPIjFGqHzEK4'

dir = os.path.dirname(os.path.realpath(__file__))
today = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')

class TwitterAuthenticator:

    def __init__(self,consumer_key, consumer_secret, access_token, access_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret

    def authenticate(self):
        auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_secret)
        return auth

class TwitterClient:

    def __init__(self):
        self.authenticator = TwitterAuthenticator(consumer_key,
                                                  consumer_secret,
                                                  access_token,
                                                  access_secret).authenticate()
        self.twitter_client = API(self.authenticator, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_timeline_tweets(self, twitter_user=None, num_tweets=50):
        tweets = list()
        for tweet in Cursor(self.twitter_client.user_timeline, id=twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self,twitter_user=None, num_friends=50):
        friend_list = list()
        for friend in Cursor(self.twitter_client.friends, id=twitter_user).items(num_friends):
            friend_list.append(friend)
        return friend_list

    def get_home_timeline_tweets(self, twitter_user=None, num_tweets=50):
        timeline_tweets = list()
        for tweet in Cursor(self.twitter_client.home_timeline, id=twitter_user).items(num_tweets):
            timeline_tweets.append(tweet)
        return timeline_tweets

    def get_tweet_replies(self, tweet_id, num_tweets=50):
        replies = list()
        for reply in Cursor(self.twitter_client.user_timeline, in_reply_to_status_id=tweet_id).items(num_tweets):
            replies.append(reply)
        return replies

class TweetListener(StreamListener):
    
    def __init__(self, filename=None):
        super().__init__()
        self.filename = filename
    
    def on_data(self,data):
        try:
            msg = json.loads(data)
            if self.filename is None:
                self.filename = 'twitterStream'
                with open('StreamingDataLog/' + self.filename + ' ' + today + '.pkl', 'ab') as f:
                    dat = pickle.dump(msg, f) #handler
            else:
                with open('StreamingDataLog/' + self.filename + ' ' + today + '.pkl', 'ab') as f:
                    dat = pickle.dump(msg, f) #handler

            print(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("ERROR: ",e)
        return True
    
    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit error status occurs.
            return False
        print(status)
        return True

def sendData(topics,streaming_method='track'):
    auth = TwitterAuthenticator(consumer_key,
                                consumer_secret,
                                access_token,
                                access_secret).authenticate()
    
    twitter_stream = Stream(auth, TweetListener())
    if streaming_method == 'track':
        twitter_stream.filter(track=topics, is_async=True)
    elif streaming_method == 'follow':
        twitter_stream.filter(follow=topics, is_async=True)
