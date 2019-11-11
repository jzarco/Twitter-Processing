
import tweepy
from tweepy import OAuthHandler, Stream, API, Cursor
from tweepy.streaming import StreamListener
import socket
import json
import pickle
import datetime
import os
import time
import credentials

dir = os.path.dirname(os.path.realpath(__file__))
today = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')

def handle_limit(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print('Rate Limit Error occurred. Waiting until allowed to continue iterating.')
            time.sleep(15 * 60)


def sendData(topics, logger=None):
    auth = TwitterAuthenticator(credentials.consumer_key,
                                credentials.consumer_secret,
                                credentials.access_token,
                                credentials.access_secret).authenticate()

    twitter_stream = Stream(auth, TweetListener(logger=logger))
    twitter_stream.filter(track=topics, is_async=True)

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

    def __init__(self,logger=None):
        self.authenticator = TwitterAuthenticator(credentials.consumer_key,
                                                  credentials.consumer_secret,
                                                  credentials.access_token,
                                                  credentials.access_secret).authenticate()
        self.twitter_client = API(self.authenticator, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        if logger is not None:
            self.logger = logger

    def get_timeline_tweets(self, twitter_user=None, num_tweets=50):
        tweets = list()
        self.logger.info('Func - get_timeline_tweets: twitter_user - {}, num_tweets - {}'.format(twitter_user, num_tweets))
        for tweet in handle_limit(Cursor(self.twitter_client.user_timeline, id=twitter_user).items(num_tweets)):
            tweets.append(tweet)
        return tweets

    def get_friend_list(self,twitter_user=None, num_friends=50):
        friend_list = list()
        self.logger.info('Func - get_friend_list: twitter_user - {}, num_friends - {}'.format(twitter_user, num_friends))
        for friend in handle_limit(Cursor(self.twitter_client.friends, id=twitter_user).items(num_friends)):
            friend_list.append(friend)
        return friend_list

    def get_home_timeline_tweets(self, twitter_user=None, num_tweets=50):
        timeline_tweets = list()
        self.logger.info('Func - get_homeline_tweets: twitter_user - {}, num_tweets - {}'.format(twitter_user, num_tweets))
        for tweet in handle_limit(Cursor(self.twitter_client.home_timeline, id=twitter_user).items(num_tweets)):
            timeline_tweets.append(tweet)
        return timeline_tweets

    def get_tweet_replies(self, tweet_id, num_tweets=50):
        """
        :param tweet_id: The Twitter ID you wish to see any replies towards.
        :param num_tweets: Number of tweets to cursor through.
        :return: List of dictionary/json objects that correspond to the tweet information
        """
        replies = list()
        self.logger.info('Func - get_tweet_replies: tweet_id - {}, num_tweets - {}'.format(tweet_id, num_tweets))
        for reply in Cursor(self.twitter_client.user_timeline, in_reply_to_status_id=tweet_id).items(num_tweets):
            replies.append(reply)
        return replies

    def get_statuses(self, ids=None):
        pass

class TweetListener(StreamListener):
    
    def __init__(self, filename=None, logger=None):
        super().__init__()
        self.filename = filename
        if logger is not None:
            self.logger = logger
    
    def on_data(self,data):
        try:
            msg = json.loads(data)
            lang = msg['lang']
            if lang == 'en':
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
            self.logger.error("ERROR: {}".format(e))
        return True
    
    def on_error(self, status):
        if status == 420:
            # Returning False on_data method in case rate limit error status occurs.
            self.logger.error(status)
            return False
        self.logger.debug(status)
        return True
