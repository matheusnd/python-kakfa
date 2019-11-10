import os
import tweepy as tw
from kafka import KafkaProducer
from json import dumps


class GetTweets:
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        self.search_words = "bolsonaro"
        self.date_since = "2019-11-01"

    def create_tweeter_connection(self):
        auth = tw.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        return tw.API(auth, wait_on_rate_limit=True)

    def get_tweets(self):
        api = self.create_tweeter_connection()
        return tw.Cursor(api.search, q=self.search_words, lang="en", since=self.date_since).items(5)


class TwitterKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                                      value_serializer=lambda x: dumps(x).encode('utf-8'))

    def send_message(self, tweet):
        self.producer.send('python_twiter', value=tweet)
        print(f"Sending tweet to Kafka: {tweet}")


if __name__ == "__main__":
    consumer_key = os.environ['CONSUMER_KEY']
    consumer_secret = os.environ['CONSUMER_SECRET']
    access_token = os.environ['ACCESS_TOKEN']
    access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

    Twitter = GetTweets(consumer_key, consumer_secret, access_token, access_token_secret)
    tweets = Twitter.get_tweets()

    twitter_producer = TwitterKafkaProducer()
    for tweet in tweets:
        twitter_producer.send_message(tweet.text)
