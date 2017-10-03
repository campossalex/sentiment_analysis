import tweepy
import json
import threading, logging, time
from kafka import KafkaConsumer, KafkaProducer, KafkaClient, SimpleProducer
from kafka.errors import KafkaError
import random, sys

topic = str(sys.argv[1])
hashtag = str(sys.argv[2])

print "Topic: " + topic
print "Hashtag: " + hashtag

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(key, secret)

api = tweepy.API(auth)

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
	self.producer = KafkaProducer(bootstrap_servers=['<IP>'], value_serializer=lambda v: json.dumps(v).encode('ascii'))

    def on_status(self, status):
        rand_partition = random.randint(0, 2)

        text = status.text.encode('utf-8').strip()
        timestamp_ms = status.timestamp_ms.encode('utf-8').strip()

	print "Tuit: " + text
	print "Partition: " + str(rand_partition)

	doc = {'text': text, 'timestamp': timestamp_ms, 'hashtag': hashtag}
	
	future = self.producer.send(str(topic), doc, partition=rand_partition)

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            log.exception()
            pass
	
myStreamListener = MyStreamListener(api)
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=[str(hashtag)])
